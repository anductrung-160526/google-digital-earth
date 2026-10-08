"""Shared 2024 output contract and evidence-based, per-month inventory.

This module does not authenticate, submit exports, or write to Drive.
"""
import re
from pathlib import Path

import numpy as np
import pandas as pd

YEAR = 2024
MONTHS = range(1, 13)
EXPECTED_COMMUNES = 11136
ADMIN_COLUMNS = ['gid_3', 'name_3', 'type_3', 'gid_2', 'name_2', 'gid_1', 'name_1']
KEY = ['gid_3', 'year', 'month']
PREFIX = ADMIN_COLUMNS + ['year', 'month']
DAY_BANDS = ['BLUE', 'GREEN', 'RED', 'NIR', 'SWIR1', 'SWIR2', 'NDVI', 'NDBI', 'MNDWI', 'BSI']
DAY_METRICS = [f'{b}_{s}' for b in DAY_BANDS for s in ('mean', 'stdDev')]
NIGHT_METRICS = ['TIME', 'COMMUNE_AREA_HA', 'TNL', 'MEAN_RAD', 'STD_RAD', 'MIN_RAD', 'MAX_RAD',
                 'SPATIAL_CV', 'LIT_PIXELS', 'LIT_AREA_HA', 'ELECTRIFICATION_RATIO_PCT',
                 'LIT_POP_PROXY', 'CLOUD_FREE_OBS', 'TNL_MA3', 'TNL_MOM_GROWTH_PCT']
COLUMNS = {'day': PREFIX + DAY_METRICS, 'night': PREFIX + NIGHT_METRICS}
FIELDS = ['day_image', 'day_indices', 'night_image', 'night_indices']
TERMINAL = {'done', 'no_source'}
STATES = {'pending', 'running', 'done', 'no_source', 'failed'}
IMAGE_RE = re.compile(r'^(.+)_(day|night)_2024(0[1-9]|1[0-2])\.tif$')
VALIDATION_VERSION = 1
_LOCAL_IMAGE_CACHE = {}


def natural_key(gid):
    return tuple((0, int(x)) if x.isdigit() else (1, x) for x in re.split(r'(\d+)', str(gid)))


def administrative_table(admin, expected=EXPECTED_COMMUNES):
    admin = admin.rename(columns={c: c.lower() for c in admin.columns if c.lower() in ADMIN_COLUMNS})
    missing = set(ADMIN_COLUMNS) - set(admin.columns)
    if missing:
        raise ValueError(f'Bảng địa giới thiếu cột: {sorted(missing)}')
    admin = admin[ADMIN_COLUMNS].copy()
    if admin.isna().any().any() or admin.eq('').any().any() or admin['gid_3'].duplicated().any():
        raise ValueError('Bảng địa giới có mã trùng hoặc thông tin hành chính trống')
    if expected is not None and len(admin) != expected:
        raise ValueError(f'Phạm vi địa giới có {len(admin):,} xã; yêu cầu {expected:,}. '
                         'GADM/asset có thể khác bộ địa giới năm 2024. Không tự thêm hoặc bỏ xã; '
                         'cần cung cấp bảng địa giới đúng phạm vi.')
    return admin.astype(str)


def aliases(frame):
    """Unify old uppercase identifiers; reject conflicting duplicate identifiers."""
    df = frame.copy()
    for old in list(df.columns):
        new = old.lower()
        if new not in PREFIX or old == new:
            continue
        if new in df:
            both = df[old].notna() & df[new].notna()
            if not df.loc[both, old].astype(str).eq(df.loc[both, new].astype(str)).all():
                raise ValueError(f'Cột {old}/{new} mâu thuẫn')
            df[new] = df[new].combine_first(df[old])
            df = df.drop(columns=old)
        else:
            df = df.rename(columns={old: new})
    return df


def normalize(frame, admin, kind, complete=True):
    """Join names by GID, preserve scientific values, and optionally create 12 rows/GID.

    Missing rows become NaN, never zero. Conflicting duplicates are an error so an
    arbitrary old/new row cannot silently replace a scientific measurement.
    """
    df = aliases(frame)
    admin = administrative_table(admin, expected=None)
    metrics = DAY_METRICS if kind == 'day' else NIGHT_METRICS
    if df.empty:
        df = pd.DataFrame(columns=KEY + metrics)
    if not set(KEY).issubset(df):
        raise ValueError('CSV thiếu gid_3/year/month (hoặc GID_3/YEAR/MONTH)')
    unknown = set(df['gid_3'].dropna().astype(str)) - set(admin['gid_3'])
    if unknown:
        raise ValueError(f'CSV có GID ngoài địa giới: {sorted(unknown)[:10]}')
    for c in ['year', 'month']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    if df['gid_3'].isna().any() or not df['year'].eq(YEAR).all() or not df['month'].isin(MONTHS).all():
        raise ValueError('CSV có mã trống hoặc năm/tháng ngoài 2024/1..12')
    df['gid_3'] = df['gid_3'].astype(str)
    df = df.reindex(columns=KEY + metrics)
    for c in metrics:
        if c != 'TIME':
            original = df[c]
            df[c] = pd.to_numeric(original, errors='coerce')
            if (original.notna() & df[c].isna()).any():
                raise ValueError(f'Chỉ số {c} có giá trị không phải số')
        else:
            df[c] = df[c].astype(object)
    df = df.drop_duplicates()
    if df.duplicated(KEY).any():
        raise ValueError('CSV có bản ghi trùng khóa với chỉ số mâu thuẫn')
    if complete:
        grid = pd.MultiIndex.from_product([admin['gid_3'], [YEAR], MONTHS], names=KEY).to_frame(index=False)
        df = grid.merge(df, on=KEY, how='left', validate='one_to_one')
    df = df.merge(admin, on='gid_3', how='left', validate='many_to_one')
    df['year'], df['month'] = df['year'].astype('int64'), df['month'].astype('int64')
    df['_sort'] = df['gid_3'].map(natural_key)
    return df.sort_values(['_sort', 'name_3', 'year', 'month']).reindex(columns=COLUMNS[kind]).reset_index(drop=True)


def metric_state(row, kind, source_count=None):
    metrics = DAY_METRICS if kind == 'day' else NIGHT_METRICS
    vals = row.reindex(metrics)
    if source_count == 0:
        return ('no_source', '') if vals.isna().all() else ('failed', 'Nguồn rỗng nhưng CSV có chỉ số')
    if vals.isna().all():
        return 'pending', 'Chưa có chỉ số; chưa có bằng chứng nguồn rỗng'
    required = [c for c in metrics if c != 'TNL_MOM_GROWTH_PCT']
    if vals[required].isna().any():
        return 'failed', 'Chỉ số thiếu: ' + ','.join(vals[required].index[vals[required].isna()])
    numeric = [c for c in required if c != 'TIME']
    if not np.isfinite(pd.to_numeric(vals[numeric]).to_numpy(dtype=float)).all():
        return 'failed', 'Chỉ số không hữu hạn'
    if kind == 'night' and str(vals['TIME']) != f"{YEAR}-{int(row['month']):02d}":
        return 'failed', 'TIME không khớp year/month'
    # pct_change: NaN first month, or infinite for a zero preceding TNL, are genuine outcomes.
    return 'done', ''


def same_table(a, b):
    """Compare CSV content without repeatedly migrating pandas dtype differences."""
    try:
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True),
                                      check_dtype=False, check_exact=True)
        return True
    except AssertionError:
        return False


def metric_states(frame, kind, sources):
    """Validate an entire national CSV without one pandas lookup per commune-month."""
    metrics = DAY_METRICS if kind == 'day' else NIGHT_METRICS
    values = frame[metrics]
    empty = values.isna().all(axis=1)
    required = [c for c in metrics if c != 'TNL_MOM_GROWTH_PCT']
    numeric = [c for c in required if c != 'TIME']
    missing = values[required].isna().any(axis=1)
    finite = pd.Series(np.isfinite(values[numeric].to_numpy(dtype=float)).all(axis=1), index=frame.index)
    state = pd.Series('done', index=frame.index)
    error = pd.Series('', index=frame.index)
    state.loc[missing | ~finite] = 'failed'
    error.loc[missing | ~finite] = 'Chỉ số thiếu hoặc không hữu hạn'
    state.loc[empty] = 'pending'
    error.loc[empty] = 'Chưa có chỉ số; chưa có bằng chứng nguồn rỗng'
    if kind == 'night':
        expected_time = frame['month'].map(lambda m: f'{YEAR}-{int(m):02d}')
        wrong_time = ~frame['TIME'].eq(expected_time) & ~empty
        state.loc[wrong_time] = 'failed'
        error.loc[wrong_time] = 'TIME không khớp year/month'
    source_zero = pd.Series([sources.get((g, int(m), kind + '_indices')) == 0
                            for g, m in zip(frame['gid_3'], frame['month'])], index=frame.index)
    state.loc[source_zero & empty] = 'no_source'
    error.loc[source_zero & empty] = ''
    state.loc[source_zero & ~empty] = 'failed'
    error.loc[source_zero & ~empty] = 'Nguồn rỗng nhưng CSV có chỉ số'
    return {(g, int(m)): (s, e) for g, m, s, e in zip(frame['gid_3'], frame['month'], state, error)}


def merge_valid(old, incoming, admin, kind, sources=None):
    """Repair only invalid/missing records, retaining previously valid measurements."""
    sources = sources or {}
    prior = normalize(old, admin, kind)
    new = normalize(incoming, admin, kind, complete=False).set_index(KEY).sort_index()
    statuses = metric_states(prior, kind, sources)
    prior = prior.set_index(KEY).sort_index()
    repair = [key for key in new.index if statuses[key[0], key[2]][0] not in TERMINAL]
    metrics = DAY_METRICS if kind == 'day' else NIGHT_METRICS
    if repair:
        prior.loc[repair, metrics] = new.loc[repair, metrics]
    return normalize(prior.reset_index(), admin, kind)


def validate_image(path, kind):
    """Decode every data block; a readable header alone does not prove integrity."""
    import rasterio
    try:
        with rasterio.open(path) as src:
            count, scale = (10, 20) if kind == 'day' else (2, 500)
            if src.count != count or src.crs is None or src.crs.to_epsg() != 4326:
                raise ValueError('Số kênh hoặc CRS không đúng')
            tr = src.transform
            m_per_deg = 111319.49079327357
            if tr.a <= 0 or tr.e >= 0 or tr.b or tr.d or any(
                    abs(v * m_per_deg - scale) > scale * .01 for v in [tr.a, -tr.e]):
                raise ValueError('Độ phân giải/lưới không đúng')
            if kind == 'night' and set(src.dtypes) != {'float64'}:
                raise ValueError('Ảnh đêm cần float64')
            if kind == 'day' and any(not np.issubdtype(np.dtype(t), np.floating) for t in src.dtypes):
                raise ValueError('Ảnh ngày cần dữ liệu float, 10 kênh')
            for _, window in src.block_windows(1):
                src.read(window=window)
        return 'done', ''
    except Exception as exc:
        return 'failed', f'{type(exc).__name__}: {exc}'


def plan_sources(plan):
    out = {}
    for gid, months in plan.items():
        if len(months) != 12:
            raise ValueError(f'Kế hoạch {gid} thiếu tháng')
        for m, counts in enumerate(months, 1):
            if len(counts) != 3 or any(not isinstance(v, (int, float)) or not np.isfinite(v)
                                       or v < 0 or v != int(v) for v in counts):
                raise ValueError(f'Kế hoạch {gid}/{m} sai số cảnh')
            out[gid, m, 'day_image'] = next((v for v in counts if v > 0), 0)
    return out


def read_sources(path):
    if not Path(path).is_file():
        return {}
    df = pd.read_csv(path, dtype={'gid_3': str})
    if not set(KEY + ['field', 'count']).issubset(df):
        raise ValueError('source_counts.csv sai cấu trúc')
    if not df['year'].eq(YEAR).all() or not df['month'].isin(MONTHS).all() or not df['field'].isin(FIELDS).all():
        raise ValueError('source_counts.csv sai năm/tháng/field')
    if df['count'].isna().any() or not np.isfinite(df['count']).all() or (df['count'] < 0).any():
        raise ValueError('source_counts.csv thiếu/sai số cảnh')
    df = df.drop_duplicates()
    if df.duplicated(KEY + ['field']).any():
        raise ValueError('source_counts.csv có bằng chứng số cảnh mâu thuẫn')
    return {(r.gid_3, int(r.month), r.field): float(r.count) for r in df.itertuples()}


def progress(admin, frames, images, sources=None, previous=None):
    sources = sources or {}
    admin = administrative_table(admin, expected=None)
    checks = {k: metric_states(normalize(frames[k], admin, k), k, sources) for k in ['day', 'night']}
    checkpoints = {}
    if previous is not None and not previous.empty and set(KEY).issubset(previous):
        checkpoints = {tuple(r[k] for k in KEY): r for r in previous.to_dict('records')}
    result = []
    for a in admin.to_dict('records'):
        for month in MONTHS:
            row = {**a, 'year': YEAR, 'month': month}
            prior = checkpoints.get((a['gid_3'], YEAR, month), {})
            for kind in ['day', 'night']:
                key = (a['gid_3'], month)
                field = kind + '_image'
                state, error = images.get((kind, *key), ('pending', 'Chưa có ảnh'))
                if state == 'pending' and sources.get((*key, field)) == 0:
                    state, error = 'no_source', ''
                row[field], row[field + '_error'] = state, error
                field = kind + '_indices'
                state, error = checks[kind][key]
                row[field], row[field + '_error'] = state, error
            for field in FIELDS:
                if row[field] == 'pending' and prior.get(field) in {'running', 'failed'}:
                    row[field] = 'failed'  # interrupted/unfinished work must be retried
                    row[field + '_error'] = prior.get(field + '_error') or 'Lượt trước bị ngắt; cần chạy tiếp'
            result.append(row)
    df = pd.DataFrame(result)
    df['_sort'] = df['gid_3'].map(natural_key)
    return df.sort_values(['_sort', 'name_3', 'year', 'month']).drop(columns='_sort').reset_index(drop=True)


def require_day_complete(table):
    if table.empty or not set(KEY + ['day_image', 'day_indices']).issubset(table):
        raise RuntimeError('Chưa có kiểm kê phần ngày hợp lệ')
    if table.duplicated(KEY).any() or not table['year'].eq(YEAR).all() or not table['month'].isin(MONTHS).all():
        raise RuntimeError('Kiểm kê trùng khóa hoặc sai năm/tháng')
    if not table.groupby('gid_3')['month'].nunique().eq(12).all():
        raise RuntimeError('Kiểm kê phần ngày thiếu tháng')
    blocked = ~table[['day_image', 'day_indices']].isin(TERMINAL).all(axis=1)
    if blocked.any():
        raise RuntimeError(f'Chặn phần đêm: {int(blocked.sum())} xã–tháng ngày còn thiếu/lỗi')


def scan_local(root, admin, plan=None):
    """Colab/mounted Drive: independently verify actual files before export submission."""
    root = Path(root)
    if not root.is_dir():
        raise RuntimeError(f'Không đọc được thư mục đích {root}; mount Drive trước')
    frames = {}
    for kind in ['day', 'night']:
        path = root / 'CSV' / f'{kind}_indices.csv'
        frames[kind] = pd.read_csv(path) if path.is_file() else pd.DataFrame(columns=COLUMNS[kind])
    sources = read_sources(root / '_control/source_counts.csv')
    sources.update(plan_sources(plan or {}))
    mapping = {g.replace('.', '_'): g for g in administrative_table(admin, None)['gid_3']}
    images = {}
    for kind in ['day', 'night']:
        for path in (root / kind.title()).rglob('*.tif'):
            match = IMAGE_RE.match(path.name)
            if match and match[2] == kind and match[1] in mapping:
                key = (kind, mapping[match[1]], int(match[3]))
                if key in images:
                    images[key] = 'failed', 'Có nhiều file cùng xã/tháng'
                else:
                    stat = path.stat()
                    fingerprint = stat.st_size, stat.st_mtime_ns, stat.st_ino, VALIDATION_VERSION
                    cached = _LOCAL_IMAGE_CACHE.get(str(path.resolve()))
                    if cached and cached[0] == fingerprint:
                        images[key] = cached[1]
                    else:
                        images[key] = validate_image(path, kind)
                        _LOCAL_IMAGE_CACHE[str(path.resolve())] = fingerprint, images[key]
    checkpoint = root / '_control/progress.csv'
    previous = pd.read_csv(checkpoint) if checkpoint.is_file() else None
    return progress(admin, frames, images, sources, previous)
