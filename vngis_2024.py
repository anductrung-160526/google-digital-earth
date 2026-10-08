# -*- coding: utf-8 -*-
"""
VNGISDash 2024: pipeline tự động cấp xã chạy trên GitHub Actions.

Nguồn khoa học: VNGISDash_Task123_Merged_final.ipynb. Pipeline chỉ giữ 3 chức năng:
  (1) trích xuất chỉ số từ ảnh ngày (Task 1) và ảnh đêm (Task 3.2), (2) lấy ảnh tif ngày (Task 2),
  (3) lấy ảnh tif đêm (Task 3.1). Không có bước chọn tỉnh, xã: VNGIS_MODE=pilot tự lấy VNGIS_PILOT_N xã,
  VNGIS_MODE=full chạy mọi xã.

Cấu trúc đầu ra (cấp 1 = thư mục trên Drive):
  Day/<GID_1>_<tỉnh>/<GID_3>_<xã>/<GID_3>_day_2024MM.tif
  Night/<GID_1>_<tỉnh>/<GID_3>_<xã>/<GID_3>_night_2024MM.tif
  CSV/day_indices.csv, CSV/night_indices.csv         (gộp toàn quốc)
  _control/                                         (trạng thái, log, báo cáo)

Nền khoa học v6 được giữ nguyên. Điều phối trong Engine kiểm kê dữ liệu thật,
hoàn tất toàn bộ ngày rồi mới xử lý đêm. Chỉ tải ảnh thiếu/lỗi; chỉ số cũ hợp lệ được giữ.

Chạy:
    python vngis_2024.py               chạy pipeline
    python vngis_2024.py --sync-only   đẩy nốt dữ liệu trên máy lên Drive

Mã thoát: 0 bước yêu cầu hoàn tất | 1 lỗi/hết giới hạn thử | 3 hết giờ (nối lượt) | 130 dừng tay
"""

import os, io, re, sys, json, time, glob, math, shutil, zipfile, signal, logging, calendar, struct
import threading, subprocess, unicodedata, warnings
sys.modules.setdefault("vngis_2024", sys.modules[__name__])
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")


# Schema và điều khiển request được gộp để chỉ giữ các file v6.
"""Shared 2024 output contract and evidence-based, per-month inventory.

This module does not authenticate, submit exports, or write to Drive.
"""
import re
from pathlib import Path

import numpy as np
import pandas as pd

CONTRACT_YEAR = 2024
CONTRACT_MONTHS = range(1, 13)
# Full Vietnam level-3 scope in GADM 4.1, explicitly selected by the user.
CONTRACT_EXPECTED_COMMUNES = 11163
CONTRACT_ADMIN_COLUMNS = ['gid_3', 'name_3', 'type_3', 'gid_2', 'name_2', 'gid_1', 'name_1']
CONTRACT_KEY = ['gid_3', 'year', 'month']
CONTRACT_PREFIX = CONTRACT_ADMIN_COLUMNS + ['year', 'month']
CONTRACT_DAY_BANDS = ['BLUE', 'GREEN', 'RED', 'NIR', 'SWIR1', 'SWIR2', 'NDVI', 'NDBI', 'MNDWI', 'BSI']
CONTRACT_DAY_METRICS = [f'{b}_{s}' for b in CONTRACT_DAY_BANDS for s in ('mean', 'stdDev')]
CONTRACT_NIGHT_METRICS = ['TIME', 'COMMUNE_AREA_HA', 'TNL', 'MEAN_RAD', 'STD_RAD', 'MIN_RAD', 'MAX_RAD',
                 'SPATIAL_CV', 'LIT_PIXELS', 'LIT_AREA_HA', 'ELECTRIFICATION_RATIO_PCT',
                 'LIT_POP_PROXY', 'CLOUD_FREE_OBS', 'TNL_MA3', 'TNL_MOM_GROWTH_PCT']
CONTRACT_COLUMNS = {'day': CONTRACT_PREFIX + CONTRACT_DAY_METRICS, 'night': CONTRACT_PREFIX + CONTRACT_NIGHT_METRICS}
CONTRACT_FIELDS = ['day_image', 'day_indices', 'night_image', 'night_indices']
CONTRACT_TERMINAL = {'done', 'no_source'}
CONTRACT_STATES = {'pending', 'running', 'done', 'no_source', 'failed'}
CONTRACT_IMAGE_RE = re.compile(r'^(.+)_(day|night)_2024(0[1-9]|1[0-2])\.tif$')
CONTRACT_VALIDATION_VERSION = 1


def contract_natural_key(gid):
    return tuple((0, int(x)) if x.isdigit() else (1, x) for x in re.split(r'(\d+)', str(gid)))


def contract_administrative_table(admin, expected=CONTRACT_EXPECTED_COMMUNES):
    admin = admin.rename(columns={c: c.lower() for c in admin.columns if c.lower() in CONTRACT_ADMIN_COLUMNS})
    missing = set(CONTRACT_ADMIN_COLUMNS) - set(admin.columns)
    if missing:
        raise ValueError(f'Bảng địa giới thiếu cột: {sorted(missing)}')
    admin = admin[CONTRACT_ADMIN_COLUMNS].copy()
    if admin.isna().any().any() or admin.eq('').any().any() or admin['gid_3'].duplicated().any():
        raise ValueError('Bảng địa giới có mã trùng hoặc thông tin hành chính trống')
    if expected is not None and len(admin) != expected:
        raise ValueError(f'Phạm vi địa giới có {len(admin):,} xã; yêu cầu {expected:,}. '
                         'GADM/asset có thể khác bộ địa giới năm 2024. Không tự thêm hoặc bỏ xã; '
                         'cần cung cấp bảng địa giới đúng phạm vi.')
    return admin.astype(str)


def contract_aliases(frame):
    """Unify old uppercase identifiers; reject conflicting duplicate identifiers."""
    df = frame.copy()
    for old in list(df.columns):
        new = old.lower()
        if new not in CONTRACT_PREFIX or old == new:
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


def contract_normalize(frame, admin, kind, complete=True):
    """Join names by GID, preserve scientific values, and optionally create 12 rows/GID.

    Missing rows become NaN, never zero. Conflicting duplicates are an error so an
    arbitrary old/new row cannot silently replace a scientific measurement.
    """
    df = contract_aliases(frame)
    admin = contract_administrative_table(admin, expected=None)
    metrics = CONTRACT_DAY_METRICS if kind == 'day' else CONTRACT_NIGHT_METRICS
    if df.empty:
        df = pd.DataFrame(columns=CONTRACT_KEY + metrics)
    if not set(CONTRACT_KEY).issubset(df):
        raise ValueError('CSV thiếu gid_3/year/month (hoặc GID_3/YEAR/MONTH)')
    unknown = set(df['gid_3'].dropna().astype(str)) - set(admin['gid_3'])
    if unknown:
        raise ValueError(f'CSV có GID ngoài địa giới: {sorted(unknown)[:10]}')
    for c in ['year', 'month']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    if df['gid_3'].isna().any() or not df['year'].eq(CONTRACT_YEAR).all() or not df['month'].isin(CONTRACT_MONTHS).all():
        raise ValueError('CSV có mã trống hoặc năm/tháng ngoài 2024/1..12')
    df['gid_3'] = df['gid_3'].astype(str)
    df = df.reindex(columns=CONTRACT_KEY + metrics)
    for c in metrics:
        if c != 'TIME':
            original = df[c]
            df[c] = pd.to_numeric(original, errors='coerce')
            if (original.notna() & df[c].isna()).any():
                raise ValueError(f'Chỉ số {c} có giá trị không phải số')
        else:
            df[c] = df[c].astype(object)
    df = df.drop_duplicates()
    if df.duplicated(CONTRACT_KEY).any():
        raise ValueError('CSV có bản ghi trùng khóa với chỉ số mâu thuẫn')
    if complete:
        grid = pd.MultiIndex.from_product([admin['gid_3'], [CONTRACT_YEAR], CONTRACT_MONTHS], names=CONTRACT_KEY).to_frame(index=False)
        df = grid.merge(df, on=CONTRACT_KEY, how='left', validate='one_to_one')
    df = df.merge(admin, on='gid_3', how='left', validate='many_to_one')
    df['year'], df['month'] = df['year'].astype('int64'), df['month'].astype('int64')
    df['_sort'] = df['gid_3'].map(contract_natural_key)
    return df.sort_values(['_sort', 'name_3', 'year', 'month']).reindex(columns=CONTRACT_COLUMNS[kind]).reset_index(drop=True)


def contract_metric_state(row, kind, source_count=None):
    metrics = CONTRACT_DAY_METRICS if kind == 'day' else CONTRACT_NIGHT_METRICS
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
    if kind == 'night' and str(vals['TIME']) != f"{CONTRACT_YEAR}-{int(row['month']):02d}":
        return 'failed', 'TIME không khớp year/month'
    # pct_change: NaN first month, or infinite for a zero preceding TNL, are genuine outcomes.
    return 'done', ''


def contract_same_table(a, b):
    """Compare CSV content without repeatedly migrating pandas dtype differences."""
    try:
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True),
                                      check_dtype=False, check_exact=True)
        return True
    except AssertionError:
        return False


def contract_metric_states(frame, kind, sources):
    """Validate an entire national CSV without one pandas lookup per commune-month."""
    metrics = CONTRACT_DAY_METRICS if kind == 'day' else CONTRACT_NIGHT_METRICS
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
        expected_time = frame['month'].map(lambda m: f'{CONTRACT_YEAR}-{int(m):02d}')
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


def contract_merge_valid(old, incoming, admin, kind, sources=None):
    """Repair only invalid/missing records, retaining previously valid measurements."""
    sources = sources or {}
    prior = contract_normalize(old, admin, kind)
    new = contract_normalize(incoming, admin, kind, complete=False).set_index(CONTRACT_KEY).sort_index()
    statuses = contract_metric_states(prior, kind, sources)
    prior = prior.set_index(CONTRACT_KEY).sort_index()
    repair = [key for key in new.index if statuses[key[0], key[2]][0] not in CONTRACT_TERMINAL]
    metrics = CONTRACT_DAY_METRICS if kind == 'day' else CONTRACT_NIGHT_METRICS
    if repair:
        prior.loc[repair, metrics] = new.loc[repair, metrics]
    return contract_normalize(prior.reset_index(), admin, kind)


def contract_validate_image(path, kind):
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


def contract_read_sources(path):
    if not Path(path).is_file():
        return {}
    df = pd.read_csv(path, dtype={'gid_3': str})
    if not set(CONTRACT_KEY + ['field', 'count']).issubset(df):
        raise ValueError('source_counts.csv sai cấu trúc')
    df['count'] = pd.to_numeric(df['count'], errors='raise').astype(float)
    if not df['year'].eq(CONTRACT_YEAR).all() or not df['month'].isin(CONTRACT_MONTHS).all() or not df['field'].isin(CONTRACT_FIELDS).all():
        raise ValueError('source_counts.csv sai năm/tháng/field')
    if df['count'].isna().any() or not np.isfinite(df['count']).all() or (df['count'] < 0).any() or df['count'].mod(1).ne(0).any():
        raise ValueError('source_counts.csv thiếu/sai số cảnh')
    df = df.drop_duplicates()
    if df.duplicated(CONTRACT_KEY + ['field']).any():
        raise ValueError('source_counts.csv có bằng chứng số cảnh mâu thuẫn')
    return {(r.gid_3, int(r.month), r.field): float(r.count) for r in df.itertuples()}


def contract_progress(admin, frames, images, sources=None, previous=None):
    sources = sources or {}
    admin = contract_administrative_table(admin, expected=None)
    checks = {k: contract_metric_states(contract_normalize(frames[k], admin, k), k, sources) for k in ['day', 'night']}
    checkpoints = {}
    if previous is not None and not previous.empty and set(CONTRACT_KEY).issubset(previous):
        checkpoints = {tuple(r[k] for k in CONTRACT_KEY): r for r in previous.to_dict('records')}
    result = []
    for a in admin.to_dict('records'):
        for month in CONTRACT_MONTHS:
            row = {**a, 'year': CONTRACT_YEAR, 'month': month}
            prior = checkpoints.get((a['gid_3'], CONTRACT_YEAR, month), {})
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
            for field in CONTRACT_FIELDS:
                if row[field] == 'pending' and prior.get(field) in {'running', 'failed'}:
                    row[field] = 'failed'  # interrupted/unfinished work must be retried
                    row[field + '_error'] = prior.get(field + '_error') or 'Lượt trước bị ngắt; cần chạy tiếp'
            result.append(row)
    df = pd.DataFrame(result)
    df['_sort'] = df['gid_3'].map(contract_natural_key)
    return df.sort_values(['_sort', 'name_3', 'year', 'month']).drop(columns='_sort').reset_index(drop=True)


def contract_require_day_complete(table):
    if table.empty or not set(CONTRACT_KEY + ['day_image', 'day_indices']).issubset(table):
        raise RuntimeError('Chưa có kiểm kê phần ngày hợp lệ')
    if table.duplicated(CONTRACT_KEY).any() or not table['year'].eq(CONTRACT_YEAR).all() or not table['month'].isin(CONTRACT_MONTHS).all():
        raise RuntimeError('Kiểm kê trùng khóa hoặc sai năm/tháng')
    if not table.groupby('gid_3')['month'].nunique().eq(12).all():
        raise RuntimeError('Kiểm kê phần ngày thiếu tháng')
    blocked = ~table[['day_image', 'day_indices']].isin(CONTRACT_TERMINAL).all(axis=1)
    if blocked.any():
        raise RuntimeError(f'Chặn phần đêm: {int(blocked.sum())} xã–tháng ngày còn thiếu/lỗi')

from types import SimpleNamespace
D = SimpleNamespace(
    YEAR=CONTRACT_YEAR,
    MONTHS=CONTRACT_MONTHS,
    EXPECTED_COMMUNES=CONTRACT_EXPECTED_COMMUNES,
    ADMIN_COLUMNS=CONTRACT_ADMIN_COLUMNS,
    KEY=CONTRACT_KEY,
    PREFIX=CONTRACT_PREFIX,
    DAY_BANDS=CONTRACT_DAY_BANDS,
    DAY_METRICS=CONTRACT_DAY_METRICS,
    NIGHT_METRICS=CONTRACT_NIGHT_METRICS,
    COLUMNS=CONTRACT_COLUMNS,
    FIELDS=CONTRACT_FIELDS,
    TERMINAL=CONTRACT_TERMINAL,
    STATES=CONTRACT_STATES,
    IMAGE_RE=CONTRACT_IMAGE_RE,
    VALIDATION_VERSION=CONTRACT_VALIDATION_VERSION,
    natural_key=contract_natural_key,
    administrative_table=contract_administrative_table,
    aliases=contract_aliases,
    normalize=contract_normalize,
    metric_state=contract_metric_state,
    same_table=contract_same_table,
    metric_states=contract_metric_states,
    merge_valid=contract_merge_valid,
    validate_image=contract_validate_image,
    read_sources=contract_read_sources,
    progress=contract_progress,
    require_day_complete=contract_require_day_complete,
)

"""Shared request pacing and bounded retries; never log signed URLs or response bodies."""
import logging
import random
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

log = logging.getLogger('vngis')


def retry_after(value, now=None):
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(str(value))
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            return max(0.0, (date - (now or datetime.now(timezone.utc))).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return None


def status_code(exc):
    response = getattr(exc, 'response', None)
    code = getattr(response, 'status_code', None) or getattr(exc, 'status_code', None)
    if code is None:
        code = getattr(getattr(exc, 'resp', None), 'status', None)
    if code is not None:
        return int(code)
    # EEException often wraps its HTTP error as text, without a response attribute.
    import re
    match = re.search(r'\b(429|500|502|503|504|401|403)\b', str(exc))
    if match:
        return int(match[1])
    if 'too many requests' in str(exc).lower():
        return 429
    return None


def retry_delay(attempt, header=None):
    explicit = retry_after(header)
    if explicit is not None:
        return explicit
    base = min(120.0, 5.0 * 2 ** attempt)
    return min(120.0, base + random.uniform(0, base * .25))


class RequestGate:
    """One concurrency budget for EE computations and image HTTP downloads.

    Service clocks pace request starts; a 429 pauses all workers using this gate.
    No semaphore is held while waiting for pacing, backoff or cooldown.
    """
    def __init__(self, concurrency, qps, check_stop, stop_event, clock=time.monotonic):
        if concurrency < 1 or qps <= 0:
            raise ValueError('Concurrency và QPS phải > 0')
        self.semaphore = threading.BoundedSemaphore(concurrency)
        self.qps = qps
        self.check_stop = check_stop
        self.stop_event = stop_event
        self.clock = clock
        self.lock = threading.Lock()
        self.next_start = {}
        self.cooldown = 0.0
        self.throttles = {}

    def wait(self, seconds, allow_stopped=False):
        if allow_stopped:
            time.sleep(seconds)
        else:
            self.check_stop()
            if self.stop_event.wait(seconds):
                self.check_stop()

    def defer(self, service, seconds, throttled=False):
        with self.lock:
            self.cooldown = max(self.cooldown, self.clock() + seconds)
            if throttled:
                self.throttles[service] = self.throttles.get(service, 0) + 1
                count = self.throttles[service]
            else:
                count = None
        if throttled:
            log.warning('%s: HTTP 429 #%s; cooldown chung %.1fs', service, count, seconds)

    @contextmanager
    def slot(self, service, allow_stopped=False):
        bucket = 'Drive' if service == 'Google Drive' else 'EE'
        while True:
            if not allow_stopped:
                self.check_stop()
            with self.lock:
                delay = max(self.cooldown, self.next_start.get(bucket, 0)) - self.clock()
            if delay > 0:
                self.wait(min(delay, 1), allow_stopped)
                continue
            if not self.semaphore.acquire(timeout=.1):
                continue
            with self.lock:
                now = self.clock()
                ready = max(self.cooldown, self.next_start.get(bucket, 0)) <= now
                if ready:
                    self.next_start[bucket] = now + 1.0 / self.qps
            if ready:
                break
            self.semaphore.release()
        try:
            yield
        finally:
            self.semaphore.release()

    def call(self, service, operation, attempts=6):
        for attempt in range(attempts):
            try:
                with self.slot(service):
                    return operation()
            except Exception as exc:
                code = status_code(exc)
                if code not in {429, 500, 502, 503, 504}:
                    raise
                if attempt == attempts - 1:
                    if code == 429:
                        self.defer(service, 0, True)
                    # Omit exception text: it can contain authenticated URLs.
                    raise RuntimeError(f'{service}: HTTP {code or "error"}; '
                                       f'{attempt + 1}/{attempts} lần thử') from None
                headers = getattr(getattr(exc, 'response', None), 'headers', {}) or getattr(exc, 'resp', {}) or {}
                delay = retry_delay(attempt, headers.get('Retry-After', headers.get('retry-after')))
                self.defer(service, delay, code == 429)

def _env(name, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    if cast is bool:
        return raw.strip().lower() in ("1", "true", "yes", "y")
    return cast(raw.strip())


# =====================================================================================
# 1. CẤU HÌNH
# =====================================================================================
YEAR = 2024
MONTHS = list(range(1, 13))

PROJECT_ID = _env("VNGIS_EE_PROJECT", "vngis-ee-2")                               # notebook cell 11
ASSET_ID = _env("VNGIS_EE_ASSET", f"projects/{PROJECT_ID}/assets/communes_l3")             # notebook cell 11

MODE = _env("VNGIS_MODE", "pilot").lower()                        # pilot | full
if MODE not in ("pilot", "full"):
    raise SystemExit(f"VNGIS_MODE phải là 'pilot' hoặc 'full', đang là '{MODE}'")
PILOT_N = _env("VNGIS_PILOT_N", 2, int)
if PILOT_N < 1:
    raise SystemExit("VNGIS_PILOT_N phải >= 1")

# Ảnh ngày: số kênh lưu vào tif. 6 = BLUE..SWIR2 (NDVI, NDBI, MNDWI, BSI tính lại được từ 6 kênh này);
# 10 = đủ 10 kênh như notebook (file lớn gần gấp đôi).
DAY_BANDS_ALL = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "NDVI", "NDBI", "MNDWI", "BSI"]
DAY_BANDS = _env("VNGIS_DAY_BANDS", 10, int)
if DAY_BANDS != 10:
    raise SystemExit("Luồng v6 này yêu cầu đủ 10 kênh ảnh ngày")
# Kiểu lưu ảnh ngày: float = giữ nguyên giá trị Earth Engine trả về, nén DEFLATE không mất dữ liệu (như bản 4);
# int16 = số nguyên round(giá trị × VNGIS_DAY_SCALE), file nhỏ hơn ~3-4 lần nhưng nhiều trình xem không mở được.
DAY_FORMAT = _env("VNGIS_DAY_FORMAT", "float").lower()
if DAY_FORMAT != "float":
    raise SystemExit("Luồng v6 này yêu cầu ảnh ngày float, không lượng tử hóa")
DAY_SCALE_INV = _env("VNGIS_DAY_SCALE", 10000, int)  # int16 = round(giá trị × 10000); 1000 cho file nhỏ hơn ~40%
if DAY_SCALE_INV not in (1000, 10000):
    raise SystemExit("VNGIS_DAY_SCALE phải là 10000 hoặc 1000")
DAY_NODATA = -32768

N_WORKERS = _env("VNGIS_WORKERS", 2, int)                         # số xã chạy song song
MONTH_THREADS = _env("VNGIS_MONTH_THREADS", 2, int)              # số ảnh tải song song trong 1 xã
EE_CONCURRENCY = _env("VNGIS_EE_CONCURRENCY", 4, int)            # tổng số lệnh gọi EE cùng lúc
RUN_ID = _env("VNGIS_RUN_ID", "local")
MAX_RUNTIME_SEC = _env("VNGIS_MAX_RUNTIME_SEC", 0, int)
EE_KEY_FILE = _env("VNGIS_EE_KEY_FILE", "")
EE_HIGH_VOLUME = _env("VNGIS_EE_HIGH_VOLUME", False, bool)
MAX_ATTEMPTS = _env("VNGIS_MAX_ATTEMPTS", 3, int)
PREFLIGHT = _env("VNGIS_PREFLIGHT", True, bool)
PREFLIGHT_TILE_TEST = _env("VNGIS_PREFLIGHT_TILE_TEST", MODE == "full", bool)   # pilot: bỏ qua
EE_DEADLINE_SEC = 300
DOWNLOAD_FAIL_LIMIT = 24
MAX_TILE_SPLIT = 8
TILING_OK = [True]

EE_QPS = _env("VNGIS_EE_QPS", 2.0, float)
REQUEST_ATTEMPTS = _env("VNGIS_REQUEST_ATTEMPTS", 6, int)
if min(N_WORKERS, MONTH_THREADS, EE_CONCURRENCY, MAX_ATTEMPTS, REQUEST_ATTEMPTS) < 1 or EE_QPS <= 0:
    raise SystemExit("Workers, concurrency, QPS và attempts phải > 0")

DRIVE_FOLDER = _env("VNGIS_DRIVE_FOLDER", "VNGISDash_2024_PILOT" if MODE == "pilot" else "VNGISDash_2024")
RCLONE_REMOTE = _env("VNGIS_RCLONE_REMOTE", "gdrive")
REMOTE_BASE = f"{RCLONE_REMOTE}:{DRIVE_FOLDER}"
LOCAL_ROOT = _env("VNGIS_LOCAL_ROOT", os.path.expanduser(f"~/vngis_2024/{DRIVE_FOLDER}"))
CACHE_DIR = _env("VNGIS_CACHE_DIR", os.path.expanduser("~/vngis_2024/_cache"))
UPLOAD_EVERY_SEC = _env("VNGIS_UPLOAD_EVERY_SEC", 300, int)
DRIVE_STOP_POLL_SEC = 300

D_DAY, D_NIGHT, D_CSV, D_CONTROL = "Day", "Night", "CSV", "_control"
D_STATUS = f"{D_CONTROL}/status"
D_PARTS = f"{D_CONTROL}/parts"          # chỉ số từng xã, gom thành CSV toàn quốc
D_LOGS = f"{D_CONTROL}/logs"
DAY_CSV = f"{D_CSV}/day_indices.csv"
NIGHT_CSV = f"{D_CSV}/night_indices.csv"

GADM_VNM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_VNM_shp.zip"   # notebook cell 3
ADM_COLS = ["GID_1", "NAME_1", "GID_2", "NAME_2", "GID_3", "NAME_3", "TYPE_3"]     # notebook cell 3

T1_FEATURES = [f"{b}_{s}" for b in DAY_BANDS_ALL for s in ("mean", "stdDev")]    # notebook cell 15
DAY_COLUMNS = D.COLUMNS["day"]
NIGHT_COLUMNS = D.COLUMNS["night"]

log = logging.getLogger("vngis")


def L(*parts):
    return os.path.join(LOCAL_ROOT, *parts)


# 2. DỪNG CÓ TRẬT TỰ, LOG
# =====================================================================================
class StopRequested(BaseException):
    """Kế thừa BaseException để khối `except Exception` của từng xã không nuốt mất."""


STOP_EVENT = threading.Event()
STOP_REASON = [None]


def request_stop(reason):
    if not STOP_EVENT.is_set():
        STOP_REASON[0] = reason
        STOP_EVENT.set()
        why = {"deadline": "hết thời gian của lượt", "fatal": "lỗi tải ảnh nghiêm trọng",
               "drive_stop": "có file STOP trên Drive"}.get(reason, "nhận tín hiệu dừng")
        log.warning(f"Dừng có trật tự ({why}): làm nốt bước đang chạy, đồng bộ rồi thoát.")


def check_stop():
    if STOP_EVENT.is_set():
        raise StopRequested()


REQUEST_GATE = RequestGate(EE_CONCURRENCY, EE_QPS, check_stop, STOP_EVENT)


def install_signal_handlers():
    def handler(_s, _f):
        if STOP_EVENT.is_set():
            os._exit(130)
        request_stop("signal")
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)


def setup_logging():
    os.makedirs(L(D_LOGS), exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname).1s [%(threadName)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    fh = logging.FileHandler(L(D_LOGS, f"run_{stamp}_{RUN_ID}.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.propagate = False


def _nb_print(*args, **_kw):
    """Các hàm chép từ notebook gọi print(); ở pipeline chuyển thành log mức DEBUG cho gọn."""
    log.debug(" ".join(str(a) for a in args))


# =====================================================================================
# =====================================================================================
# 3. CHUẨN HÓA TÊN (đúng notebook cell 15, 17)
# =====================================================================================
def normalize_str_t1(s):
    """Bản của Task 1 (notebook cell 15): bỏ ký tự đặc biệt, không thêm '_'."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = re.sub(r"[̀-ͯ]", "", s)
    s = s.replace("đ", "d").replace("Đ", "d")
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()


def normalize_str(s):
    """Bản của Task 2/3 (notebook cell 17)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = re.sub(r"[̀-ͯ]", "", s)
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    return s.strip("_").lower()


def commune_full_name(row):
    """Tên xã kèm loại đơn vị, đúng build_commune_map (notebook cell 13)."""
    c_type = str(row.get("TYPE_3", "")).strip()
    c_name = str(row["NAME_3"]).strip()
    return f"{c_type} {c_name}" if c_type and c_type.lower() != "nan" else c_name




def build_ctx(row):
    row = dict(row)
    gid1, gid3 = str(row["GID_1"]), str(row["GID_3"])
    name1 = str(row["NAME_1"])
    cname_full = commune_full_name(row)
    clean_pname = normalize_str(name1)
    clean_cname = normalize_str(cname_full)
    safe_gid3 = gid3.replace(".", "_")
    rel_sub = f"{gid1}_{clean_pname}/{gid3}_{clean_cname}"
    return {"row": row, "gid1": gid1, "gid3": gid3, "name1": name1, "cname_full": cname_full,
            "safe_gid3": safe_gid3,
            "rel_day_dir": f"{D_DAY}/{rel_sub}", "rel_night_dir": f"{D_NIGHT}/{rel_sub}"}


def day_name(ctx, m):
    return f"{ctx['safe_gid3']}_day_{YEAR}{m:02d}.tif"


def night_name(ctx, m):
    return f"{ctx['safe_gid3']}_night_{YEAR}{m:02d}.tif"


# =====================================================================================
# 4. EARTH ENGINE
# =====================================================================================
import ee

communes_fc = None
EE_CREDENTIALS = None


def init_earth_engine():
    global communes_fc, EE_CREDENTIALS
    ee.data.setMaxRetries(0)  # application retries own the bounded backoff
    kwargs = {"project": PROJECT_ID}
    if EE_HIGH_VOLUME:
        kwargs["opt_url"] = "https://earthengine-highvolume.googleapis.com"
    if EE_KEY_FILE:
        with open(EE_KEY_FILE, encoding="utf-8") as f:
            email = json.load(f)["client_email"]
        EE_CREDENTIALS = ee.ServiceAccountCredentials(email, EE_KEY_FILE)
        REQUEST_GATE.call("Earth Engine", lambda: ee.Initialize(credentials=EE_CREDENTIALS, **kwargs), REQUEST_ATTEMPTS)
        log.info(f"Earth Engine sẵn sàng: service account {email}, endpoint "
                 f"{'high-volume' if EE_HIGH_VOLUME else 'standard'}.")
    else:
        REQUEST_GATE.call("Earth Engine", lambda: ee.Initialize(**kwargs), REQUEST_ATTEMPTS)
        try:
            EE_CREDENTIALS = ee.data.get_persistent_credentials()
        except Exception:
            EE_CREDENTIALS = None
        log.info("Earth Engine sẵn sàng (tài khoản cá nhân).")
    ee.data.setDeadline(EE_DEADLINE_SEC * 1000)
    communes_fc = ee.FeatureCollection(ASSET_ID)


# ---------- Task 1: chép nguyên văn notebook cell 15 (print -> _nb_print, thêm tham số commune_gid như bản [LOCAL]) ----------
def mask_s2_sr(img):
  qa = img.select("QA60")
  cloud_mask = (qa.bitwiseAnd(1 << 10).eq(0)).And(qa.bitwiseAnd(1 << 11).eq(0))
  return (
      img.updateMask(cloud_mask)
      .divide(10000)
      .select(
          ["B2", "B3", "B4", "B8", "B11", "B12"],
          ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"],
      )
  )


def add_indices(img):
  ndvi = img.normalizedDifference(["NIR", "RED"]).rename("NDVI")
  ndbi = img.normalizedDifference(["SWIR1", "NIR"]).rename("NDBI")
  mndwi = img.normalizedDifference(["GREEN", "SWIR1"]).rename("MNDWI")
  bsi = img.expression(
      "((SWIR1 + RED) - (NIR + BLUE)) / ((SWIR1 + RED) + (NIR + BLUE))",
      {
          "SWIR1": img.select("SWIR1"),
          "RED": img.select("RED"),
          "NIR": img.select("NIR"),
          "BLUE": img.select("BLUE"),
      },
  ).rename("BSI")
  return img.addBands([ndvi, ndbi, mndwi, bsi])


# ---------- notebook cell 18 ----------
def mask_s2_clean(img):
  qa = img.select("QA60")
  cloud_mask = (qa.bitwiseAnd(1 << 10).eq(0)).And(qa.bitwiseAnd(1 << 11).eq(0))
  return (
      img.updateMask(cloud_mask)
      .divide(10000)
      .select(
          ["B2", "B3", "B4", "B8", "B11", "B12"],
          ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"],
      )
  )




# ---------- Ngày tháng (đúng cách notebook tính s_date, e_date) ----------
def _month_dates(year, month):
  _, last_day = calendar.monthrange(year, month)
  return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"


def _s2_windows(year, month):
  """3 cửa sổ thời gian của get_adaptive_monthly_composite (notebook cell 18): tháng, ±15 ngày, ±30 ngày."""
  _, last_day = calendar.monthrange(year, month)
  dt_start = datetime(year, month, 1)
  dt_end = datetime(year, month, last_day)
  return [
      (dt_start.strftime("%Y-%m-%d"), dt_end.strftime("%Y-%m-%d")),
      ((dt_start - timedelta(days=15)).strftime("%Y-%m-%d"), (dt_end + timedelta(days=15)).strftime("%Y-%m-%d")),
      ((dt_start - timedelta(days=30)).strftime("%Y-%m-%d"), (dt_end + timedelta(days=30)).strftime("%Y-%m-%d")),
  ]


S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED" if YEAR >= 2019 else "COPERNICUS/S2_HARMONIZED"
VIIRS_A = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"
VIIRS_B = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG"


def ee_getinfo(obj):
  return REQUEST_GATE.call("Earth Engine", obj.getInfo, REQUEST_ATTEMPTS)


# ---------- Kế hoạch tải: 1 lần gọi cho cả 12 tháng ----------
def fetch_plan(commune_fc, commune_geom, phase="day"):
  """Same v6 windows/VIIRS precedence; query only the active phase, recording source counts."""
  months = []
  for m in MONTHS:
    s_date, e_date = _month_dates(YEAR, m)
    if phase == "day":
      s2 = [ee.ImageCollection(S2_COLLECTION).filterBounds(commune_geom).filterDate(s, e).size()
            for s, e in _s2_windows(YEAR, m)]
      raw = ee.ImageCollection(S2_COLLECTION).filterBounds(commune_fc).filterDate(s_date, e_date).size()
      months.append(ee.List(s2 + [raw]))
    elif phase == "night":
      va = ee.ImageCollection(VIIRS_A).filterBounds(commune_geom).filterDate(s_date, e_date).size()
      vb = ee.ImageCollection(VIIRS_B).filterBounds(commune_geom).filterDate(s_date, e_date).size()
      months.append(ee.List([va, vb]))
    else:
      raise ValueError("phase phải là day hoặc night")
  out = ee_getinfo(ee.Dictionary({"n_fc": commune_fc.size(), "months": ee.List(months)}))
  if len(out["months"]) != 12:
    raise RuntimeError("Kế hoạch nguồn thiếu tháng")
  plan = {}
  for m, row in zip(MONTHS, out["months"]):
    if any(v is None or v < 0 or v != int(v) for v in row):
      raise RuntimeError("Số cảnh nguồn không hợp lệ")
    if phase == "day":
      c0, c1, c2, raw = row
      win = 0 if c0 > 0 else (1 if c1 > 0 else (2 if c2 > 0 else None))
      plan[m] = {"s2_window": win, "image_count": (c0, c1, c2)[win] if win is not None else 0,
                 "indices_count": raw}
    else:
      va, vb = row
      plan[m] = {"viirs": VIIRS_A if va > 0 else (VIIRS_B if vb > 0 else None),
                 "image_count": va if va > 0 else vb, "indices_count": va if va > 0 else vb}
  return out["n_fc"], plan


def day_image(m, window, commune_geom):
  """= add_indices(get_adaptive_monthly_composite(...)).clip(commune_geom) của notebook cell 18-19,
  với cửa sổ thời gian đã chọn ở fetch_plan."""
  s, e = _s2_windows(YEAR, m)[window]
  col = ee.ImageCollection(S2_COLLECTION).filterBounds(commune_geom).filterDate(s, e)
  composite = col.map(mask_s2_clean).median()
  return add_indices(composite).clip(commune_geom)


def night_image(m, collection_id, commune_geom):
  """= get_viirs_monthly_composite(...) của notebook cell 34, rồi .toDouble() như cell 36."""
  s_date, e_date = _month_dates(YEAR, m)
  col = ee.ImageCollection(collection_id).filterBounds(commune_geom).filterDate(s_date, e_date)
  return (col.select(["avg_rad", "cf_cvg"]).mean().clip(commune_geom)
          .set("system:time_start", s_date).toDouble())


# ---------- Task 1: notebook cell 15, gom 12 tháng thành 1 lần gọi ----------
def task1_all_months(commune_fc, months=None):
  """Mỗi tháng: lọc CLOUDY_PIXEL_PERCENTAGE < 85, nếu rỗng thì dùng toàn bộ cảnh; median; add_indices;
  reduceRegions(mean + stdDev, scale 50, tileScale 4, EPSG:4326). Tháng không có cảnh nào: bỏ (như notebook).
  Nhánh if/else của notebook chuyển thành ee.Algorithms.If phía máy chủ, cùng điều kiện, cùng kết quả."""
  bands = DAY_BANDS_ALL
  reducers = ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True)
  selected_cols = ["GID_3"] + T1_FEATURES
  per_month = []
  for m in MONTHS if months is None else months:
    s_date, e_date = _month_dates(YEAR, m)
    raw_col = ee.ImageCollection(S2_COLLECTION).filterBounds(commune_fc).filterDate(s_date, e_date)
    filtered_col = raw_col.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 85))
    composite = ee.Image(ee.Algorithms.If(filtered_col.size().eq(0),
                                          raw_col.map(mask_s2_sr).median(),
                                          filtered_col.map(mask_s2_sr).median()))
    tensor = add_indices(composite)
    stats = tensor.select(bands).reduceRegions(
        collection=commune_fc, reducer=reducers, scale=50, tileScale=4, crs="EPSG:4326")
    stats = stats.select(selected_cols).map(lambda f, m=m: f.set("MONTH", m))
    per_month.append(ee.FeatureCollection(ee.Algorithms.If(raw_col.size().gt(0), stats,
                                                           ee.FeatureCollection([]))))
  feats = ee_getinfo(ee.FeatureCollection(per_month).flatten())["features"]
  return [f["properties"] for f in feats]


# ---------- Task 3.2: notebook cell 38, gom 12 tháng thành 1 lần gọi ----------
def task3_all_months(commune_geom, target_prov_gid, matched_prov_name, matched_gid3, matched_cname):
  reducers = (
      ee.Reducer.sum()
      .combine(ee.Reducer.mean(), sharedInputs=True)
      .combine(ee.Reducer.stdDev(), sharedInputs=True)
      .combine(ee.Reducer.min(), sharedInputs=True)
      .combine(ee.Reducer.max(), sharedInputs=True)
      .combine(ee.Reducer.count(), sharedInputs=True)
  )
  per_month = []
  for m in MONTHS:
    s_date, e_date = _month_dates(YEAR, m)
    col_a = ee.ImageCollection(VIIRS_A).filterBounds(commune_geom).filterDate(s_date, e_date)
    col_b = ee.ImageCollection(VIIRS_B).filterBounds(commune_geom).filterDate(s_date, e_date)
    col = ee.ImageCollection(ee.Algorithms.If(col_a.size().eq(0), col_b, col_a))
    img = col.mean().clip(commune_geom)
    rad = img.select("avg_rad")
    cf_cvg = img.select("cf_cvg")
    lit_mask = rad.gte(1.5).rename("is_lit")
    lit_rad = rad.updateMask(lit_mask).rename("lit_rad")
    kw = dict(geometry=commune_geom, scale=500, maxPixels=1e9, crs="EPSG:4326")
    d = ee.Dictionary({
        "n": col.size(),
        "all": rad.reduceRegion(reducer=reducers, **kw),
        "lit": lit_rad.reduceRegion(reducer=ee.Reducer.sum(), **kw),
        "cnt": lit_mask.reduceRegion(reducer=ee.Reducer.sum(), **kw),
        "cf": cf_cvg.reduceRegion(reducer=ee.Reducer.mean(), **kw),
    })
    per_month.append(ee.Algorithms.If(col.size().gt(0), d, ee.Dictionary({"n": 0})))
  out = ee_getinfo(ee.Dictionary({"area_ha": commune_geom.area(maxError=1).divide(10000),
                                  "months": ee.List(per_month)}))
  commune_area_ha = out["area_ha"]

  # Phần tính chỉ số dưới đây chép nguyên văn notebook cell 38
  records = []
  for m, mo in zip(MONTHS, out["months"]):
    yr = YEAR
    if not mo or mo.get("n", 0) == 0:
      continue
    stats_all = mo.get("all") or {}
    stats_lit = mo.get("lit") or {}
    lit_pixel_count = (mo.get("cnt") or {}).get("is_lit", 0)
    cloud_free_obs = (mo.get("cf") or {}).get("cf_cvg", 0)

    total_pixels = stats_all.get("avg_rad_count", 0)
    tnl = stats_all.get("avg_rad_sum", 0.0) or 0.0
    mean_rad = stats_all.get("avg_rad_mean", 0.0) or 0.0
    std_rad = stats_all.get("avg_rad_stdDev", 0.0) or 0.0
    min_rad = stats_all.get("avg_rad_min", 0.0) or 0.0
    max_rad = stats_all.get("avg_rad_max", 0.0) or 0.0
    lit_pop_proxy = (stats_lit.get("lit_rad", 0.0) or 0.0)
    electrification_ratio = (
        (lit_pixel_count / total_pixels * 100.0) if total_pixels > 0 else 0.0
    )
    lit_area_ha = lit_pixel_count * 25.0
    spatial_cv = (std_rad / mean_rad) if mean_rad > 0 else 0.0

    records.append({
        "GID_1": target_prov_gid,
        "NAME_1": matched_prov_name,
        "GID_3": matched_gid3,
        "NAME_3": matched_cname,
        "YEAR": yr,
        "MONTH": m,
        "TIME": f"{yr}-{m:02d}",
        "COMMUNE_AREA_HA": round(commune_area_ha, 2),
        "TNL": round(tnl, 4),
        "MEAN_RAD": round(mean_rad, 4),
        "STD_RAD": round(std_rad, 4),
        "MIN_RAD": round(min_rad, 4),
        "MAX_RAD": round(max_rad, 4),
        "SPATIAL_CV": round(spatial_cv, 4),
        "LIT_PIXELS": int(lit_pixel_count),
        "LIT_AREA_HA": round(lit_area_ha, 2),
        "ELECTRIFICATION_RATIO_PCT": round(electrification_ratio, 2),
        "LIT_POP_PROXY": round(lit_pop_proxy, 4),
        "CLOUD_FREE_OBS": round(cloud_free_obs, 1),
    })

  df_ntl = pd.DataFrame(records)
  if df_ntl.empty:
    raise RuntimeError("Task 3.2: không tháng nào có ảnh VIIRS cho xã này.")
  df_ntl["TNL_MA3"] = df_ntl["TNL"].rolling(window=3, min_periods=1).mean()
  df_ntl["TNL_MOM_GROWTH_PCT"] = df_ntl["TNL"].pct_change() * 100.0
  return df_ntl


# =====================================================================================
# 5. TẢI ẢNH (getDownloadURL như notebook cell 21) + CHIA Ô KHI QUÁ HẠN MỨC
# =====================================================================================
_dl_lock = threading.Lock()
_dl_stats = {"ok": 0, "fail": 0, "last_err": ""}
_PERMANENT_ERR = ("401", "403", "forbidden", "unauthorized", "permission denied", "permission_denied",
                  "not authorized", "caller does not have permission")
_TOO_LARGE_ERR = ("must be less than or equal to", "request size", "too large", "request payload size",
                  "user memory limit", "pixel grid dimensions")


class PermanentError(RuntimeError):
    pass


class TooLargeError(RuntimeError):
    pass


def _dl_record(ok, err=None):
    trip = False
    with _dl_lock:
        if ok:
            _dl_stats["ok"] += 1
        else:
            _dl_stats["fail"] += 1
            _dl_stats["last_err"] = str(err)[:500]
            trip = _dl_stats["ok"] == 0 and _dl_stats["fail"] >= DOWNLOAD_FAIL_LIMIT
    if trip:
        log.error(f"{DOWNLOAD_FAIL_LIMIT} lượt tải liên tiếp thất bại, chưa có lượt nào thành công. "
                  f"Lỗi gần nhất: {_dl_stats['last_err']}")
        request_stop("fatal")


def _http_get(url, timeout=600):
    with REQUEST_GATE.slot('Image download'):
        r = requests.get(url, timeout=timeout)
    if r.status_code in (401, 403) and EE_CREDENTIALS is not None:
        try:
            from google.auth.transport.requests import AuthorizedSession
            with REQUEST_GATE.slot('Image download'):
                r2 = AuthorizedSession(EE_CREDENTIALS).get(url, timeout=timeout)
            if r2.status_code < 400:
                return r2
            r = r2
        except Exception:
            log.debug("AuthorizedSession: lỗi truy cập, không ghi URL có chữ ký")
    return r


def _classify(msg):
    low = msg.lower()
    if any(k in low for k in _TOO_LARGE_ERR):
        return "too_large"
    if any(k in low for k in _PERMANENT_ERR):
        return "permanent"
    return "transient"


def fetch_geotiff_bytes(img, region, scale, max_retry=None):
    """v6 download parameters, with bounded retry/pacing shared across all workers."""
    attempts = REQUEST_ATTEMPTS if max_retry is None else max_retry
    for attempt in range(attempts):
        check_stop()
        try:
            with REQUEST_GATE.slot("Earth Engine"):
                url = img.getDownloadURL({"region": region, "scale": scale, "crs": "EPSG:4326",
                                          "format": "GEO_TIFF", "filePerBand": False})
            r = _http_get(url)
            if r.status_code >= 400:
                if r.status_code in (401, 403):
                    raise PermanentError(f"Image download: HTTP {r.status_code}")
                if _classify(r.text or "") == "too_large":
                    raise TooLargeError("Image download: request too large")
                r.raise_for_status()
            data = r.content
            if data[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    data = z.read(next(n for n in z.namelist() if n.lower().endswith(".tif")))
            if len(data) < 200:
                raise RuntimeError("Image download: file quá ngắn")
            return data
        except (PermanentError, TooLargeError):
            raise
        except Exception as exc:
            code = status_code(exc)
            message = re.sub(r'https?://\S+', '', str(exc))
            if code != 429 and _classify(message) == "too_large":
                raise TooLargeError("Earth Engine: request too large") from None
            if code in (401, 403) or (code is None and _classify(message) == "permanent"):
                raise PermanentError(f"Earth Engine: HTTP {code or 'permission'}") from None
            if attempt == attempts - 1:
                if code == 429:
                    REQUEST_GATE.defer('Image download' if getattr(exc, 'response', None) is not None else 'Earth Engine', 0, True)
                raise RuntimeError(f"Tải ảnh: HTTP {code or 'network/invalid file'}; hết {attempts} lần thử") from None
            response = getattr(exc, "response", None)
            header = response.headers.get("Retry-After") if response is not None else None
            service = "Image download" if response is not None else "Earth Engine"
            REQUEST_GATE.defer(service, retry_delay(attempt, header), code == 429)
    raise RuntimeError("Không có lượt tải ảnh")


# =====================================================================================


def _grid_offset(a, b, res):
    """Số pixel lệch giữa hai gốc tọa độ; phải là số nguyên nếu cùng lưới."""
    k = (a - b) / res
    if abs(k - round(k)) > 1e-3:
        raise RuntimeError(f"Các ô ảnh không cùng lưới pixel (lệch {k:.4f} pixel). Không ghép để tránh sai giá trị.")
    return int(round(k))


def mosaic_tiles(tile_bytes_list):
    """Ghép các ô GeoTIFF cùng lưới pixel thành một mảng. Không resample: chỉ đặt từng ô vào đúng vị trí."""
    import rasterio
    tiles = []
    for data in tile_bytes_list:
        with rasterio.MemoryFile(data) as mf, mf.open() as src:
            tiles.append({"arr": src.read(), "tr": src.transform, "crs": src.crs, "nodata": src.nodata,
                          "dtype": src.dtypes[0], "desc": src.descriptions, "h": src.height, "w": src.width})
    t0 = tiles[0]
    resx, resy = t0["tr"].a, t0["tr"].e
    for t in tiles:
        if abs(t["tr"].a - resx) > 1e-12 or abs(t["tr"].e - resy) > 1e-12 or t["dtype"] != t0["dtype"] \
                or t["arr"].shape[0] != t0["arr"].shape[0]:
            raise RuntimeError("Các ô ảnh khác độ phân giải, kiểu dữ liệu hoặc số kênh: không ghép.")
    left = min(t["tr"].c for t in tiles)
    top = max(t["tr"].f for t in tiles)
    pos = []
    for t in tiles:
        col = _grid_offset(t["tr"].c, left, resx)
        row = _grid_offset(t["tr"].f, top, resy)
        pos.append((row, col))
    H = max(r + t["h"] for (r, _c), t in zip(pos, tiles))
    W = max(c + t["w"] for (_r, c), t in zip(pos, tiles))
    fill = t0["nodata"] if t0["nodata"] is not None else 0
    out = np.full((t0["arr"].shape[0], H, W), fill, dtype=t0["dtype"])
    filled = np.zeros((H, W), dtype=bool)
    for (r, c), t in zip(pos, tiles):
        a = t["arr"]
        if t["nodata"] is not None:
            valid = ~np.all((a == t["nodata"]) | np.isnan(a) if np.issubdtype(a.dtype, np.floating)
                            else (a == t["nodata"]), axis=0)
        else:
            valid = np.ones(a.shape[1:], dtype=bool)
        sub = out[:, r:r + t["h"], c:c + t["w"]]
        seen = filled[r:r + t["h"], c:c + t["w"]]
        # Phần chồng lấn giữa hai ô phải có giá trị trùng nhau (cùng lưới, cùng ảnh)
        both = seen & valid
        if both.any() and not np.allclose(sub[:, both], a[:, both], equal_nan=True, rtol=0, atol=0):
            raise RuntimeError("Phần chồng lấn giữa các ô ảnh không trùng giá trị: không ghép.")
        write = valid | ~seen
        sub[:, write] = a[:, write]
        seen |= valid
    from rasterio.transform import Affine
    transform = Affine(resx, 0, left, 0, resy, top)
    return out, transform, t0["crs"], t0["nodata"], t0["desc"]


def compare_on_grid(a_ref, tr_ref, a_new, tr_new):
    """a_new phải cùng lưới với a_ref, trùng giá trị ở phần chung, phần thừa (nếu có) chỉ là NoData/0."""
    if abs(tr_ref.a - tr_new.a) > 1e-12 or abs(tr_ref.e - tr_new.e) > 1e-12:
        raise RuntimeError("khác kích thước pixel")
    c = _grid_offset(tr_ref.c, tr_new.c, tr_new.a)
    r = _grid_offset(tr_ref.f, tr_new.f, tr_new.e)
    if r < 0 or c < 0 or r + a_ref.shape[1] > a_new.shape[1] or c + a_ref.shape[2] > a_new.shape[2]:
        raise RuntimeError(f"ảnh ghép không phủ hết ảnh gốc (lệch {r},{c})")
    win = a_new[:, r:r + a_ref.shape[1], c:c + a_ref.shape[2]]
    if not np.array_equal(win, a_ref, equal_nan=True):
        raise RuntimeError("giá trị pixel khác nhau ở phần chung")
    extra = np.ones(a_new.shape[1:], dtype=bool)
    extra[r:r + a_ref.shape[1], c:c + a_ref.shape[2]] = False
    if extra.any():
        e = a_new[:, extra]
        if np.any(np.nan_to_num(e, nan=0.0) != 0):
            raise RuntimeError("phần viền thừa có giá trị khác NoData")
    return True


def write_tif(path, arr, transform, crs, nodata, desc):
    import rasterio
    profile = {"driver": "GTiff", "height": arr.shape[1], "width": arr.shape[2], "count": arr.shape[0],
               "dtype": arr.dtype, "crs": crs, "transform": transform, "nodata": nodata,
               "compress": "DEFLATE", "zlevel": 9,
               "predictor": 3 if np.issubdtype(arr.dtype, np.floating) else 2,
               "tiled": True, "blockxsize": 256, "blockysize": 256}
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr)
        for i, d in enumerate(desc or [], start=1):
            if d:
                dst.set_band_description(i, d)


_COMP = []


def _best_compression():
    """ZSTD (nhỏ hơn DEFLATE ~5-10%) nếu GDAL hỗ trợ, nếu không thì DEFLATE mức 9. Cả hai đều không mất dữ liệu."""
    if not _COMP:
        import rasterio
        try:
            with rasterio.MemoryFile() as mf, mf.open(driver="GTiff", width=8, height=8, count=1, dtype="int16",
                                                      compress="ZSTD", zstd_level=19) as d:
                d.write(np.zeros((1, 8, 8), "int16"))
            _COMP.append({"compress": "DEFLATE", "zlevel": 9})   # DEFLATE: mọi phần mềm GIS đọc được
        except Exception:
            _COMP.append({"compress": "DEFLATE", "zlevel": 9})
    return _COMP[0]


def write_day_int16(path, arr, transform, crs, nodata, desc):
    """Ảnh ngày: lưu reflectance/chỉ số dưới dạng int16 = round(giá trị × DAY_SCALE_INV), scale ghi trong file.
    Với 10000: sai số tối đa 0,00005 (nửa bước 1e-4, bằng độ chính xác gốc của Sentinel-2 SR). NoData = -32768."""
    a = arr.astype("float64")
    invalid = np.isnan(a)
    if nodata is not None and not (isinstance(nodata, float) and math.isnan(nodata)):
        invalid |= (arr == nodata)
    q = np.round(a * DAY_SCALE_INV)
    over = int(np.sum((np.abs(q) > 32767) & ~invalid))
    if over:
        log.debug(f"{os.path.basename(path)}: {over} pixel vượt ngưỡng int16, bị chặn ở ±3.2767")
    q = np.clip(np.nan_to_num(q, nan=0.0), -32767, 32767).astype("int16")
    q[invalid] = DAY_NODATA
    import rasterio
    profile = {"driver": "GTiff", "height": q.shape[1], "width": q.shape[2], "count": q.shape[0], "dtype": "int16",
               "crs": crs, "transform": transform, "nodata": DAY_NODATA, "predictor": 2,
               "tiled": True, "blockxsize": 256, "blockysize": 256, **_best_compression()}
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(q)
        dst.scales = [1.0 / DAY_SCALE_INV] * q.shape[0]
        dst.offsets = [0.0] * q.shape[0]
        dst.update_tags(SCALE_FACTOR=str(1.0 / DAY_SCALE_INV),
                        NOTE=f"value = DN * {1.0 / DAY_SCALE_INV:g}; NoData = {DAY_NODATA}")
        for i, d in enumerate(desc or [], start=1):
            if d:
                dst.set_band_description(i, d)


def _rewrite_bytes_to_tif(data, path, writer):
    import rasterio
    with rasterio.MemoryFile(data) as mf, mf.open() as src:
        arr, tr, crs, nd, desc = src.read(), src.transform, src.crs, src.nodata, src.descriptions
    writer(path, arr, tr, crs, nd, desc)


def _bbox(region):
    coords = ee_getinfo(region.bounds(maxError=1))["coordinates"][0]
    xs, ys = [c[0] for c in coords], [c[1] for c in coords]
    return min(xs), min(ys), max(xs), max(ys)


def _split_bbox(bbox, n):
    x0, y0, x1, y1 = bbox
    dx, dy = (x1 - x0) / n, (y1 - y0) / n
    return [ee.Geometry.Rectangle([x0 + i * dx, y0 + j * dy, x0 + (i + 1) * dx, y0 + (j + 1) * dy],
                                  "EPSG:4326", False)
            for j in range(n) for i in range(n)]


def download_tif(img, region, scale, path, label, writer=write_tif, force_tiles=0):
    """Tải ảnh về `path`. Nếu vượt hạn mức thì chia ô (cùng scale, cùng lưới) rồi ghép.
    Trả số ô đã dùng (1 = tải nguyên). Mọi thất bại đều được ném ra kèm nguyên nhân."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    try:
        if not force_tiles:
            try:
                data = fetch_geotiff_bytes(img, region, scale)
                _rewrite_bytes_to_tif(data, tmp, writer)
                os.replace(tmp, path)
                _dl_record(True)
                return 1
            except TooLargeError as exc:
                m = re.search(r"\((\d+)\s*bytes\)", str(exc))
                ratio = (int(m.group(1)) / 50331648) if m else 4
                n = max(2, math.ceil(math.sqrt(ratio * 1.3)))
                log.info(f"{label}: vượt hạn mức tải, chia {n}x{n} ô (giữ nguyên scale={scale})")
        else:
            n = force_tiles
        if not TILING_OK[0]:
            raise RuntimeError(f"{label}: ảnh vượt hạn mức tải nhưng bước kiểm tra chia ô ở preflight không đạt, "
                               f"nên không ghép ô để tránh sai lưới pixel. Xã này cần xử lý riêng.")
        bbox = _bbox(region)
        while n <= MAX_TILE_SPLIT:
            try:
                parts = [fetch_geotiff_bytes(img, rect, scale) for rect in _split_bbox(bbox, n)]
                arr, tr, crs, nd, desc = mosaic_tiles(parts)
                writer(tmp, arr, tr, crs, nd, desc)
                os.replace(tmp, path)
                _dl_record(True)
                return n * n
            except TooLargeError:
                n += 1
                log.info(f"{label}: ô vẫn quá lớn, tăng lên {n}x{n}")
        raise RuntimeError(f"{label}: vẫn vượt hạn mức khi đã chia {MAX_TILE_SPLIT}x{MAX_TILE_SPLIT} ô")
    except StopRequested:
        raise
    except Exception as exc:
        _dl_record(False, exc)
        raise
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def inspect_tif(path, expect_bands):
    """Đọc lại file vừa ghi. Trả (ok, empty, ghi_chú)."""
    import rasterio
    try:
        with rasterio.open(path) as src:
            if src.count != expect_bands:
                return False, False, f"có {src.count} kênh, cần {expect_bands}"
            if src.crs is None or src.crs.to_epsg() != 4326:
                return False, False, f"CRS {src.crs}"
            a = src.read(1, masked=True)
            if np.issubdtype(a.dtype, np.floating):
                a = np.ma.masked_invalid(a)
            empty = a.count() == 0 or not np.any(a.filled(0) != 0)
            return True, bool(empty), ""
    except Exception as exc:
        return False, False, f"không đọc được: {exc}"



def _write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path + ".part", index=False, encoding="utf-8-sig")
    os.replace(path + ".part", path)


# =====================================================================================
# 7. ĐIỀU PHỐI V6: ngày → đêm, kiểm kê và chạy tiếp
# =====================================================================================
ADMIN_DF = None
ADMIN_BY_GID = {}
RCLONE_COMMON = ["--transfers", str(_env("VNGIS_RCLONE_TRANSFERS", 2, int)),
                 "--checkers", str(_env("VNGIS_RCLONE_CHECKERS", 4, int)),
                 "--tpslimit", str(_env("VNGIS_RCLONE_TPSLIMIT", 2.0, float)),
                 "--tpslimit-burst", str(_env("VNGIS_RCLONE_TPSLIMIT_BURST", 2, int)),
                 "--retries", "1", "--low-level-retries", "1"]
if any(float(RCLONE_COMMON[i]) <= 0 for i in (1, 3, 5, 7)):
    raise SystemExit('Giới hạn rclone phải > 0; không dùng TPS=0 để bỏ giới hạn')


def _rclone(args, timeout=1200, quiet=False):
    return rclone_run(args, timeout=timeout, allow_stopped=True).returncode == 0


# =====================================================================================
# 10. DANH SÁCH XÃ (GADM 4.1, giống notebook cell 3; đọc thẳng file .dbf, không cần geopandas)
# =====================================================================================
def read_dbf(path, encoding="utf-8"):
    """Đọc bảng thuộc tính dBase của shapefile (chỉ cần để lấy cột hành chính)."""
    with open(path, "rb") as f:
        head = f.read(32)
        n_rec, hdr_len, rec_len = struct.unpack("<IHH", head[4:12])
        fields = []
        while True:
            d = f.read(32)
            if not d or d[0] == 0x0D:
                break
            name = d[:11].split(b"\x00")[0].decode("ascii")
            fields.append((name, d[16]))
        f.seek(hdr_len)
        rows = []
        for _ in range(n_rec):
            rec = f.read(rec_len)
            if not rec or rec[0:1] == b"*":
                continue
            pos, row = 1, {}
            for name, size in fields:
                row[name] = rec[pos:pos + size].decode(encoding, errors="replace").strip()
                pos += size
            rows.append(row)
    return pd.DataFrame(rows)


def build_admin_table():
    idx_csv = os.path.join(CACHE_DIR, "gadm41_VNM_3_admin.csv")
    if os.path.isfile(idx_csv):
        return D.administrative_table(pd.read_csv(idx_csv, dtype=str, keep_default_na=False)).rename(columns=str.upper)
    os.makedirs(CACHE_DIR, exist_ok=True)
    zip_path = os.path.join(CACHE_DIR, "gadm41_VNM_shp.zip")
    if not os.path.isfile(zip_path):
        log.info("Tải ranh giới GADM 4.1 (một lần, sau đó dùng cache)...")
        r = requests.get(GADM_VNM_URL, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=900)
        r.raise_for_status()
        with open(zip_path + ".part", "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
        os.replace(zip_path + ".part", zip_path)
    with zipfile.ZipFile(zip_path) as z:
        z.extract("gadm41_VNM_3.dbf", CACHE_DIR)
        cpg = "gadm41_VNM_3.cpg"
        enc = z.read(cpg).decode().strip() if cpg in z.namelist() else "utf-8"
    enc = "utf-8" if enc.upper().replace("-", "") in ("UTF8", "") else enc
    tbl = read_dbf(os.path.join(CACHE_DIR, "gadm41_VNM_3.dbf"), enc)[ADM_COLS]
    tbl.to_csv(idx_csv, index=False, encoding="utf-8-sig")
    return D.administrative_table(pd.read_csv(idx_csv, dtype=str, keep_default_na=False)).rename(columns=str.upper)


def natural_sort_key(gid_str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(gid_str))]


def load_targets(admin):
    """Không chọn tỉnh, xã bằng tay. full: mọi xã. pilot: tự lấy PILOT_N xã đầu tiên theo thứ tự mã,
    xen kẽ phường (đô thị) và xã (nông thôn) để thử cả hai loại; thiếu loại nào thì lấy bù theo thứ tự."""
    df = admin.iloc[sorted(range(len(admin)), key=lambda i: natural_sort_key(admin.iloc[i]["GID_3"]))]
    df = df.reset_index(drop=True)
    if MODE == "full":
        return df
    t = df["TYPE_3"].str.strip().str.lower()
    pools = [list(df.index[t == "phường"]), list(df.index[t == "xã"])]
    picks = []
    while len(picks) < PILOT_N and any(pools):
        for pool in pools:
            if pool and len(picks) < PILOT_N:
                picks.append(pool.pop(0))
    for k in df.index:
        if len(picks) >= PILOT_N:
            break
        if k not in picks:
            picks.append(k)
    return df.loc[sorted(picks[:PILOT_N])].reset_index(drop=True)


# =====================================================================================
# =====================================================================================
# 11. ENTRYPOINT (luồng trực tiếp v6, không gọi batch export)
# =====================================================================================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("step", nargs="?", choices=["run", "inventory"], default="run")
    parser.add_argument("--sync-only", action="store_true")
    args = parser.parse_args()
    return pipeline_main(sys.modules[__name__], step=args.step, sync_only=args.sync_only)



# Điều phối ngày → đêm, kiểm kê và chạy tiếp.
"""Inventory and scheduling around the v6 scientific routines.

Only successfully verified/uploaded artifacts become done. JSONL is a checkpoint,
not proof of an artifact's existence. No national run executes on import.
"""
import glob
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
V = sys.modules[__name__]

log = logging.getLogger('vngis')
_drive_lock = threading.Lock()


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def safe_error(exc):
    # HTTP libraries can include a signed download URL in exception messages.
    text = re.sub(r'https?://\S+', '[URL omitted]', str(exc))
    text = re.sub(r'(?i)(token|signature|key|authorization)\s*[=:]\s*\S+', r'\1=[omitted]', text)
    return f'{type(exc).__name__}: {text[:300]}'


def rclone_run(args, timeout=1200, allow_stopped=False):
    """All Drive commands share pacing/serialization and bounded application retries."""
    common = V.RCLONE_COMMON
    for attempt in range(V.REQUEST_ATTEMPTS):
        with V.REQUEST_GATE.slot('Google Drive', allow_stopped), _drive_lock:
            result = subprocess.run(['rclone', *args, *common], capture_output=True,
                                    text=True, timeout=timeout)
        throttled = bool(re.search(r'\b429\b|rateLimitExceeded|userRateLimitExceeded', result.stderr, re.I))
        if result.returncode == 0:
            if throttled:
                V.REQUEST_GATE.defer('Google Drive', 0, True)
            return result
        transient = throttled or bool(re.search(r'\b(500|502|503|504)\b', result.stderr))
        if not transient or attempt == V.REQUEST_ATTEMPTS - 1:
            if throttled:
                V.REQUEST_GATE.defer('Google Drive', 0, True)
                log.error('Google Drive: hết %s lần thử vì giới hạn request', V.REQUEST_ATTEMPTS)
            return result
        header = re.search(r'(?im)^\s*Retry-After:\s*([^\r\n]+)', result.stderr)
        V.REQUEST_GATE.defer('Google Drive', retry_delay(attempt, header[1] if header else None), throttled)
    raise RuntimeError('Không có lượt rclone')


class Drive:
    def __init__(self, base=None, local=None):
        self.base = base or V.REMOTE_BASE
        self.local = Path(local or V.LOCAL_ROOT)
        self.backup_stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + uuid.uuid4().hex[:8]

    def command(self, args, optional=False, allow_stopped=False):
        result = rclone_run(args, allow_stopped=allow_stopped)
        if result.returncode:
            if optional and re.search(r'directory not found|object not found|file not found', result.stderr, re.I):
                return None
            raise RuntimeError(f'Google Drive: {args[0]} thất bại (mã {result.returncode}); '
                               'kiểm tra quyền, kết nối hoặc quota')
        return result.stdout

    def fetch(self, rel, dest, optional=False):
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        text = self.command(['copyto', f'{self.base}/{rel}', str(dest)], optional)
        return dest if text is not None else None

    def listing(self, rel):
        raw = self.command(['lsjson', f'{self.base}/{rel}', '-R', '--files-only', '--hash'], optional=True)
        return json.loads(raw) if raw is not None else []

    def stat(self, rel, allow_stopped=False):
        raw = self.command(['lsjson', f'{self.base}/{rel}', '--stat', '--hash'], optional=True,
                           allow_stopped=allow_stopped)
        return json.loads(raw) if raw is not None else None

    def _put(self, path, rel, backup=True):
        # This is also the final flush after STOP; new scientific work stays stopped.
        if backup and self.stat(rel, allow_stopped=True) is not None:
            backup_id = self.backup_stamp + '_' + uuid.uuid4().hex[:8]
            self.command(['copyto', f'{self.base}/{rel}',
                          f'{self.base}/_control/backups/{backup_id}/{rel}'], allow_stopped=True)
        temporary = f'{self.base}/_control/staging/{uuid.uuid4().hex}.part'
        self.command(['copyto', str(path), temporary], allow_stopped=True)
        self.command(['moveto', temporary, f'{self.base}/{rel}'], allow_stopped=True)

    def put(self, path, rel, backup=True):
        # Keep an immutable local outbox so --sync-only can retry a failed final upload.
        outbox = self.local/'_control/outbox'/uuid.uuid4().hex
        outbox.mkdir(parents=True, exist_ok=True)
        payload = outbox/'payload'
        path = Path(path)
        if path.suffix in {'.csv', '.tif', '.json'}:
            # These writers replace files atomically; a hard link preserves the old inode.
            try:
                os.link(path, payload)
            except OSError:
                shutil.copyfile(path, payload)
        else:
            shutil.copyfile(path, payload)
        (outbox/'manifest.json').write_text(json.dumps(dict(base=self.base, rel=rel, backup=backup)))
        self._put(payload, rel, backup)
        shutil.rmtree(outbox)

    def flush_outbox(self):
        for path in sorted((self.local/'_control/outbox').glob('*/manifest.json'), key=lambda p: p.stat().st_mtime_ns):
            job = json.loads(path.read_text())
            if job['base'] != self.base:
                raise ValueError('Outbox thuộc thư mục Drive khác; không tự đổi đích upload')
            self._put(path.parent/'payload', job['rel'], job['backup'])
            shutil.rmtree(path.parent)

    def pull_history(self):
        for sub in ['_control/status', '_control/parts']:
            log.info('KIỂM KÊ: đọc lịch sử %s', sub)
            for entry in self.listing(sub):
                if entry['Path'].endswith('.jsonl'):
                    self.fetch(f"{sub}/{entry['Path']}", self.local/sub/entry['Path'])


def fingerprint(entry):
    return dict(size=entry.get('Size'), mtime=entry.get('ModTime'),
                hashes=entry.get('Hashes', {}), version=D.VALIDATION_VERSION)


def read_jsonl(path):
    lines = Path(path).read_text(encoding='utf-8').splitlines(keepends=True)
    for index, line in enumerate(lines):
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            if index == len(lines) - 1 and not line.endswith('\n'):
                log.warning('Bỏ dòng cuối JSONL chưa ghi xong: %s', Path(path).name)
            else:
                raise ValueError(f'JSONL hỏng: {Path(path).name}, dòng {index + 1}') from None


class Engine:
    def __init__(self, admin, drive=None):
        self.admin = D.administrative_table(admin, expected=None)
        self.drive = drive or Drive()
        self.root = self.drive.local
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.sync_lock = threading.Lock()
        self.contexts = {r['gid_3']: V.build_ctx({k.upper(): v for k, v in r.items()})
                         for r in self.admin.to_dict('records')}
        self.mapping = {c['safe_gid3']: g for g, c in self.contexts.items()}
        self.sources = {}
        self.cache = {}
        self.images = {}
        self.frames = {}
        self.table = None
        self.positions = {}
        self.history = {}
        self.dirty = set()
        self.last_sync = time.monotonic()
        self.run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + V.RUN_ID + '_' + uuid.uuid4().hex[:8]

    def save_cache(self, cache):
        path = self.root/'_control/validated_images.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.json.part')
        temporary.write_text(json.dumps(cache), encoding='utf-8')
        temporary.replace(path)
        self.drive.put(path, '_control/validated_images.json', backup=False)

    def scan_images(self):
        old = self.cache.copy()
        current = {}
        self.images = {}
        new_checks, reused, dirty = 0, 0, False
        last_save = time.monotonic()
        every = max(1, int(os.getenv('VNGIS_INVENTORY_CHECKPOINT_EVERY', '250')))
        seen = set()
        try:
            for kind in ['day', 'night']:
                log.info('KIỂM KÊ %s: đang liệt kê Drive', kind)
                listing = self.drive.listing(kind.title())
                log.info('KIỂM KÊ %s: %s file, bắt đầu đối chiếu', kind, len(listing))
                for i, entry in enumerate(listing, 1):
                    match = D.IMAGE_RE.match(Path(entry['Path']).name)
                    if not match or match[2] != kind or match[1] not in self.mapping:
                        continue
                    V.check_stop()
                    key = (kind, self.mapping[match[1]], int(match[3]))
                    rel = f"{kind.title()}/{entry['Path']}"
                    if key in seen:
                        self.images[key] = 'failed', 'Có nhiều file cùng xã/tháng'
                        continue
                    seen.add(key)
                    fp = fingerprint(entry)
                    previous = old.get(rel, {})
                    if previous.get('fingerprint') == fp and previous.get('state') in {'done', 'failed'}:
                        state, error = previous['state'], previous.get('error', '')
                        reused += 1
                    else:
                        log.info('KIỂM KÊ %s %s/%s: tải %s (%.1f MB)', kind, i, len(listing), rel,
                                 entry.get('Size', 0)/1e6)
                        size = entry.get('Size', 0)
                        if shutil.disk_usage(self.root).free < size + 512 * 1024 ** 2:
                            raise RuntimeError('Không đủ dung lượng kiểm tra ảnh; cache đã hoàn tất sẽ được giữ')
                        start = time.monotonic()
                        path = self.root/'_control/validation'/Path(rel).name
                        self.drive.fetch(rel, path)
                        try:
                            log.info('KIỂM KÊ: đã tải, đang đọc mọi block GeoTIFF')
                            state, error = D.validate_image(path, kind)
                        finally:
                            path.unlink(missing_ok=True)
                        new_checks += 1
                        dirty = True
                        log.info('KIỂM KÊ: %s trong %.1fs; mới %s, dùng cache %s',
                                 state, time.monotonic()-start, new_checks, reused)
                    current[rel] = dict(fingerprint=fp, state=state, error=error)
                    self.images[key] = state, error
                    if dirty and (new_checks % every == 0 or time.monotonic()-last_save >= 120):
                        self.save_cache({**old, **current})
                        dirty, last_save = False, time.monotonic()
                    elif reused and reused % every == 0:
                        log.info('KIỂM KÊ: đã tái sử dụng %s kết quả kiểm tra', reused)
            self.save_cache(current)
            self.cache = current
            dirty = False
        finally:
            if dirty:
                self.cache = {**old, **current}
                try:
                    self.save_cache(self.cache)
                except Exception as exc:
                    log.error('Không lưu được cache giữa chừng: %s', safe_error(exc))

    def inventory(self):
        log.info('KIỂM KÊ: đọc tiến độ, nguồn và cache cũ')
        previous = self.drive.fetch('_control/progress.csv', self.root/'_control/progress.csv', True)
        prior = pd.read_csv(previous) if previous else None
        source = self.drive.fetch('_control/source_counts.csv', self.root/'_control/source_counts.csv', True)
        self.sources = D.read_sources(source) if source else {}
        if set(g for g, _, _ in self.sources) - set(self.contexts):
            raise ValueError('Bằng chứng nguồn chứa GID ngoài phạm vi thư mục')
        cache = self.drive.fetch('_control/validated_images.json', self.root/'_control/validated_images.json', True)
        self.cache = json.loads(cache.read_text()) if cache else {}
        self.drive.pull_history()
        for path in sorted((self.root/'_control/status').glob('status_*.jsonl')):
            for record in read_jsonl(path):
                if 'gid_3' in record:
                    self.history[record['gid_3']] = record
                    for item in record.get('source_counts', []):
                        self.sources[record['gid_3'], int(item['month']), item['field']] = item['count']
        self.scan_images()
        for kind in ['day', 'night']:
            log.info('KIỂM KÊ: đọc CSV %s và parts v6', kind)
            rel = f'CSV/{kind}_indices.csv'
            path = self.drive.fetch(rel, self.root/rel, True)
            original = pd.read_csv(path) if path else pd.DataFrame()
            frame = D.normalize(original, self.admin, kind)
            parts = [r for p in sorted((self.root/'_control/parts').glob(f'{kind}_*.jsonl')) for r in read_jsonl(p)]
            if parts:
                # Ignore measurements already backed by a valid CSV, preserving them exactly.
                states = D.metric_states(frame, kind, self.sources)
                rows = D.aliases(pd.DataFrame(parts))
                selected = [states.get((str(r.gid_3), int(r.month)), ('pending', ''))[0] not in D.TERMINAL
                            for r in rows.itertuples()]
                frame = D.merge_valid(frame, rows.loc[selected], self.admin, kind, self.sources)
            self.frames[kind] = frame
            if path is None or not D.same_table(frame, original):
                self.dirty.add(kind)
        self.table = D.progress(self.admin, self.frames, self.images, self.sources, prior)
        self.table['updated_at'] = stamp()
        self.positions = {(r.gid_3, int(r.month)): i for i, r in enumerate(self.table.itertuples())}
        self.checkpoint(force=True)
        for field in D.FIELDS:
            log.info('KIỂM KÊ %s: %s', field, self.table[field].value_counts().to_dict())
        return self.table

    def checkpoint(self, force=False):
        if not force and time.monotonic()-self.last_sync < V.UPLOAD_EVERY_SEC:
            return
        with self.sync_lock, self.lock:
            if not force and time.monotonic()-self.last_sync < V.UPLOAD_EVERY_SEC:
                return
            for kind in sorted(self.dirty):
                rel = f'CSV/{kind}_indices.csv'
                path = self.root/rel
                V._write_csv(self.frames[kind], str(path))
                self.drive.put(path, rel, backup=True)
            self.dirty.clear()
            sources = pd.DataFrame([dict(gid_3=g, year=D.YEAR, month=m, field=f, count=n)
                                    for (g, m, f), n in sorted(self.sources.items())],
                                   columns=D.KEY+['field', 'count'])
            for rel, frame in [('_control/source_counts.csv', sources), ('_control/progress.csv', self.table)]:
                V._write_csv(frame, str(self.root/rel))
                self.drive.put(self.root/rel, rel, backup=False)
            self.save_cache(self.cache)
            for sub in ['status', 'parts']:
                for path in (self.root/'_control'/sub).glob(f'*_{self.run}.jsonl'):
                    self.drive.put(path, f'_control/{sub}/{path.name}', backup=False)
            self.last_sync = time.monotonic()
            log.info('CHECKPOINT: đã lưu CSV, nguồn, cache và tiến độ xã–tháng lên Drive')

    def update(self, gid, month, field, state, error=''):
        with self.lock:
            i = self.positions[gid, month]
            self.table.loc[i, [field, field+'_error', 'updated_at']] = [state, error, stamp()]
            self.write_status(gid, month)

    def write_status(self, gid, month=None, include_sources=False):
        rows = self.table.iloc[[self.positions[gid, m] for m in D.MONTHS]]
        info = dict(gid_3=gid, run_id=V.RUN_ID, version='v6-resume',
                    phase_attempts=self.history.get(gid, {}).get('phase_attempts', {}),
                    status='done' if rows[D.FIELDS].isin(D.TERMINAL).all().all() else 'partial',
                    finished_at=stamp())
        if month is not None:
            info['month_status'] = self.table.iloc[self.positions[gid, month]].to_dict()
        if include_sources:
            info['source_counts'] = [dict(month=m, field=f, count=self.sources[gid,m,f])
                                    for m in D.MONTHS for f in D.FIELDS if (gid,m,f) in self.sources]
        self.history[gid] = {**self.history.get(gid, {}), **info}
        path = self.root/f'_control/status/status_{self.run}.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(info, ensure_ascii=False)+'\n')

    def needed(self, gid, field):
        with self.lock:
            rows = self.table.iloc[[self.positions[gid, m] for m in D.MONTHS]]
            return list(rows.loc[~rows[field].isin(D.TERMINAL), 'month'].astype(int))

    def add_metrics(self, gid, kind, incoming, months):
        normalized = D.normalize(incoming, self.admin.loc[self.admin.gid_3.eq(gid)], kind)
        with self.lock:
            for month in months:
                row = normalized.loc[normalized.month.eq(month)].iloc[0]
                field = kind+'_indices'
                count = self.sources.get((gid, month, field))
                state, error = D.metric_state(row, kind, count)
                if state == 'pending' and count and count > 0:
                    state, error = 'failed', 'Có nguồn nhưng phép tính không trả chỉ số tháng này'
                i = self.positions[gid, month]
                if state in D.TERMINAL and self.table.loc[i, field] not in D.TERMINAL:
                    metrics = D.DAY_METRICS if kind == 'day' else D.NIGHT_METRICS
                    self.frames[kind].loc[i, metrics] = row[metrics].to_numpy()
                    self.dirty.add(kind)
                    path = self.root/f'_control/parts/{kind}_{self.run}.jsonl'
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with path.open('a', encoding='utf-8') as fh:
                        fh.write(row.to_json(force_ascii=False)+'\n')
                self.update(gid, month, field, state, error)

    def check_asset(self, full_admin):
        gids = V.ee_getinfo(V.communes_fc.aggregate_array('GID_3'))
        expected = set(D.administrative_table(full_admin).gid_3)
        actual = set(gids)
        missing, extra = expected-actual, actual-expected
        if missing or extra or len(gids) != len(actual):
            message = f'Asset không khớp GADM: thiếu {len(missing)}, ngoài phạm vi {len(extra)}, trùng {len(gids)-len(actual)}; thiếu mẫu {sorted(missing)[:10]}'
            for gid in missing & set(self.contexts):
                for month in D.MONTHS:
                    self.update(gid, month, 'day_image', 'failed', message)
            self.checkpoint(force=True)
            raise ValueError(message)

    def process(self, gid, kind):
        V.check_stop()
        if kind == 'night':
            D.require_day_complete(self.table)
        ctx = self.contexts[gid].copy()
        fc = V.communes_fc.filter(V.ee.Filter.eq('GID_3', gid))
        geom = fc.geometry()
        ctx['geom'] = geom
        missing = {field: self.needed(gid, kind+'_'+field) for field in ['image', 'indices']}
        if not any(missing.values()):
            return
        for suffix, months in missing.items():
            for month in months:
                self.update(gid, month, kind+'_'+suffix, 'running')
        try:
            n, plan = V.fetch_plan(fc, geom, kind)
            if n != 1:
                raise ValueError(f'Asset phải có đúng một feature cho GID {gid}; có {n}')
            with self.lock:
                for month, entry in plan.items():
                    for suffix in ['image', 'indices']:
                        field = kind+'_'+suffix
                        count = entry[suffix+'_count']
                        self.sources[gid, month, field] = count
                        if month in missing[suffix] and count == 0:
                            if suffix == 'indices':
                                i = self.positions[gid, month]
                                state, error = D.metric_state(self.frames[kind].iloc[i], kind, 0)
                                self.update(gid, month, field, state, error)
                            else:
                                image_state = self.images.get((kind, gid, month))
                                if image_state and image_state[0] == 'failed':
                                    self.update(gid, month, field, 'failed', 'Nguồn rỗng nhưng file ảnh hiện có hỏng/trùng')
                                else:
                                    self.update(gid, month, field, 'no_source')
                self.write_status(gid, include_sources=True)
            months = self.needed(gid, kind+'_indices')
            sourced = [m for m in months if plan[m]['indices_count'] > 0]
            if sourced:
                try:
                    if kind == 'day':
                        props = V.task1_all_months(fc, months=sourced)
                        incoming = pd.DataFrame([{**r, 'YEAR': D.YEAR} for r in props])
                    else:
                        # All sourced months are needed to preserve v6 rolling/pct_change semantics.
                        incoming = V.task3_all_months(geom, ctx['gid1'], ctx['name1'], gid, ctx['cname_full'])
                    self.add_metrics(gid, kind, incoming, sourced)
                    self.checkpoint()
                except Exception as exc:
                    for month in sourced:
                        self.update(gid, month, kind+'_indices', 'failed', safe_error(exc))
            jobs = self.needed(gid, kind+'_image')
            with ThreadPoolExecutor(max_workers=V.MONTH_THREADS) as pool:
                futures = {pool.submit(self.process_image, gid, kind, m, ctx, plan[m]): m for m in jobs}
                for future in as_completed(futures):
                    month = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        self.update(gid, month, kind+'_image', 'failed', safe_error(exc))
        except Exception as exc:
            for suffix in ['image', 'indices']:
                for month in self.needed(gid, kind+'_'+suffix):
                    self.update(gid, month, kind+'_'+suffix, 'failed', safe_error(exc))
        finally:
            self.checkpoint()

    def process_image(self, gid, kind, month, ctx, entry):
        V.check_stop()
        if entry['image_count'] == 0:
            return
        rel = str(Path(ctx['rel_'+kind+'_dir'])/(V.day_name(ctx, month) if kind == 'day' else V.night_name(ctx, month)))
        image = (V.day_image(month, entry['s2_window'], ctx['geom']).select(V.DAY_BANDS_ALL)
                 if kind == 'day' else V.night_image(month, entry['viirs'], ctx['geom']))
        path = self.root/'_control/candidates'/Path(rel).name
        min_free = V._env('VNGIS_MIN_FREE_GB', 6.0, float) * 1024 ** 3
        if shutil.disk_usage(self.root).free < min_free:
            raise RuntimeError('Dung lượng đĩa dưới ngưỡng VNGIS_MIN_FREE_GB; không bắt đầu tải ảnh mới')
        V.download_tif(image, ctx['geom'], 20 if kind == 'day' else 500, str(path), f'[{gid}] {kind}/{month}')
        state, error = D.validate_image(path, kind)
        if state != 'done':
            raise ValueError(error)
        self.drive.put(path, rel, backup=True)
        stat = self.drive.stat(rel, allow_stopped=True)
        if stat is None:
            raise RuntimeError('File vừa upload không tồn tại trên Drive')
        with self.lock:
            self.cache[rel] = dict(fingerprint=fingerprint(stat), state='done', error='')
            self.images[kind, gid, month] = 'done', ''
            self.update(gid, month, kind+'_image', 'done')
        path.unlink()
        self.checkpoint()

    def preflight(self, kind):
        if kind == 'night':
            D.require_day_complete(self.table)
        gid = next((g for g in self.contexts if self.needed(g, kind+'_image') or self.needed(g, kind+'_indices')), None)
        if gid is None:
            return
        fc = V.communes_fc.filter(V.ee.Filter.eq('GID_3', gid))
        geom = fc.geometry()
        _, plan = V.fetch_plan(fc, geom, kind)
        month = next((m for m in D.MONTHS if plan[m]['image_count'] > 0), None)
        if month is None:
            log.info('PREFLIGHT %s: không có nguồn tại xã mẫu; worker sẽ xác minh no_source', kind)
            return
        region = geom.centroid(maxError=1).buffer(1500)
        image = (V.day_image(month, plan[month]['s2_window'], geom).select(V.DAY_BANDS_ALL)
                 if kind == 'day' else V.night_image(month, plan[month]['viirs'], geom)).clip(region)
        scale = 20 if kind == 'day' else 500
        whole = V.fetch_geotiff_bytes(image, region, scale)
        import rasterio
        with rasterio.MemoryFile(whole) as memory, memory.open() as source:
            if source.count != (10 if kind == 'day' else 2) or source.crs.to_epsg() != 4326:
                raise ValueError('PREFLIGHT: số kênh/CRS sai')
            array, transform = source.read(), source.transform
        if V.PREFLIGHT_TILE_TEST:
            V.TILING_OK[0] = False
            parts = [V.fetch_geotiff_bytes(image, rect, scale) for rect in V._split_bbox(V._bbox(region), 2)]
            tiled, tiled_transform, *_ = V.mosaic_tiles(parts)
            V.compare_on_grid(array, transform, tiled, tiled_transform)
            V.TILING_OK[0] = True
        log.info('PREFLIGHT %s: đạt; không truy vấn ảnh của giai đoạn khác', kind)

    def run_phase(self, kind):
        if kind == 'night':
            D.require_day_complete(self.table)
        fields = [kind+'_image', kind+'_indices']
        if self.table[fields].isin(D.TERMINAL).all().all():
            return True
        exhausted = [g for g in self.contexts if any(self.needed(g, f) for f in fields)
                     and self.history.get(g, {}).get('phase_attempts', {}).get(kind, 0) >= V.MAX_ATTEMPTS]
        if len(exhausted) == sum(any(self.needed(g, f) for f in fields) for g in self.contexts):
            log.error('Giai đoạn %s đã hết số lần thử; không gửi preflight/request mới', kind)
            return False
        if V.PREFLIGHT:
            self.preflight(kind)
        while True:
            V.check_stop()
            jobs = []
            for gid in self.contexts:
                if not any(self.needed(gid, f) for f in fields):
                    continue
                attempts = self.history.get(gid, {}).get('phase_attempts', {}).get(kind, 0)
                if attempts < V.MAX_ATTEMPTS:
                    jobs.append(gid)
            if not jobs:
                break
            log.info('GIAI ĐOẠN %s: còn %s xã cần xử lý', kind, len(jobs))
            with ThreadPoolExecutor(max_workers=V.N_WORKERS) as pool:
                futures = {}
                def attempt_process(gid):
                    V.check_stop()
                    with self.lock:
                        previous = self.history.setdefault(gid, {})
                        counts = previous.setdefault('phase_attempts', {})
                        counts[kind] = counts.get(kind, 0)+1
                        self.write_status(gid)
                    self.process(gid, kind)
                for gid in jobs:
                    futures[pool.submit(attempt_process, gid)] = gid
                for future in as_completed(futures):
                    future.result()
            self.checkpoint(force=True)
        complete = self.table[fields].isin(D.TERMINAL).all().all()
        if not complete:
            log.error('Giai đoạn %s còn thiếu/lỗi sau giới hạn thử; dừng nối lượt tự động', kind)
        return bool(complete)


def pipeline_main(module, step='run', sync_only=False):
    global V
    V = module
    V.setup_logging()
    V.install_signal_handlers()
    timer = None
    engine = None
    watcher = None
    watcher_stop = threading.Event()
    code = 1
    if V.MAX_RUNTIME_SEC > 0:
        timer = threading.Timer(V.MAX_RUNTIME_SEC, V.request_stop, args=('deadline',))
        timer.daemon = True
        timer.start()
    try:
        if sync_only:
            Drive().flush_outbox()
            log.info('sync-only: đã gửi nốt outbox, không ghi đè bằng CSV cũ ngoài outbox')
            return 0
        admin = V.build_admin_table()
        targets = V.load_targets(admin)
        engine = Engine(targets)
        engine.drive.flush_outbox()
        if engine.drive.stat('_control/STOP') is not None:
            return 130
        def watch_stop():
            while not watcher_stop.wait(V.DRIVE_STOP_POLL_SEC):
                try:
                    if engine.drive.stat('_control/STOP') is not None:
                        V.request_stop('drive_stop')
                        return
                except V.StopRequested:
                    return
                except Exception as exc:
                    log.warning('Không kiểm tra được STOP trên Drive: %s', safe_error(exc))
        watcher = threading.Thread(target=watch_stop, daemon=True, name='drive-stop')
        watcher.start()
        engine.inventory()
        if step == 'inventory':
            return 0
        V.init_earth_engine()
        engine.check_asset(admin)
        if V._env('VNGIS_RETRY_FAILED', False, bool):
            for gid in engine.contexts:
                engine.history.setdefault(gid, {}).setdefault('phase_attempts', {}).pop('day', None)
        V.TILING_OK[0] = False  # Only successful preflight verifies the tiling grid.
        if not engine.run_phase('day'):
            return 1
        # Read actual uploaded files and CSVs again before admitting the night phase.
        engine.inventory()
        D.require_day_complete(engine.table)
        if V._env('VNGIS_RETRY_FAILED', False, bool):
            for gid in engine.contexts:
                engine.history.setdefault(gid, {}).setdefault('phase_attempts', {}).pop('night', None)
        V.TILING_OK[0] = False
        code = 0 if engine.run_phase('night') else 1
        return code
    except V.StopRequested:
        return {'deadline': 3, 'fatal': 1}.get(V.STOP_REASON[0], 130)
    except Exception as exc:
        log.error('LỖI: %s', safe_error(exc))
        return 1
    finally:
        watcher_stop.set()
        if watcher:
            watcher.join(timeout=5)
        if timer:
            timer.cancel()
        if engine is not None and engine.table is not None:
            try:
                engine.checkpoint(force=True)
                for path in (engine.root/'_control/logs').glob('*.log'):
                    engine.drive.put(path, f'_control/logs/{path.name}', backup=False)
            except Exception as exc:
                log.error('Không lưu được checkpoint cuối lượt: %s', safe_error(exc))
                return 1


if __name__ == "__main__" and os.environ.get("VNGIS_SKIP_MAIN") != "1":
    sys.exit(main())
