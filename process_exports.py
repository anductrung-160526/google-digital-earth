# -*- coding: utf-8 -*-
"""Xử lý kết quả batch export trên GitHub Actions, ghi vào VNGISDash_2024 đúng cấu trúc cũ.

    python process_exports.py inventory     kiểm kê Drive -> _control/progress.csv
    python process_exports.py day-csv       gộp CSV chỉ số ảnh ngày -> CSV/day_indices.csv
    python process_exports.py day-img       cắt ảnh ngày toàn quốc theo xã -> Day/<tỉnh>/<xã>/...
    python process_exports.py night-csv     tính chỉ số đêm (notebook cell 38) -> CSV/night_indices.csv
    python process_exports.py night-img     cắt ảnh đêm toàn quốc theo xã -> Night/<tỉnh>/<xã>/...
    python process_exports.py day           = day-csv + day-img + inventory
    python process_exports.py night         = night-csv + night-img + inventory
    Thêm --compare N: với N xã đã có ảnh trên Drive, cắt thử và so sánh từng pixel với ảnh cũ.

Mã thoát: 0 xong | 1 lỗi | 3 hết giờ hoặc còn chờ Earth Engine (workflow tự nối lượt)
"""

import os, re, sys, json, math, time, argparse, subprocess, threading
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone

import numpy as np
import pandas as pd

os.environ.setdefault('VNGIS_MODE', 'full')
os.environ.setdefault('VNGIS_DRIVE_FOLDER', 'VNGISDash_2024')
import vngis_2024 as V
import batch_config as B
import data_contract as D
from pathlib import Path

log = V.log
EXP = f"{V.RCLONE_REMOTE}:{B.EXPORT_FOLDER}"
WORK = os.environ.get("VNGIS_BATCH_WORK", os.path.expanduser("~/vngis_batch"))
WAIT_POLL_SEC = 300
STABLE_SEC = 900              # nếu không đọc được trạng thái tác vụ: file export phải "đứng yên" 15 phút mới xử lý
COMPARE_N = 0

ADMIN = None
PROGRESS = None
SOURCES = {}
IMAGE_CACHE = {}
CTX = {}                      # GID_3 -> ctx (đường dẫn, tên file)
SAFE2GID = {}


# ------------------------------------------------------------------------------------
# Tiện ích
# ------------------------------------------------------------------------------------
def rclone_json(remote, *extra):
    res = subprocess.run(["rclone", "lsjson", remote, "--files-only", "--fast-list", *extra],
                         capture_output=True, text=True, timeout=3600)
    if res.returncode != 0:
        if "directory not found" in res.stderr:
            return []
        raise RuntimeError(f"rclone lsjson {remote}: {res.stderr.strip()[-300:]}")
    return json.loads(res.stdout or "[]")


def fetch(remote_file, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    ok = V._rclone(["copyto", remote_file, local_path, "--multi-thread-streams", "8", "--retries", "5"], timeout=7200)
    if not ok or not os.path.isfile(local_path):
        raise RuntimeError(f"Không tải được {remote_file}")
    return local_path


def fetch_optional(remote_file, local_path):
    # Permission/network failures must never masquerade as a missing optional file.
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if os.path.isfile(local_path):
        os.remove(local_path)
    res = subprocess.run(["rclone", "copyto", remote_file, local_path], capture_output=True, text=True, timeout=3600)
    if res.returncode:
        if "directory not found" in res.stderr or "object not found" in res.stderr:
            return None
        raise RuntimeError(f"Không đọc được {remote_file}: {res.stderr.strip()[-300:]}")
    return local_path if os.path.isfile(local_path) else None


def upload_atomic(path, rel, backup=True):
    """Stage and verify upload before replacing a target; retain the previous file."""
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    target = f"{V.REMOTE_BASE}/{rel}"
    if backup:
        existing = fetch_optional(target, os.path.join(WORK, 'backup', rel))
        if existing:
            dest = f"{V.REMOTE_BASE}/_control/backups/{stamp}/{rel}"
            if not V._rclone(['copyto', existing, dest]):
                raise RuntimeError(f'Không sao lưu được {rel}; giữ nguyên bản cũ')
    staged = target + f'.{stamp}.part'
    if not V._rclone(['copyto', path, staged]) or not V._rclone(['moveto', staged, target]):
        raise RuntimeError(f'Không ghi nguyên tử được {rel}; giữ file tạm để kiểm tra')


def checkpoint(field, gids, month, state, error=''):
    global PROGRESS
    if PROGRESS is None:
        return
    selected = PROGRESS['gid_3'].isin(gids)
    if month is not None:
        selected &= PROGRESS['month'].eq(month)
    # Do not downgrade evidence of valid output or confirmed no-source.
    selected &= ~PROGRESS[field].isin(D.TERMINAL)
    PROGRESS.loc[selected, field] = state
    PROGRESS.loc[selected, field + '_error'] = error
    path = V.L(V.D_CONTROL, 'progress.csv')
    V._write_csv(PROGRESS, path)
    upload_atomic(path, '_control/progress.csv', backup=False)


def flush_logs():
    for handler in log.handlers:
        handler.flush()
    folder = Path(V.L(V.D_LOGS))
    if folder.is_dir():
        for path in folder.glob('*.log'):
            upload_atomic(str(path), f'_control/logs/{path.name}', backup=False)


def save_sources():
    rows = [dict(gid_3=g, year=V.YEAR, month=m, field=f, count=c)
            for (g, m, f), c in SOURCES.items()]
    path = V.L(V.D_CONTROL, 'source_counts.csv')
    V._write_csv(pd.DataFrame(rows, columns=D.KEY + ['field', 'count']), path)
    upload_atomic(path, '_control/source_counts.csv')


def free_gb(path=None):
    import shutil
    path = path or WORK
    os.makedirs(path, exist_ok=True)
    return shutil.disk_usage(path).free / 1e9


def summary(md):
    p = os.environ.get("GITHUB_STEP_SUMMARY")
    if p:
        with open(p, "a", encoding="utf-8") as f:
            f.write(md + "\n")


def setup():
    global ADMIN
    if V.DRIVE_FOLDER != 'VNGISDash_2024':
        raise RuntimeError('Batch yêu cầu VNGIS_DRIVE_FOLDER=VNGISDash_2024; không ghi vào thư mục khác')
    for d in (V.LOCAL_ROOT, V.L(V.D_CONTROL), V.L(V.D_LOGS), V.CACHE_DIR, WORK):
        os.makedirs(d, exist_ok=True)
    V.setup_logging()
    ADMIN = V.build_admin_table()
    D.administrative_table(ADMIN)  # fail explicitly if scope is not exactly 11,136
    CTX.clear()
    SAFE2GID.clear()
    V.ADMIN_DF = ADMIN
    V.ADMIN_BY_GID = {r["GID_3"]: r for r in ADMIN.to_dict("records")}
    for r in ADMIN.to_dict("records"):
        c = V.build_ctx(r)
        CTX[r["GID_3"]] = c
        SAFE2GID[c["safe_gid3"]] = r["GID_3"]
    log.info(f"GADM: {len(ADMIN):,} xã | Drive đích {V.REMOTE_BASE} | thư mục export {EXP}")


def load_plan():
    p = fetch_optional(f"{EXP}/{B.PLAN_DAY}", os.path.join(WORK, B.PLAN_DAY))
    if not p:
        raise FileNotFoundError(f"Chưa có {B.PLAN_DAY} trong {EXP}. Chạy cx.compute_day_plan() trong Colab trước.")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------------------------------------------
# 1. Kiểm kê
# ------------------------------------------------------------------------------------
_TIF_RE = re.compile(r"^(.+)_(day|night)_2024(\d\d)\.tif$")


def list_existing():
    """Validate actual Drive files; reuse only unchanged, previously decoded images."""
    global IMAGE_CACHE
    path = fetch_optional(f'{V.REMOTE_BASE}/_control/validated_images.json',
                          os.path.join(WORK, 'validated_images.json'))
    cache = json.loads(Path(path).read_text(encoding='utf-8')) if path else {}
    have = {'day': {}, 'night': {}}
    IMAGE_CACHE = {}
    next_cache = {}
    seen = set()
    for kind in ['day', 'night']:
        listing = rclone_json(f'{V.REMOTE_BASE}/{kind.title()}', '-R', '--hash', '--include', '*.tif')
        for f in listing:
            match = D.IMAGE_RE.match(os.path.basename(f['Path']))
            if not match or match[2] != kind or match[1] not in SAFE2GID:
                continue
            V.check_stop()
            gid, month = SAFE2GID[match[1]], int(match[3])
            key = kind, gid, month
            relative = f"{kind.title()}/{f['Path']}"
            fingerprint = {'size': f.get('Size'), 'mtime': f.get('ModTime'),
                           'hashes': f.get('Hashes', {}), 'version': D.VALIDATION_VERSION}
            if key in seen:
                IMAGE_CACHE[key] = 'failed', 'Có nhiều file cùng xã/tháng'
                have[kind].get(gid, set()).discard(month)
                continue
            seen.add(key)
            previous = cache.get(relative, {})
            if previous.get('fingerprint') == fingerprint and previous.get('state') == 'done':
                state, error = 'done', ''
            else:
                local = os.path.join(WORK, 'validate', os.path.basename(f['Path']))
                _wait_space(f.get('Size', 0))
                fetch(f'{V.REMOTE_BASE}/{relative}', local)
                state, error = D.validate_image(local, kind)
                os.remove(local)
            IMAGE_CACHE[key] = state, error
            next_cache[relative] = dict(fingerprint=fingerprint, state=state, error=error)
            if state == 'done':
                have[kind].setdefault(gid, set()).add(month)
    local = V.L(V.D_CONTROL, 'validated_images.json')
    with open(local + '.part', 'w', encoding='utf-8') as fh:
        json.dump(next_cache, fh)
    os.replace(local + '.part', local)
    upload_atomic(local, '_control/validated_images.json', backup=False)
    return have


def read_national(kind):
    rel = f'CSV/{kind}_indices.csv'
    path = fetch_optional(f'{V.REMOTE_BASE}/{rel}', os.path.join(WORK, 'national', kind + '.csv'))
    return pd.read_csv(path) if path else pd.DataFrame(columns=D.COLUMNS[kind])


def inventory():
    global PROGRESS, SOURCES
    previous = fetch_optional(f'{V.REMOTE_BASE}/_control/progress.csv', os.path.join(WORK, 'previous.csv'))
    prior = pd.read_csv(previous) if previous else None
    source = fetch_optional(f'{V.REMOTE_BASE}/_control/source_counts.csv', os.path.join(WORK, 'sources.csv'))
    SOURCES = D.read_sources(source) if source else {}
    try:
        plan = load_plan()
    except FileNotFoundError:
        plan = {}
    SOURCES.update(D.plan_sources(plan))
    if SOURCES != (D.read_sources(source) if source else {}):
        save_sources()
    list_existing()
    frames = {kind: read_national(kind) for kind in ['day', 'night']}
    for kind, frame in frames.items():
        normalized = D.normalize(frame, ADMIN, kind)
        # Migration changes presentation only, never recomputes valid scientific values.
        if not frame.empty and not D.same_table(normalized, frame):
            path = V.L('CSV', kind + '_indices.csv')
            V._write_csv(normalized, path)
            upload_atomic(path, 'CSV/' + kind + '_indices.csv')
        frames[kind] = normalized
    PROGRESS = D.progress(ADMIN, frames, IMAGE_CACHE, SOURCES, prior)
    path = V.L(V.D_CONTROL, 'progress.csv')
    V._write_csv(PROGRESS, path)
    upload_atomic(path, '_control/progress.csv', backup=False)
    for field in D.FIELDS:
        counts = PROGRESS[field].value_counts().to_dict()
        log.info(f'KIỂM KÊ {field}: {counts}')
        summary(f'- {field}: {counts}')
    return PROGRESS


# ------------------------------------------------------------------------------------
# 2. CSV
# ------------------------------------------------------------------------------------
def _download_csvs(prefix):
    refresh_task_states()
    files = [f for f in rclone_json(EXP, "--include", f"{prefix}*.csv")]
    out = []
    for f in files:
        description = os.path.splitext(os.path.basename(f['Path']))[0]
        if not export_ready(description, {0: f}):
            continue
        out.append(fetch(f"{EXP}/{f['Path']}", os.path.join(WORK, "csv", f["Path"])))
    return out


def _merge_national(df_new, rel, cols=None):
    kind = 'day' if rel == V.DAY_CSV else 'night'
    df = D.merge_valid(read_national(kind), df_new, ADMIN, kind, SOURCES)
    path = V.L(rel)
    V._write_csv(df, path)
    upload_atomic(path, rel)
    log.info(f'{rel}: {len(df):,} dòng, {df["gid_3"].nunique():,} xã')
    return df


def day_csv():
    if PROGRESS is not None and PROGRESS['day_indices'].isin(D.TERMINAL).all():
        return True
    checkpoint('day_indices', list(CTX), None, 'running')
    paths = _download_csvs('day_csv_')
    if not paths:
        checkpoint('day_indices', list(CTX), None, 'pending', 'Đang chờ export CSV ngày')
        return False
    df = pd.concat([pd.read_csv(p, dtype={'GID_3': str}) for p in paths], ignore_index=True)
    df['YEAR'] = V.YEAR
    if 'SOURCE_COUNT' in df:
        for r in df.to_dict('records'):
            if pd.notna(r.get('SOURCE_COUNT')):
                SOURCES[r['GID_3'], int(r['MONTH']), 'day_indices'] = float(r['SOURCE_COUNT'])
    _merge_national(df, V.DAY_CSV)
    save_sources()
    return True


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def night_records(row):
    """Chép nguyên văn phần tính chỉ số của notebook cell 38 (như V.task3_all_months)."""
    g = row["GID_3"]
    adm = V.ADMIN_BY_GID[g]
    ctx = CTX[g]
    commune_area_ha = _num(row.get("area_ha")) or 0.0
    records = []
    for m in V.MONTHS:
        mm = f"{m:02d}"
        yr = V.YEAR
        n = _num(row.get(f"n_{mm}")) or 0
        if n == 0:
            continue
        get = lambda k: _num(row.get(f"{k}_{mm}"))
        lit_pixel_count = get("is_lit") or 0
        cloud_free_obs = get("cf_cvg") or 0
        total_pixels = get("avg_rad_count") or 0
        tnl = get("avg_rad_sum") or 0.0
        mean_rad = get("avg_rad_mean") or 0.0
        std_rad = get("avg_rad_stdDev") or 0.0
        min_rad = get("avg_rad_min") or 0.0
        max_rad = get("avg_rad_max") or 0.0
        lit_pop_proxy = get("lit_rad") or 0.0
        electrification_ratio = ((lit_pixel_count / total_pixels * 100.0) if total_pixels > 0 else 0.0)
        lit_area_ha = lit_pixel_count * 25.0
        spatial_cv = (std_rad / mean_rad) if mean_rad > 0 else 0.0
        records.append({
            "GID_1": adm["GID_1"], "NAME_1": adm["NAME_1"], "GID_3": g, "NAME_3": ctx["cname_full"],
            "YEAR": yr, "MONTH": m, "TIME": f"{yr}-{m:02d}",
            "COMMUNE_AREA_HA": round(commune_area_ha, 2), "TNL": round(tnl, 4), "MEAN_RAD": round(mean_rad, 4),
            "STD_RAD": round(std_rad, 4), "MIN_RAD": round(min_rad, 4), "MAX_RAD": round(max_rad, 4),
            "SPATIAL_CV": round(spatial_cv, 4), "LIT_PIXELS": int(lit_pixel_count),
            "LIT_AREA_HA": round(lit_area_ha, 2), "ELECTRIFICATION_RATIO_PCT": round(electrification_ratio, 2),
            "LIT_POP_PROXY": round(lit_pop_proxy, 4), "CLOUD_FREE_OBS": round(cloud_free_obs, 1),
        })
    if not records:
        return None
    df_ntl = pd.DataFrame(records)
    df_ntl["TNL_MA3"] = df_ntl["TNL"].rolling(window=3, min_periods=1).mean()
    df_ntl["TNL_MOM_GROWTH_PCT"] = df_ntl["TNL"].pct_change() * 100.0
    return df_ntl


def night_csv():
    D.require_day_complete(inventory())
    if PROGRESS['night_indices'].isin(D.TERMINAL).all():
        return True
    checkpoint('night_indices', list(CTX), None, 'running')
    paths = _download_csvs('night_csv_')
    if not paths:
        checkpoint('night_indices', list(CTX), None, 'pending', 'Đang chờ export CSV đêm')
        return False
    raw = pd.concat([pd.read_csv(p, dtype={'GID_3': str}) for p in paths], ignore_index=True)
    unknown = set(raw['GID_3']) - set(CTX)
    if unknown:
        raise ValueError(f'Export đêm có GID ngoài phạm vi: {sorted(unknown)[:10]}')
    frames = []
    for row in raw.to_dict('records'):
        for m in V.MONTHS:
            count = _num(row.get(f'n_{m:02d}'))
            if count is not None:
                SOURCES[row['GID_3'], m, 'night_indices'] = count
                SOURCES[row['GID_3'], m, 'night_image'] = count
            if count and _num(row.get(f'avg_rad_count_{m:02d}')) is None:
                raise RuntimeError(f'CSV đêm thiếu avg_rad_count: {row["GID_3"]}/{m}')
        d = night_records(row)
        if d is not None:
            frames.append(d)
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=D.COLUMNS['night'])
    _merge_national(df, V.NIGHT_CSV)
    save_sources()
    return True


# ------------------------------------------------------------------------------------
# 3. Ranh giới xã (GeoJSON export từ asset communes_l3)
# ------------------------------------------------------------------------------------
GEOMS = {}


def _polys(geom):
    t = geom.get("type")
    if t in ("Polygon", "MultiPolygon"):
        return [geom]
    if t == "GeometryCollection":
        return [p for g in geom.get("geometries", []) for p in _polys(g)]
    return []


def _bounds(polys):
    xs, ys = [], []

    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for x in c:
                walk(x)
    for p in polys:
        walk(p["coordinates"])
    return min(xs), min(ys), max(xs), max(ys)


def load_geoms():
    if GEOMS:
        return GEOMS
    files = rclone_json(EXP, "--include", f"{B.COMMUNES_GEOJSON}*.geojson")
    if not files:
        raise RuntimeError("Chưa có communes_l3.geojson trong thư mục export. Chạy cx.submit_communes() trong Colab.")
    for f in files:
        p = fetch(f"{EXP}/{f['Path']}", os.path.join(WORK, f["Path"]))
        with open(p, encoding="utf-8") as fh:
            fc = json.load(fh)
        for ft in fc["features"]:
            g = (ft.get("properties") or {}).get("GID_3")
            polys = _polys(ft.get("geometry") or {})
            if g and polys:
                GEOMS.setdefault(g, []).extend(polys)
    for g, polys in GEOMS.items():
        GEOMS[g] = (polys, _bounds(polys))
    log.info(f"Ranh giới: {len(GEOMS):,} xã")
    return GEOMS


# ------------------------------------------------------------------------------------
# 4. Cắt ảnh theo xã
# ------------------------------------------------------------------------------------
def _tile_re(prefix):
    return re.compile(rf"^{re.escape(prefix)}(?:-(\d+)-(\d+))?\.tif$")


def group_files(listing, prefix):
    rx = _tile_re(prefix)
    tiles = {}
    for f in listing:
        m = rx.match(f["Path"])
        if m:
            a, b = int(m.group(1) or 0), int(m.group(2) or 0)
            tiles[(b, a) if os.environ.get("VNGIS_TILE_ORDER") == "colrow" else (a, b)] = f
    return tiles


def _cut_worker(job):
    """Chạy trong tiến trình con. Ghép phần cần thiết từ các ô ảnh, che ngoài ranh giới xã, ghi GeoTIFF."""
    import rasterio
    from rasterio.windows import Window
    from rasterio.features import geometry_mask
    from rasterio.transform import Affine
    try:
        (r0, r1, c0, c1), X0, Y0, d = job["win"], job["X0"], job["Y0"], job["d"]
        H, W = r1 - r0, c1 - c0
        arr = None
        for t in job["tiles"]:
            with rasterio.open(t["path"]) as src:
                tr0, tc0 = t["ro"], t["co"]
                rr0, rr1 = max(r0, tr0), min(r1, tr0 + src.height)
                cc0, cc1 = max(c0, tc0), min(c1, tc0 + src.width)
                if arr is None:
                    fill = src.nodata if src.nodata is not None else 0
                    arr = np.full((src.count, H, W), fill, dtype=src.dtypes[0])
                    nodata, desc = src.nodata, src.descriptions
                if rr0 >= rr1 or cc0 >= cc1:
                    continue
                a = src.read(window=Window(cc0 - tc0, rr0 - tr0, cc1 - cc0, rr1 - rr0))
                arr[:, rr0 - r0:rr1 - r0, cc0 - c0:cc1 - c0] = a
        if arr is None:
            return job["gid"], False, "không có ô ảnh nào phủ xã", None
        transform = Affine(d, 0, X0 + c0 * d, 0, -d, Y0 - r0 * d)
        inside = geometry_mask(job["polys"], out_shape=(H, W), transform=transform, invert=True, all_touched=False)
        fill = nodata if nodata is not None else 0
        arr[:, ~inside] = fill
        if not any(desc or []):
            desc = job["bands"]
        tmp = job["out"] + ".part"
        os.makedirs(os.path.dirname(job["out"]), exist_ok=True)
        V.write_tif(tmp, arr, transform, "EPSG:4326", nodata, desc)
        os.replace(tmp, job["out"])
        return job["gid"], True, f"{H}x{W}", job["out"]
    except Exception as exc:
        return job["gid"], False, f"{type(exc).__name__}: {exc}", None


def compare_tifs(new_path, old_path, d):
    """So từng pixel ảnh cắt mới với ảnh cũ trên Drive (tải từng xã)."""
    import rasterio
    with rasterio.open(new_path) as a, rasterio.open(old_path) as b:
        A, Bb = a.read().astype("float64"), b.read().astype("float64")
        ta, tb = a.transform, b.transform
        fa = a.nodata if a.nodata is not None else 0
        fb = b.nodata if b.nodata is not None else 0
        out = {"new_shape": A.shape, "old_shape": Bb.shape, "old_dtype": b.dtypes[0], "new_dtype": a.dtypes[0],
               "old_nodata": b.nodata, "old_res": tb.a}
    dc, dr = (tb.c - ta.c) / d, (ta.f - tb.f) / d
    out["grid_offset_px"] = (round(dr, 4), round(dc, 4))
    if abs(dc - round(dc)) > 1e-3 or abs(dr - round(dr)) > 1e-3 or abs(tb.a - ta.a) > 1e-12:
        out["verdict"] = "KHÁC LƯỚI"
        return out
    dr, dc = int(round(dr)), int(round(dc))
    H = max(A.shape[1], dr + Bb.shape[1]) - min(0, dr)
    W = max(A.shape[2], dc + Bb.shape[2]) - min(0, dc)
    oa = (-min(0, dr), -min(0, dc))
    ob = (oa[0] + dr, oa[1] + dc)
    GA = np.full((A.shape[0], H, W), np.nan)
    GB = np.full((A.shape[0], H, W), np.nan)
    GA[:, oa[0]:oa[0] + A.shape[1], oa[1]:oa[1] + A.shape[2]] = np.where(A == fa, np.nan, A)
    GB[:, ob[0]:ob[0] + Bb.shape[1], ob[1]:ob[1] + Bb.shape[2]] = np.where(Bb == fb, np.nan, Bb)
    va, vb = ~np.isnan(GA[0]), ~np.isnan(GB[0])
    both = va & vb
    out["px_both"], out["px_only_new"], out["px_only_old"] = int(both.sum()), int((va & ~vb).sum()), int((vb & ~va).sum())
    out["max_abs_diff"] = float(np.nanmax(np.abs(GA[:, both] - GB[:, both]))) if both.any() else None
    md = out["max_abs_diff"]
    out["verdict"] = "KHỚP" if md is not None and md <= 1e-6 else ("LỆCH GIÁ TRỊ" if md is not None else "KHÔNG CHỒNG")
    return out


def export_ready(desc, tiles):
    """Tác vụ export đã xong chưa: hỏi Earth Engine nếu được, nếu không thì xem file đã đứng yên đủ lâu chưa."""
    st = TASK_STATES.get(desc)
    if st is not None:
        return st == "COMPLETED"
    if not tiles:
        return False
    newest = max(datetime.fromisoformat(t["ModTime"].replace("Z", "+00:00")).timestamp() for t in tiles.values())
    return time.time() - newest > STABLE_SEC


TASK_STATES = {}


def refresh_task_states():
    """Đọc trạng thái tác vụ export bằng service account (cùng project). Không được thì bỏ qua."""
    if not V.EE_KEY_FILE:
        return
    try:
        if V.EE_CREDENTIALS is None:
            V.PROJECT_ID = B.PROJECT_ID
            V.ASSET_ID = B.ASSET_ID
            V.init_earth_engine()
        TASK_STATES.clear()
        for t in V.ee.data.getTaskList():
            d = t.get("description")
            if d and d not in TASK_STATES:
                TASK_STATES[d] = t.get("state")
        log.info(f"Trạng thái tác vụ Earth Engine: {len(TASK_STATES)} tác vụ")
    except Exception as exc:
        log.info(f"Không đọc được trạng thái tác vụ ({str(exc)[:150]}): dùng cách chờ file đứng yên {STABLE_SEC//60} phút.")


def process_group(prefix, kind, month, gids, listing, have, done_set):
    """Cắt 1 ảnh export (có thể nhiều ô) thành ảnh từng xã. Trả 'done' | 'waiting' | 'stopped'."""
    import rasterio
    if all(month in have[kind].get(g, set()) or SOURCES.get((g, month, kind + '_image')) == 0 for g in gids):
        return 'done'
    done_set.discard(prefix)
    if TASK_STATES.get(prefix) in ("FAILED", "CANCELLED", "CANCEL_REQUESTED"):
        log.error(f"[{prefix}] tác vụ export {TASK_STATES[prefix]}: gửi lại trong Colab (force=True).")
        checkpoint(kind + '_image', gids, month, 'failed', f'Export {prefix}: {TASK_STATES[prefix]}')
        return "failed"
    tiles = group_files(listing, prefix)
    if not tiles or not export_ready(prefix, tiles):
        return "waiting"
    geoms = load_geoms()
    d = B.D20 if kind == "day" else B.D500
    bands = V.DAY_BANDS_ALL if kind == "day" else ["avg_rad", "cf_cvg"]
    namer = V.day_name if kind == "day" else V.night_name
    dir_key = "rel_day_dir" if kind == "day" else "rel_night_dir"
    already = {g for g in gids if month in have[kind].get(g, set())
               or SOURCES.get((g, month, kind + '_image')) == 0}
    todo = [g for g in gids if g not in already and g in geoms]
    no_geom = [g for g in gids if g not in already and g not in geoms]
    if no_geom:
        checkpoint(kind + '_image', no_geom, month, 'failed', 'Không có ranh giới trong asset')
        return 'failed'
    cmp_gids = [g for g in gids if month in have[kind].get(g, set()) and g in geoms][:COMPARE_N]
    log.info(f"[{prefix}] {len(tiles)} ô | {len(gids):,} xã: {len(already):,} đã có, cần cắt {len(todo):,}"
             + (f", {len(no_geom)} xã không có ranh giới" if no_geom else "")
             + (f", so sánh {len(cmp_gids)} xã" if cmp_gids else ""))
    if not todo and not cmp_gids:
        return _finish_group(prefix, tiles, done_set)

    tdir = os.path.join(WORK, "tiles", prefix)
    os.makedirs(tdir, exist_ok=True)
    keys = sorted(tiles)
    first = keys[0]
    p0 = os.path.join(tdir, tiles[first]["Path"])
    if not os.path.isfile(p0):
        _wait_space(tiles[first]["Size"])
        fetch(f"{EXP}/{tiles[first]['Path']}", p0)
    with rasterio.open(p0) as src:
        tr = src.transform
        untiled = len(keys) == 1 and _tile_re(prefix).match(tiles[first]['Path']).group(1) is None
        single_shape = src.height, src.width
        if abs(tr.a - d) > 1e-9 * d * 1e3:
            raise RuntimeError(f'[{prefix}] độ phân giải {tr.a} khác dự kiến {d}')
    X0, Y0 = tr.c - first[1] * d, tr.f + first[0] * d
    FD = B.FILE_DIMENSIONS

    def win_of(g):
        minx, miny, maxx, maxy = geoms[g][1]
        e = 1e-9
        return (math.floor((Y0 - maxy) / d + e), math.ceil((Y0 - miny) / d - e),
                math.floor((minx - X0) / d + e), math.ceil((maxx - X0) / d - e))

    def tiles_of(w):
        r0, r1, c0, c1 = w
        if untiled:
            if r0 < 0 or c0 < 0 or r1 > single_shape[0] or c1 > single_shape[1]:
                return {(0, 0), (-1, -1)}  # incomplete coverage must fail, not produce fabricated zeros
            return {(0, 0)}
        # Require every tile crossing the commune bounding box, including absent files.
        return {(r, c) for r in range((r0 // FD) * FD, r1, FD)
                for c in range((c0 // FD) * FD, c1, FD)}

    pend = {}
    for g in todo + cmp_gids:
        w = win_of(g)
        if w[1] <= w[0] or w[3] <= w[2]:
            w = (w[0], w[0] + 1, w[2], w[2] + 1)
        pend[g] = (w, tiles_of(w))
    checkpoint(kind + '_image', todo, month, 'running')
    loaded = {}
    fails, n_ok, cmp_rows = [], 0, []
    with ProcessPoolExecutor(max_workers=max(1, int(os.environ.get("VNGIS_CUT_WORKERS", "2")))) as pool:
        for k in keys:
            if V.STOP_EVENT.is_set():
                return "stopped"
            if not any(k in need for _w, need in pend.values()):
                continue
            path = os.path.join(tdir, tiles[k]["Path"])
            if not os.path.isfile(path):
                _wait_space(tiles[k]["Size"])
                fetch(f"{EXP}/{tiles[k]['Path']}", path)
            _check_tile(path, k, X0, Y0, d)
            loaded[k] = path
            ready = [g for g, (_w, need) in pend.items() if need <= set(loaded)]
            jobs = []
            for g in ready:
                w, need = pend.pop(g)
                ctx = CTX[g]
                is_cmp = g in already
                out = (os.path.join(WORK, "cmp", namer(ctx, month)) if is_cmp
                       else V.L(ctx[dir_key], namer(ctx, month)))
                jobs.append({"gid": g, "win": w, "X0": X0, "Y0": Y0, "d": d, "polys": geoms[g][0], "bands": bands,
                             "out": out, "cmp": is_cmp,
                             "tiles": [{"path": loaded[t], "ro": t[0], "co": t[1]} for t in sorted(need)]})
            for job, res in zip(jobs, pool.map(_cut_worker, jobs, chunksize=4)):
                g, ok, msg, out = res
                if not ok:
                    fails.append((g, msg))
                elif job["cmp"]:
                    cmp_rows.append(_compare_one(g, month, kind, out, d))
                else:
                    state, error = D.validate_image(out, kind)
                    if state != 'done':
                        fails.append((g, error))
                        os.remove(out)
                    else:
                        rel = f"{CTX[g][dir_key]}/{namer(CTX[g], month)}"
                        upload_atomic(out, rel)
                        os.remove(out)
                        have[kind].setdefault(g, set()).add(month)
                        n_ok += 1
            if ready:
                V.UPLOAD_NOW.set()
                log.info(f"[{prefix}] ô {k}: cắt xong {n_ok:,}/{len(todo):,} xã | lỗi {len(fails)}")
            for kk in [kk for kk in loaded if not any(kk in need for _w, need in pend.values())]:
                os.remove(loaded.pop(kk))          # ô không còn xã nào cần: xóa cho trống ổ
    if cmp_rows:
        cdf = pd.DataFrame(cmp_rows)
        V._write_csv(cdf, V.L(V.D_CONTROL, f"compare_{prefix}.csv"))
        log.info(f"[{prefix}] SO SÁNH với ảnh cũ: {cdf['verdict'].value_counts().to_dict()}")
        summary(f"- So sánh {prefix}: {cdf['verdict'].value_counts().to_dict()} (xem _control/compare_{prefix}.csv)")
    for g in pend:
        fails.append((g, 'Không đủ ô export phủ ranh giới xã'))
    if fails:
        for g, error in fails:
            checkpoint(kind + '_image', [g], month, 'failed', error)
        log.warning(f"[{prefix}] {len(fails)} xã lỗi: {fails[:5]}")
        V._write_csv(pd.DataFrame(fails, columns=["GID_3", "error"]), V.L(V.D_CONTROL, f"errors_{prefix}.csv"))
    # Outputs are uploaded individually; shared Drive exports are retained.
    left = _local_tifs(kind)
    if left:
        log.warning(f"[{prefix}] còn {left} ảnh chưa đẩy lên Drive: giữ file export, lượt sau làm tiếp.")
        return "stopped"
    if fails:
        return "partial"
    return _finish_group(prefix, tiles, done_set)


def _check_tile(path, k, X0, Y0, d):
    """Vị trí thật của ô (đọc từ file) phải khớp với vị trí suy ra từ tên file; nếu không thì dừng để tránh ghép sai."""
    import rasterio
    with rasterio.open(path) as src:
        tr = src.transform
        kind = 'day' if abs(d - B.D20) < 1e-12 else 'night'
        if src.crs is None or src.crs.to_epsg() != 4326 or src.count != (10 if kind == 'day' else 2):
            raise RuntimeError('Ô export sai CRS hoặc số kênh')
        if abs(tr.a - d) > d * 1e-6 or abs(tr.e + d) > d * 1e-6 or tr.b or tr.d:
            raise RuntimeError('Ô export sai độ phân giải/lưới')
    ro, co = (Y0 - tr.f) / d, (tr.c - X0) / d
    if abs(ro - k[0]) > 1e-3 or abs(co - k[1]) > 1e-3:
        raise RuntimeError(f"Ô {os.path.basename(path)}: vị trí thật ({ro:.2f}, {co:.2f}) khác tên file {k}. "
                           f"Đặt VNGIS_TILE_ORDER=colrow rồi chạy lại.")


def _compare_one(g, month, kind, new_path, d):
    ctx = CTX[g]
    namer = V.day_name if kind == "day" else V.night_name
    rel = f"{ctx['rel_day_dir' if kind == 'day' else 'rel_night_dir']}/{namer(ctx, month)}"
    old = fetch_optional(f"{V.REMOTE_BASE}/{rel}", os.path.join(WORK, "cmp_old", os.path.basename(rel)))
    row = {"GID_3": g, "MONTH": month}
    if not old:
        row["verdict"] = "không tải được ảnh cũ"
        return row
    try:
        row.update(compare_tifs(new_path, old, d))
    except Exception as exc:
        row["verdict"] = f"lỗi so sánh: {exc}"
    for p in (new_path, old):
        try:
            os.remove(p)
        except OSError:
            pass
    return row


def _local_tifs(kind):
    root = V.L(V.D_DAY if kind == "day" else V.D_NIGHT)
    n = 0
    for _dp, _dn, fn in os.walk(root):
        n += sum(1 for f in fn if f.endswith(".tif"))
    return n


def _wait_space(size_bytes):
    need = size_bytes / 1e9 + V.MIN_FREE_GB
    if free_gb() < need:
        raise RuntimeError(f'Không đủ ổ trống để xử lý file {size_bytes} bytes; giảm phạm vi export hoặc tăng dung lượng')
    warned = False
    while free_gb() < need:
        V.UPLOAD_NOW.set()
        if not warned:
            log.warning(f"Ổ máy chạy còn {free_gb():.1f} GB, cần {need:.1f} GB: chờ đẩy ảnh lên Drive.")
            warned = True
        if V.STOP_EVENT.is_set():
            raise V.StopRequested()
        time.sleep(10)


def _finish_group(prefix, tiles, done_set):
    """Keep shared exports for repair/resume; batch_done is informational only."""
    done_set.add(prefix)
    path = V.L(V.D_CONTROL, 'batch_done.txt')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sorted(done_set)) + '\n')
    upload_atomic(path, '_control/batch_done.txt', backup=False)
    import shutil
    shutil.rmtree(os.path.join(WORK, 'tiles', prefix), ignore_errors=True)
    log.info(f'[{prefix}] XONG, giữ nguyên file export trên Drive.')
    return 'done'


def load_done():
    p = fetch_optional(f"{V.REMOTE_BASE}/{V.D_CONTROL}/batch_done.txt", V.L(V.D_CONTROL, "batch_done.txt"))
    if not p:
        return set()
    with open(p, encoding="utf-8") as f:
        return {x.strip() for x in f if x.strip()}


def run_images(kind):
    """Xử lý mọi nhóm ảnh của phần ngày hoặc đêm; chờ Earth Engine nếu còn tác vụ chưa xong."""
    have = list_existing()
    done_set = load_done()
    if kind == "day":
        plan = load_plan()
        missing = set(CTX) - set(plan)
        unknown = set(plan) - set(CTX)
        if missing or unknown:
            raise RuntimeError(f'Kế hoạch ảnh ngày không khớp địa giới: thiếu {len(missing)}, dư {len(unknown)} xã')
        groups = []
        for m in V.MONTHS:
            by_w = {}
            for g, rows in plan.items():
                w = B.window_class(rows[m - 1])
                if w is not None and g in CTX:
                    by_w.setdefault(w, []).append(g)
            groups += [(B.day_img_prefix(m, w), m, gids) for w, gids in sorted(by_w.items())]
    else:
        all_g = list(ADMIN["GID_3"])
        groups = [(B.night_img_prefix(m), m, all_g) for m in V.MONTHS]
    if kind == 'night':
        D.require_day_complete(inventory())
    try:
        while True:
            refresh_task_states()
            listing = rclone_json(EXP, "--include", f"{kind}_img_*.tif")
            states = {}
            for prefix, m, gids in groups:
                if V.STOP_EVENT.is_set():
                    break
                states[prefix] = process_group(prefix, kind, m, gids, listing, have, done_set)
            vals = list(states.values())
            log.info(f"Ảnh {kind}: {sum(v == 'done' for v in vals)}/{len(groups)} nhóm xong | "
                     f"chờ Earth Engine {sum(v == 'waiting' for v in vals)} | lỗi một phần {sum(v == 'partial' for v in vals)}")
            if V.STOP_EVENT.is_set():
                return 3
            if all(v == "done" for v in vals):
                return 0
            if "waiting" not in vals and "stopped" not in vals:
                return 1 if ("partial" in vals or "failed" in vals) else 0
            if "partial" in vals or "failed" in vals:
                have = list_existing()      # thử lại các xã lỗi ở vòng sau
            log.info(f"Chờ {WAIT_POLL_SEC // 60} phút rồi kiểm tra lại các tác vụ export...")
            if V.STOP_EVENT.wait(WAIT_POLL_SEC):
                return 3
    finally:
        inventory()


# ------------------------------------------------------------------------------------
def main():
    global COMPARE_N
    ap = argparse.ArgumentParser()
    ap.add_argument('step', choices=['inventory', 'day-csv', 'day-img', 'night-csv', 'night-img', 'day', 'night'])
    ap.add_argument('--compare', type=int, default=0)
    a = ap.parse_args()
    COMPARE_N = a.compare
    V.install_signal_handlers()
    if V.MAX_RUNTIME_SEC > 0:
        t = threading.Timer(V.MAX_RUNTIME_SEC, V.request_stop, args=('deadline',))
        t.daemon = True
        t.start()
    code = 0
    phase_field = None
    try:
        setup()
        inventory()  # actual outputs before doing any work
        if a.step.startswith('night'):
            D.require_day_complete(PROGRESS)
        if a.step in ('day-csv', 'day'):
            phase_field = 'day_indices'
            code = 0 if day_csv() else 3
        if a.step in ('night-csv', 'night'):
            phase_field = 'night_indices'
            code = 0 if night_csv() else 3
        if a.step in ('day-img', 'day'):
            phase_field = 'day_image'
            images_code = run_images('day')
            code = 1 if 1 in (code, images_code) else max(code, images_code)
        if a.step in ('night-img', 'night'):
            phase_field = 'night_image'
            images_code = run_images('night')
            code = 1 if 1 in (code, images_code) else max(code, images_code)
        table = inventory()
        if a.step != 'inventory':
            kind = 'day' if a.step.startswith('day') else 'night'
            fields = [kind + '_image', kind + '_indices']
            if a.step.endswith('-csv'):
                fields = [kind + '_indices']
            elif a.step.endswith('-img'):
                fields = [kind + '_image']
            if not table[fields].isin(D.TERMINAL).all().all():
                code = 1 if code == 1 or table[fields].eq('failed').any().any() else 3
    except V.StopRequested:
        code = 3
    except Exception as exc:
        log.error(f'LỖI: {type(exc).__name__}: {exc}')
        code = 1
        if phase_field and PROGRESS is not None:
            try:
                checkpoint(phase_field, list(CTX), None, 'failed', f'{type(exc).__name__}: {exc}')
            except Exception as write_error:
                log.error(f'Không lưu được lỗi vào tiến độ: {write_error}')
    log.info(f'Kết thúc, mã {code}')
    try:
        flush_logs()
    except Exception as exc:
        log.error(f'Không lưu được log lên Drive: {exc}')
        code = 1 if code == 0 else code
    return code


if __name__ == '__main__':
    sys.exit(main())
