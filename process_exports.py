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

import vngis_2024 as V
import batch_config as B

log = V.log
EXP = f"{V.RCLONE_REMOTE}:{B.EXPORT_FOLDER}"
WORK = os.path.expanduser("~/vngis_batch")
WAIT_POLL_SEC = 300
STABLE_SEC = 900              # nếu không đọc được trạng thái tác vụ: file export phải "đứng yên" 15 phút mới xử lý
COMPARE_N = 0

ADMIN = None
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
    res = subprocess.run(["rclone", "copyto", remote_file, local_path], capture_output=True, text=True, timeout=3600)
    return local_path if res.returncode == 0 and os.path.isfile(local_path) else None


def free_gb(path=WORK):
    import shutil
    os.makedirs(path, exist_ok=True)
    return shutil.disk_usage(path).free / 1e9


def summary(md):
    p = os.environ.get("GITHUB_STEP_SUMMARY")
    if p:
        with open(p, "a", encoding="utf-8") as f:
            f.write(md + "\n")


def setup():
    global ADMIN
    for d in (V.LOCAL_ROOT, V.L(V.D_CONTROL), V.L(V.D_LOGS), V.CACHE_DIR, WORK):
        os.makedirs(d, exist_ok=True)
    V.setup_logging()
    ADMIN = V.build_admin_table()
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
        raise RuntimeError(f"Chưa có {B.PLAN_DAY} trong {EXP}. Chạy cx.compute_day_plan() trong Colab trước.")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


# ------------------------------------------------------------------------------------
# 1. Kiểm kê
# ------------------------------------------------------------------------------------
_TIF_RE = re.compile(r"^(.+)_(day|night)_2024(\d\d)\.tif$")


def list_existing():
    """Tập (GID_3, tháng) đã có ảnh trên Drive, cho Day và Night."""
    have = {"day": {}, "night": {}}
    for kind, d in (("day", V.D_DAY), ("night", V.D_NIGHT)):
        res = subprocess.run(["rclone", "lsf", "-R", "--files-only", "--fast-list", "--include", "*.tif",
                              f"{V.REMOTE_BASE}/{d}"], capture_output=True, text=True, timeout=3600)
        if res.returncode != 0 and "directory not found" not in res.stderr:
            raise RuntimeError(f"Không liệt kê được {d}: {res.stderr.strip()[-300:]}")
        for line in res.stdout.splitlines():
            m = _TIF_RE.match(os.path.basename(line.strip()))
            if m and m.group(2) == kind and m.group(1) in SAFE2GID:
                have[kind].setdefault(SAFE2GID[m.group(1)], set()).add(int(m.group(3)))
    log.info(f"Trên Drive: ảnh ngày {sum(map(len, have['day'].values())):,} file ({len(have['day']):,} xã), "
             f"ảnh đêm {sum(map(len, have['night'].values())):,} file ({len(have['night']):,} xã)")
    return have


def csv_months(rel):
    p = fetch_optional(f"{V.REMOTE_BASE}/{rel}", os.path.join(WORK, "inv", os.path.basename(rel)))
    if not p:
        return {}
    df = pd.read_csv(p, usecols=["GID_3", "MONTH"], dtype={"GID_3": str})
    return df.groupby("GID_3")["MONTH"].nunique().to_dict()


def inventory():
    have = list_existing()
    day_csv, night_csv = csv_months(V.DAY_CSV), csv_months(V.NIGHT_CSV)
    try:
        plan = load_plan()
    except Exception:
        plan = {}
    rows = []
    for r in ADMIN.itertuples():
        g = r.GID_3
        exp_day = ([m for m in V.MONTHS if B.window_class(plan[g][m - 1]) is not None] if g in plan else V.MONTHS)
        hd, hn = have["day"].get(g, set()), have["night"].get(g, set())
        miss_d = [m for m in exp_day if m not in hd]
        miss_n = [m for m in V.MONTHS if m not in hn]
        dc, nc = int(day_csv.get(g, 0)), int(night_csv.get(g, 0))
        rows.append({"GID_1": r.GID_1, "NAME_1": r.NAME_1, "GID_3": g, "NAME_3": r.NAME_3,
                     "day_tif": len(hd), "day_tif_expected": len(exp_day),
                     "day_tif_missing": ",".join(f"{m:02d}" for m in miss_d), "day_csv_months": dc,
                     "night_tif": len(hn), "night_tif_missing": ",".join(f"{m:02d}" for m in miss_n),
                     "night_csv_months": nc,
                     "day_status": "done" if not miss_d and dc > 0 else ("partial" if hd or dc else "missing"),
                     "night_status": "done" if not miss_n and nc > 0 else ("partial" if hn or nc else "missing")})
    df = pd.DataFrame(rows)
    path = V.L(V.D_CONTROL, "progress.csv")
    V._write_csv(df, path)
    V._rclone(["copyto", path, f"{V.REMOTE_BASE}/{V.D_CONTROL}/progress.csv"])
    dv, nv = df["day_status"].value_counts().to_dict(), df["night_status"].value_counts().to_dict()
    log.info(f"KIỂM KÊ: ngày {dv} | đêm {nv} | đã ghi _control/progress.csv")
    summary(f"### Kiểm kê {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC\n\n"
            f"| | done | partial | missing |\n|---|---|---|---|\n"
            f"| Ngày | {dv.get('done', 0):,} | {dv.get('partial', 0):,} | {dv.get('missing', 0):,} |\n"
            f"| Đêm | {nv.get('done', 0):,} | {nv.get('partial', 0):,} | {nv.get('missing', 0):,} |\n\n"
            f"Ảnh ngày: {df['day_tif'].sum():,}/{df['day_tif_expected'].sum():,} file. "
            f"Ảnh đêm: {df['night_tif'].sum():,}/{12 * len(df):,} file. "
            f"CSV ngày: {(df['day_csv_months'] > 0).sum():,} xã. CSV đêm: {(df['night_csv_months'] > 0).sum():,} xã.\n")
    return df


# ------------------------------------------------------------------------------------
# 2. CSV
# ------------------------------------------------------------------------------------
def _download_csvs(prefix):
    files = [f for f in rclone_json(EXP, "--include", f"{prefix}*.csv")]
    out = []
    for f in files:
        out.append(fetch(f"{EXP}/{f['Path']}", os.path.join(WORK, "csv", f["Path"])))
    return out


def _merge_national(df_new, rel, cols):
    old = fetch_optional(f"{V.REMOTE_BASE}/{rel}", os.path.join(WORK, "old_" + os.path.basename(rel)))
    if old:
        df_old = pd.read_csv(old, dtype={"GID_1": str, "GID_2": str, "GID_3": str})
        df_new = pd.concat([df_old, df_new], ignore_index=True)
    df = df_new.reindex(columns=cols).drop_duplicates(subset=["GID_3", "MONTH"], keep="last")
    for c in ("YEAR", "MONTH", "LIT_PIXELS"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["_k"] = df["GID_3"].map(lambda g: tuple(V.natural_sort_key(g)))
    df = df.sort_values(["_k", "MONTH"]).drop(columns="_k")
    path = V.L(rel)
    V._write_csv(df, path)
    if not V._rclone(["copyto", path, f"{V.REMOTE_BASE}/{rel}"]):
        raise RuntimeError(f"Không ghi được {rel} lên Drive")
    log.info(f"{rel}: {len(df):,} dòng, {df['GID_3'].nunique():,} xã (đã ghi lên Drive)")
    return df


def day_csv():
    paths = _download_csvs("day_csv_")
    if not paths:
        log.warning("Chưa có file day_csv_*.csv trong thư mục export.")
        return False
    df = pd.concat([pd.read_csv(p, dtype={"GID_3": str}) for p in paths], ignore_index=True)
    adm = ADMIN[V.ADM_COLS]
    unknown = set(df["GID_3"]) - set(adm["GID_3"])
    if unknown:
        log.warning(f"{len(unknown)} GID_3 trong CSV không có trong GADM, bỏ: {sorted(unknown)[:5]}")
    df = df.merge(adm, on="GID_3", how="inner")          # = merge(lookup, df_s2, on="GID_3") của notebook
    df["YEAR"] = V.YEAR
    _merge_national(df, V.DAY_CSV, V.DAY_COLUMNS)
    summary(f"- CSV ngày: gộp {len(paths)} file tỉnh, {df['GID_3'].nunique():,} xã")
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
    paths = _download_csvs("night_csv_")
    if not paths:
        log.warning("Chưa có file night_csv_*.csv trong thư mục export.")
        return False
    raw = pd.concat([pd.read_csv(p, dtype={"GID_3": str}) for p in paths], ignore_index=True)
    n_cols = [c for c in raw.columns if c.startswith("n_")]
    has_img = (raw[n_cols].fillna(0) > 0).any(axis=1)
    cnt_cols = [c for c in raw.columns if c.startswith("avg_rad_count_")]
    if has_img.any() and (not cnt_cols or raw.loc[has_img, cnt_cols].isna().all().all()):
        raise RuntimeError("CSV đêm không có cột avg_rad_count_MM: tên khóa reduceRegion khác dự kiến, kiểm tra lại.")
    frames = []
    for row in raw.to_dict("records"):
        if row["GID_3"] not in V.ADMIN_BY_GID:
            continue
        d = night_records(row)
        if d is not None:
            frames.append(d)
    df = pd.concat(frames, ignore_index=True)
    _merge_national(df, V.NIGHT_CSV, V.NIGHT_COLUMNS)
    summary(f"- CSV đêm: gộp {len(paths)} file tỉnh, {df['GID_3'].nunique():,} xã")
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
    if prefix in done_set:
        return "done"
    if TASK_STATES.get(prefix) in ("FAILED", "CANCELLED", "CANCEL_REQUESTED"):
        log.error(f"[{prefix}] tác vụ export {TASK_STATES[prefix]}: gửi lại trong Colab (force=True).")
        return "failed"
    tiles = group_files(listing, prefix)
    if not export_ready(prefix, tiles):
        return "waiting"
    geoms = load_geoms()
    d = B.D20 if kind == "day" else B.D500
    bands = V.DAY_BANDS_ALL if kind == "day" else ["avg_rad", "cf_cvg"]
    namer = V.day_name if kind == "day" else V.night_name
    dir_key = "rel_day_dir" if kind == "day" else "rel_night_dir"
    already = {g for g in gids if month in have[kind].get(g, set())}
    todo = [g for g in gids if g not in already and g in geoms]
    no_geom = [g for g in gids if g not in geoms]
    cmp_gids = [g for g in gids if g in already and g in geoms][:COMPARE_N]
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
        if abs(tr.a - d) > 1e-9 * d * 1e3:
            log.warning(f"[{prefix}] độ phân giải {tr.a} khác dự kiến {d}: dùng giá trị trong file")
            d = tr.a
    X0, Y0 = tr.c - first[1] * d, tr.f + first[0] * d
    FD = B.FILE_DIMENSIONS

    def win_of(g):
        minx, miny, maxx, maxy = geoms[g][1]
        e = 1e-9
        return (math.floor((Y0 - maxy) / d + e), math.ceil((Y0 - miny) / d - e),
                math.floor((minx - X0) / d + e), math.ceil((maxx - X0) / d - e))

    def tiles_of(w):
        r0, r1, c0, c1 = w
        return {k for k in keys if k[0] < r1 and k[0] + FD > r0 and k[1] < c1 and k[1] + FD > c0}

    pend = {}
    for g in todo + cmp_gids:
        w = win_of(g)
        if w[1] <= w[0] or w[3] <= w[2]:
            w = (w[0], w[0] + 1, w[2], w[2] + 1)
        pend[g] = (w, tiles_of(w))
    loaded = {}
    fails, n_ok, cmp_rows = [], 0, []
    with ProcessPoolExecutor(max_workers=max(2, os.cpu_count() or 2)) as pool:
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
    if fails:
        log.warning(f"[{prefix}] {len(fails)} xã lỗi: {fails[:5]}")
        V._write_csv(pd.DataFrame(fails, columns=["GID_3", "error"]), V.L(V.D_CONTROL, f"errors_{prefix}.csv"))
    # Đợi đẩy hết ảnh của nhóm lên Drive rồi mới xóa file export
    V.rclone_sync_once(final=True)
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
    """Đánh dấu xong và xóa file export tạm trên Drive (giải phóng dung lượng)."""
    done_set.add(prefix)
    path = V.L(V.D_CONTROL, "batch_done.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(done_set)) + "\n")
    V._rclone(["copyto", path, f"{V.REMOTE_BASE}/{V.D_CONTROL}/batch_done.txt"])
    if os.environ.get("VNGIS_KEEP_EXPORTS", "").lower() not in ("1", "true"):
        lst = os.path.join(WORK, f"del_{prefix}.txt")
        with open(lst, "w") as f:
            f.write("\n".join(t["Path"] for t in tiles.values()) + "\n")
        V._rclone(["delete", EXP, "--files-from", lst, "--drive-use-trash=false"])
    import shutil
    shutil.rmtree(os.path.join(WORK, "tiles", prefix), ignore_errors=True)
    log.info(f"[{prefix}] XONG, đã xóa file export tạm.")
    return "done"


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
        groups = []
        for m in V.MONTHS:
            by_w = {}
            for g, rows in plan.items():
                w = B.window_class(rows[m - 1])
                if w is not None and g in V.ADMIN_BY_GID:
                    by_w.setdefault(w, []).append(g)
            groups += [(B.day_img_prefix(m, w), m, gids) for w, gids in sorted(by_w.items())]
    else:
        all_g = list(ADMIN["GID_3"])
        groups = [(B.night_img_prefix(m), m, all_g) for m in V.MONTHS]
    uploader = V.Uploader()
    uploader.start()
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
        uploader.stop_event.set()
        uploader.join(timeout=120)
        V.rclone_sync_once(final=True)


# ------------------------------------------------------------------------------------
def main():
    global COMPARE_N
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["inventory", "day-csv", "day-img", "night-csv", "night-img", "day", "night"])
    ap.add_argument("--compare", type=int, default=0)
    a = ap.parse_args()
    COMPARE_N = a.compare
    setup()
    V.install_signal_handlers()
    if V.MAX_RUNTIME_SEC > 0:
        t = threading.Timer(V.MAX_RUNTIME_SEC, V.request_stop, args=("deadline",))
        t.daemon = True
        t.start()
    code = 0
    try:
        if a.step in ("day-csv", "day"):
            day_csv()
        if a.step in ("night-csv", "night"):
            night_csv()
        if a.step in ("day-img", "day"):
            code = run_images("day")
        if a.step in ("night-img", "night"):
            code = run_images("night")
        if a.step in ("inventory", "day", "night") or code == 0:
            inventory()
    except V.StopRequested:
        code = 3
    except Exception as exc:
        log.error(f"LỖI: {type(exc).__name__}: {exc}")
        code = 1
    log.info(f"Kết thúc, mã {code}")
    return code


if __name__ == "__main__":
    sys.exit(main())
