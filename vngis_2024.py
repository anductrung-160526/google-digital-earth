# -*- coding: utf-8 -*-
"""
VNGISDash 2024: pipeline tự động cấp xã chạy trên GitHub Actions.

Nguồn khoa học: VNGISDash_Task123_Merged_final.ipynb. Pipeline chỉ giữ 3 chức năng:
  (1) trích xuất chỉ số từ ảnh ngày (Task 1) và ảnh đêm (Task 3.2), (2) lấy ảnh tif ngày (Task 2),
  (3) lấy ảnh tif đêm (Task 3.1). Không có bước chọn tỉnh, xã: VNGIS_MODE=pilot tự lấy 2 xã,
  VNGIS_MODE=full chạy mọi xã.
Mọi hàm Earth Engine và công thức được chép nguyên văn từ notebook (xem DOI_CHIEU_NOTEBOOK.md).
Phần viết mới chỉ là "vỏ": duyệt danh sách xã, tải ảnh bằng getDownloadURL, ghi file, đồng bộ Drive,
ghi trạng thái, chạy nối lượt.

Chạy:
    python vngis_2024.py               chạy pipeline
    python vngis_2024.py --sync-only   đẩy nốt dữ liệu trên máy lên Drive
    python vngis_2024.py --merge-only  gộp CSV toàn quốc từ dữ liệu trên Drive

Mã thoát: 0 xong toàn bộ | 1 lỗi cấu hình hoặc preflight | 2 sự cố EE kéo dài | 3 hết giờ (nối lượt) | 130 dừng tay
"""

import os, io, re, sys, json, time, glob, math, shutil, zipfile, signal, logging, calendar
import threading, subprocess, unicodedata, warnings
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")


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
TIME_START_STR, TIME_END_STR = f"{YEAR}01", f"{YEAR}12"          # "202401", "202412" như notebook

PROJECT_ID = "digital-vietnam-earth"                               # notebook cell 11
ASSET_ID = f"projects/{PROJECT_ID}/assets/communes_l3"             # notebook cell 11

N_WORKERS = _env("VNGIS_WORKERS", 8, int)
RUN_ID = _env("VNGIS_RUN_ID", "local")
MAX_RUNTIME_SEC = _env("VNGIS_MAX_RUNTIME_SEC", 0, int)
EE_KEY_FILE = _env("VNGIS_EE_KEY_FILE", "")
EE_HIGH_VOLUME = _env("VNGIS_EE_HIGH_VOLUME", False, bool)        # mặc định endpoint standard
MAX_ATTEMPTS = _env("VNGIS_MAX_ATTEMPTS", 3, int)
MODE = _env("VNGIS_MODE", "pilot").lower()                        # pilot: tự lấy 2 xã | full: toàn bộ xã
if MODE not in ("pilot", "full"):
    raise SystemExit(f"VNGIS_MODE phải là 'pilot' hoặc 'full', đang là '{MODE}'")
PILOT_N = _env("VNGIS_PILOT_N", 2, int)                            # số xã thí điểm, tùy chọn
if PILOT_N < 1:
    raise SystemExit("VNGIS_PILOT_N phải >= 1")
PREFLIGHT = _env("VNGIS_PREFLIGHT", True, bool)
COMPRESS_TIF = _env("VNGIS_COMPRESS_TIF", True, bool)             # nén DEFLATE không mất dữ liệu
EE_DEADLINE_SEC = 300
DOWNLOAD_FAIL_LIMIT = 24          # 24 lượt tải liên tiếp thất bại, chưa thành công lần nào: dừng job
MAX_TILE_SPLIT = 8                # chia tối đa 8x8 ô khi ảnh vượt hạn mức tải
TILING_OK = [True]                # preflight kiểm tra chia ô; nếu không đạt, xã cần chia ô sẽ báo lỗi rõ ràng

DRIVE_FOLDER = _env("VNGIS_DRIVE_FOLDER", "VNGISDash_PILOT_2024" if MODE == "pilot" else "VNGISDash_Communes_2024")
RCLONE_REMOTE = _env("VNGIS_RCLONE_REMOTE", "gdrive")
REMOTE_BASE = f"{RCLONE_REMOTE}:{DRIVE_FOLDER}"
LOCAL_ROOT = _env("VNGIS_LOCAL_ROOT", os.path.expanduser(f"~/vngis_2024/{DRIVE_FOLDER}"))
CACHE_DIR = _env("VNGIS_CACHE_DIR", os.path.expanduser("~/vngis_2024/_cache"))
UPLOAD_EVERY_SEC = _env("VNGIS_UPLOAD_EVERY_SEC", 300, int)
DRIVE_STOP_POLL_SEC = 300

# Cấu trúc thư mục (giống hệt trên Drive)
D_CONTROL = "_control"
D_STATUS = f"{D_CONTROL}/status"
D_LOGS = f"{D_CONTROL}/logs"
D_T1 = "1_Task1_Spectral_Indices"
D_T2 = "2_Task2_Day_S2"
D_T3IMG = "3_Task3_Night_VIIRS"
D_T3CSV = "4_Task3_Economic_Indices"
D_MERGED = "_merged"

GADM_VNM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_VNM_shp.zip"   # notebook cell 3
ADM_COLS = ["GID_1", "NAME_1", "GID_2", "NAME_2", "GID_3", "NAME_3", "TYPE_3"]     # notebook cell 3

log = logging.getLogger("vngis")


def L(*parts):
    return os.path.join(LOCAL_ROOT, *parts)


# =====================================================================================
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
# 3. CHUẨN HÓA TÊN (2 bản khác nhau, đúng như notebook)
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
    """Biến của một xã, đặt tên giống các biến matched_* trong notebook."""
    row = dict(row)
    gid1, gid3 = str(row["GID_1"]), str(row["GID_3"])
    name1 = str(row["NAME_1"])
    cname_full = commune_full_name(row)
    clean_pname = normalize_str(name1)
    clean_cname = normalize_str(cname_full)
    safe_gid3 = gid3.replace(".", "_")
    prov_dir = f"{gid1}_{clean_pname}"
    t2_folder = f"S2_Day_{clean_pname}_{clean_cname}_{safe_gid3}_{TIME_START_STR}-{TIME_END_STR}"
    t3_folder = f"VIIRS_Night_{clean_pname}_{clean_cname}_{safe_gid3}_{TIME_START_STR}-{TIME_END_STR}"
    t1_name = f"s2_{normalize_str_t1(name1)}_{gid1}_{gid3.replace('.', '_')}_{YEAR}_Spectral_Indices.csv"
    t3_csv = f"VIIRS_Night_{clean_pname}_{clean_cname}_{safe_gid3}_{TIME_START_STR}-{TIME_END_STR}_Economic_Indices.csv"
    return {
        "row": row, "gid1": gid1, "gid3": gid3, "name1": name1, "cname_full": cname_full,
        "clean_pname": clean_pname, "clean_cname": clean_cname, "safe_gid3": safe_gid3,
        "rel_t1": f"{D_T1}/{prov_dir}/{t1_name}",
        "rel_t2_dir": f"{D_T2}/{prov_dir}/{t2_folder}",
        "rel_t3_dir": f"{D_T3IMG}/{prov_dir}/{t3_folder}",
        "rel_t3csv": f"{D_T3CSV}/{prov_dir}/{t3_csv}",
    }


def t2_name(ctx, y, m):
    return f"S2_Day_{ctx['clean_pname']}_{ctx['clean_cname']}_{ctx['safe_gid3']}_{y}{m:02d}.tif"


def t3_name(ctx, y, m):
    return f"VIIRS_Night_{ctx['clean_pname']}_{ctx['clean_cname']}_{ctx['safe_gid3']}_{y}{m:02d}.tif"


# =====================================================================================
# 4. EARTH ENGINE
# =====================================================================================
import ee

communes_fc = None
EE_CREDENTIALS = None


def init_earth_engine():
    global communes_fc, EE_CREDENTIALS
    kwargs = {"project": PROJECT_ID}
    if EE_HIGH_VOLUME:
        kwargs["opt_url"] = "https://earthengine-highvolume.googleapis.com"
    if EE_KEY_FILE:
        with open(EE_KEY_FILE, encoding="utf-8") as f:
            email = json.load(f)["client_email"]
        EE_CREDENTIALS = ee.ServiceAccountCredentials(email, EE_KEY_FILE)
        ee.Initialize(credentials=EE_CREDENTIALS, **kwargs)
        log.info(f"Earth Engine sẵn sàng: service account {email}, endpoint "
                 f"{'high-volume' if EE_HIGH_VOLUME else 'standard'}.")
    else:
        ee.Initialize(**kwargs)
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


def export_province_s2_local(
    province_gid,
    province_name,
    year,
    month,
    save_dir=None,
    gdf_admin=None,
    commune_gid=None,
):
  prov_prefix = province_gid.replace("_1", "") + "."
  prov_communes = communes_fc.filter(
      ee.Filter.stringStartsWith("GID_3", prov_prefix)
  )
  if commune_gid is not None:
    prov_communes = prov_communes.filter(ee.Filter.eq("GID_3", commune_gid))

  _, last_day = calendar.monthrange(year, month)
  s_date = f"{year}-{month:02d}-01"
  e_date = f"{year}-{month:02d}-{last_day:02d}"

  collection_id = (
      "COPERNICUS/S2_SR_HARMONIZED" if year >= 2019 else "COPERNICUS/S2_HARMONIZED"
  )
  _nb_print(f"[*] Nguồn dữ liệu: {collection_id}")

  raw_col = (
      ee.ImageCollection(collection_id)
      .filterBounds(prov_communes)
      .filterDate(s_date, e_date)
  )

  img_count = raw_col.size().getInfo()
  if img_count == 0:
    _nb_print(f"[-] Không có cảnh ảnh nào trong tháng {month:02d}/{year}")
    return None

  filtered_col = raw_col.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 85))
  if filtered_col.size().getInfo() == 0:
    composite = raw_col.map(mask_s2_sr).median()
  else:
    composite = filtered_col.map(mask_s2_sr).median()

  tensor = add_indices(composite)
  bands = [
      "BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2",
      "NDVI", "NDBI", "MNDWI", "BSI",
  ]
  reducers = ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True)

  stats = tensor.select(bands).reduceRegions(
      collection=prov_communes,
      reducer=reducers,
      scale=50,
      tileScale=4,
      crs="EPSG:4326",
  )

  feature_cols = [
      "BLUE_mean", "BLUE_stdDev", "GREEN_mean", "GREEN_stdDev",
      "RED_mean", "RED_stdDev", "NIR_mean", "NIR_stdDev",
      "SWIR1_mean", "SWIR1_stdDev", "SWIR2_mean", "SWIR2_stdDev",
      "NDVI_mean", "NDVI_stdDev", "NDBI_mean", "NDBI_stdDev",
      "MNDWI_mean", "MNDWI_stdDev", "BSI_mean", "BSI_stdDev",
  ]
  selected_cols = ["GID_3"] + feature_cols

  features = stats.select(selected_cols).getInfo()["features"]
  rows = [f["properties"] for f in features]
  df_s2 = pd.DataFrame(rows)

  if gdf_admin is not None:
    admin_cols = ["GID_1", "NAME_1", "GID_2", "NAME_2", "GID_3", "NAME_3", "TYPE_3"]
    existing_cols = [c for c in admin_cols if c in gdf_admin.columns]
    lookup = gdf_admin[existing_cols].drop_duplicates(subset=["GID_3"])
    df_res = pd.merge(lookup, df_s2, on="GID_3", how="inner")
  else:
    df_res = df_s2
  # Bản notebook ghi thêm 1 CSV cho từng tháng; pipeline chỉ giữ file gộp 12 tháng (save_dir=None).
  return df_res


# ---------- Task 2: chép nguyên văn notebook cell 18 ----------
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


def get_adaptive_monthly_composite(year, month, commune_geom, collection_id):
  _, last_day = calendar.monthrange(year, month)
  dt_start = datetime(year, month, 1)
  dt_end = datetime(year, month, last_day)

  s_date = dt_start.strftime("%Y-%m-%d")
  e_date = dt_end.strftime("%Y-%m-%d")

  col = (
      ee.ImageCollection(collection_id)
      .filterBounds(commune_geom)
      .filterDate(s_date, e_date)
  )

  if col.size().getInfo() == 0:
    exp_start = (dt_start - timedelta(days=15)).strftime("%Y-%m-%d")
    exp_end = (dt_end + timedelta(days=15)).strftime("%Y-%m-%d")

    col = (
        ee.ImageCollection(collection_id)
        .filterBounds(commune_geom)
        .filterDate(exp_start, exp_end)
    )

    if col.size().getInfo() == 0:
      exp_start_max = (dt_start - timedelta(days=30)).strftime("%Y-%m-%d")
      exp_end_max = (dt_end + timedelta(days=30)).strftime("%Y-%m-%d")

      col = (
          ee.ImageCollection(collection_id)
          .filterBounds(commune_geom)
          .filterDate(exp_start_max, exp_end_max)
      )

  if col.size().getInfo() > 0:
    return col.map(mask_s2_clean).median()
  return None


# ---------- Task 3: chép nguyên văn notebook cell 34 ----------
def get_viirs_monthly_composite(year, month, commune_geom):
  _, last_day = calendar.monthrange(year, month)
  s_date = f"{year}-{month:02d}-01"
  e_date = f"{year}-{month:02d}-{last_day:02d}"

  col = (
      ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG")
      .filterBounds(commune_geom)
      .filterDate(s_date, e_date)
  )

  if col.size().getInfo() == 0:
    col = (
        ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
        .filterBounds(commune_geom)
        .filterDate(s_date, e_date)
    )

  if col.size().getInfo() > 0:
    return (
        col.select(["avg_rad", "cf_cvg"])
        .mean()
        .clip(commune_geom)
        .set("system:time_start", s_date)
    )
  return None


# ---------- Task 3.2: chép nguyên văn notebook cell 38 (bọc thành hàm, print -> _nb_print) ----------
def compute_ntl_indices(commune_geom, target_prov_gid, matched_prov_name, matched_gid3, matched_cname):
  start_year = YEAR
  start_month = 1
  current_year = YEAR
  current_month = 12

  commune_area_ha = (
      commune_geom.area(maxError=1).divide(10000).getInfo()
  )

  records = []

  reducers = (
      ee.Reducer.sum()
      .combine(ee.Reducer.mean(), sharedInputs=True)
      .combine(ee.Reducer.stdDev(), sharedInputs=True)
      .combine(ee.Reducer.min(), sharedInputs=True)
      .combine(ee.Reducer.max(), sharedInputs=True)
      .combine(ee.Reducer.count(), sharedInputs=True)
  )

  for yr in range(start_year, current_year + 1):
    end_m = current_month if yr == current_year else 12
    for m in range(1, end_m + 1):
      check_stop()
      _, last_day = calendar.monthrange(yr, m)
      s_date = f"{yr}-{m:02d}-01"
      e_date = f"{yr}-{m:02d}-{last_day:02d}"

      col = (
          ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG")
          .filterBounds(commune_geom)
          .filterDate(s_date, e_date)
      )
      if col.size().getInfo() == 0:
        col = (
            ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
            .filterBounds(commune_geom)
            .filterDate(s_date, e_date)
        )

      if col.size().getInfo() == 0:
        _nb_print(f"  [-] Tháng {m:02d}/{yr}: Không có ảnh vệ tinh.")
        continue

      img = col.mean().clip(commune_geom)

      rad = img.select("avg_rad")
      cf_cvg = img.select("cf_cvg")

      lit_mask = rad.gte(1.5).rename("is_lit")
      lit_rad = rad.updateMask(lit_mask).rename("lit_rad")

      stats_all = rad.reduceRegion(
          reducer=reducers, geometry=commune_geom, scale=500, maxPixels=1e9, crs="EPSG:4326",
      ).getInfo()

      stats_lit = lit_rad.reduceRegion(
          reducer=ee.Reducer.sum(), geometry=commune_geom, scale=500, maxPixels=1e9, crs="EPSG:4326",
      ).getInfo()

      lit_pixel_count = lit_mask.reduceRegion(
          reducer=ee.Reducer.sum(), geometry=commune_geom, scale=500, maxPixels=1e9, crs="EPSG:4326",
      ).getInfo().get("is_lit", 0)

      cloud_free_obs = cf_cvg.reduceRegion(
          reducer=ee.Reducer.mean(), geometry=commune_geom, scale=500, maxPixels=1e9, crs="EPSG:4326",
      ).getInfo().get("cf_cvg", 0)

      total_pixels = stats_all.get("avg_rad_count", 0)
      tnl = stats_all.get("avg_rad_sum", 0.0) or 0.0
      mean_rad = stats_all.get("avg_rad_mean", 0.0) or 0.0
      std_rad = stats_all.get("avg_rad_stdDev", 0.0) or 0.0
      min_rad = stats_all.get("avg_rad_min", 0.0) or 0.0
      max_rad = stats_all.get("avg_rad_max", 0.0) or 0.0

      lit_pop_proxy = (
          stats_lit.get("lit_rad", 0.0) or 0.0
      )

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
    # Notebook sẽ báo KeyError ở dòng dưới; pipeline báo lỗi rõ ràng thay vì ghi file rỗng.
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
    r = requests.get(url, timeout=timeout)
    if r.status_code in (401, 403) and EE_CREDENTIALS is not None:
        try:
            from google.auth.transport.requests import AuthorizedSession
            r2 = AuthorizedSession(EE_CREDENTIALS).get(url, timeout=timeout)
            if r2.status_code < 400:
                return r2
            r = r2
        except Exception as exc:
            log.debug(f"AuthorizedSession lỗi: {exc}")
    return r


def _classify(msg):
    low = msg.lower()
    if any(k in low for k in _TOO_LARGE_ERR):
        return "too_large"
    if any(k in low for k in _PERMANENT_ERR):
        return "permanent"
    return "transient"


def fetch_geotiff_bytes(img, region, scale, max_retry=4):
    """Đúng tham số notebook cell 21. Trả bytes GeoTIFF; lỗi nào cũng kèm mã HTTP và nội dung."""
    last = None
    for attempt in range(max_retry):
        check_stop()
        try:
            url = img.getDownloadURL({"region": region, "scale": scale, "crs": "EPSG:4326",
                                      "format": "GEO_TIFF", "filePerBand": False})
        except ee.EEException as exc:
            kind = _classify(str(exc))
            if kind == "too_large":
                raise TooLargeError(str(exc))
            if kind == "permanent":
                raise PermanentError(f"getDownloadURL bị từ chối: {exc}")
            last = f"getDownloadURL: {exc}"
            time.sleep(5 * (attempt + 1))
            continue
        try:
            r = _http_get(url)
        except requests.RequestException as exc:
            last = f"mạng: {exc}"
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code >= 400:
            body = (r.text or "")[:400].replace("\n", " ")
            msg = f"HTTP {r.status_code}: {body}"
            kind = _classify(msg) if r.status_code not in (401, 403) else "permanent"
            if kind == "too_large":
                raise TooLargeError(msg)
            if kind == "permanent":
                raise PermanentError(msg)
            last = msg
            time.sleep(5 * (attempt + 1))
            continue
        data = r.content
        if data[:2] == b"PK":                       # đôi khi EE trả về file zip (notebook cell 21)
            z = zipfile.ZipFile(io.BytesIO(data))
            data = z.read([n for n in z.namelist() if n.lower().endswith(".tif")][0])
        if len(data) < 200:
            last = f"file tải về chỉ {len(data)} byte"
            time.sleep(5 * (attempt + 1))
            continue
        return data
    raise RuntimeError(f"Tải thất bại sau {max_retry} lần: {last}")


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
               "dtype": arr.dtype, "crs": crs, "transform": transform, "nodata": nodata}
    if COMPRESS_TIF:
        profile.update(compress="DEFLATE", predictor=3 if np.issubdtype(arr.dtype, np.floating) else 2,
                       tiled=True, blockxsize=256, blockysize=256)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr)
        for i, d in enumerate(desc or [], start=1):
            if d:
                dst.set_band_description(i, d)


def _rewrite_bytes_to_tif(data, path):
    import rasterio
    with rasterio.MemoryFile(data) as mf, mf.open() as src:
        arr, tr, crs, nd, desc = src.read(), src.transform, src.crs, src.nodata, src.descriptions
    write_tif(path, arr, tr, crs, nd, desc)


def _bbox(region):
    coords = region.bounds(maxError=1).getInfo()["coordinates"][0]
    xs, ys = [c[0] for c in coords], [c[1] for c in coords]
    return min(xs), min(ys), max(xs), max(ys)


def _split_bbox(bbox, n):
    x0, y0, x1, y1 = bbox
    dx, dy = (x1 - x0) / n, (y1 - y0) / n
    return [ee.Geometry.Rectangle([x0 + i * dx, y0 + j * dy, x0 + (i + 1) * dx, y0 + (j + 1) * dy],
                                  "EPSG:4326", False)
            for j in range(n) for i in range(n)]


def download_tif(img, region, scale, path, label, force_tiles=0):
    """Tải ảnh về `path`. Nếu vượt hạn mức thì chia ô (cùng scale, cùng lưới) rồi ghép.
    Trả số ô đã dùng (1 = tải nguyên). Mọi thất bại đều được ném ra kèm nguyên nhân."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    try:
        if not force_tiles:
            try:
                data = fetch_geotiff_bytes(img, region, scale)
                _rewrite_bytes_to_tif(data, tmp)
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
                write_tif(tmp, arr, tr, crs, nd, desc)
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


# =====================================================================================
# 7. XỬ LÝ MỘT XÃ
# =====================================================================================
class NotInAsset(RuntimeError):
    pass


def _write_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path + ".part", index=False, encoding="utf-8-sig")
    os.replace(path + ".part", path)


ADMIN_DF = None      # bảng hành chính GADM (vai trò gdf_cleaned trong notebook)


def process_commune(row, prev):
    """prev: bản ghi trạng thái lần trước (để chạy tiếp phần còn thiếu, không làm lại phần đã xong)."""
    check_stop()
    t_start = time.time()
    ctx = build_ctx(row)
    gid3 = ctx["gid3"]
    prev = prev or {}
    info = {"gid_3": gid3, "gid_1": ctx["gid1"], "run_id": RUN_ID,
            "t1": prev.get("t1", "pending"),
            "t2": dict(prev.get("t2") or {}), "t3img": dict(prev.get("t3img") or {}),
            "t3csv": prev.get("t3csv", "pending"),
            "empty_months_t2": prev.get("empty_months_t2", []), "tiles_used": prev.get("tiles_used", {}),
            "errors": []}

    fc = communes_fc.filter(ee.Filter.eq("GID_3", gid3))
    if fc.size().getInfo() == 0:
        raise NotInAsset(f"GID_3 {gid3} không có trong asset {ASSET_ID}")
    commune_geom = fc.geometry()

    # ---------- Task 1 ----------
    if info["t1"] != "ok":
        try:
            t1_monthly = []
            for m in MONTHS:
                check_stop()
                df_result = export_province_s2_local(
                    province_gid=ctx["gid1"], province_name=ctx["name1"], year=YEAR, month=m,
                    save_dir=None, gdf_admin=ADMIN_DF, commune_gid=gid3)
                if df_result is not None:
                    t1_monthly.append(df_result.assign(YEAR=YEAR, MONTH=m))
            if not t1_monthly:
                raise RuntimeError("Task 1: không tháng nào có cảnh Sentinel-2")
            df_t1_year = pd.concat(t1_monthly, ignore_index=True)
            if df_t1_year.empty:
                raise RuntimeError("Task 1: kết quả rỗng (GID_3 không khớp bảng GADM?)")
            _write_csv(df_t1_year, L(ctx["rel_t1"]))
            info["t1"] = "ok"
            info["t1_months"] = int(len(df_t1_year))
        except StopRequested:
            raise
        except Exception as exc:
            info["t1"] = "fail"
            info["errors"].append(f"T1: {type(exc).__name__}: {str(exc)[:200]}")
            if isinstance(exc, PermanentError):
                raise

    # ---------- Task 2 ----------
    for m in MONTHS:
        key = f"{m:02d}"
        if info["t2"].get(key) in ("ok", "none"):
            continue
        check_stop()
        try:
            collection_id = "COPERNICUS/S2_SR_HARMONIZED" if YEAR >= 2019 else "COPERNICUS/S2_HARMONIZED"
            composite = get_adaptive_monthly_composite(YEAR, m, commune_geom, collection_id)
            if composite is None:
                info["t2"][key] = "none"           # notebook: "Bỏ qua tháng: không tìm thấy ảnh"
                continue
            export_img = add_indices(composite).clip(commune_geom)
            path = L(ctx["rel_t2_dir"], t2_name(ctx, YEAR, m))
            n = download_tif(export_img, commune_geom, 20, path, f"[{gid3}] T2 {YEAR}-{key}")
            ok, empty, note = inspect_tif(path, 10)
            if not ok:
                raise RuntimeError(f"file kiểm tra lỗi: {note}")
            if empty and key not in info["empty_months_t2"]:
                info["empty_months_t2"].append(key)
            if n > 1:
                info["tiles_used"][f"t2_{key}"] = n
            info["t2"][key] = "ok"
        except StopRequested:
            raise
        except PermanentError:
            raise
        except Exception as exc:
            info["t2"][key] = "fail"
            info["errors"].append(f"T2 {key}: {type(exc).__name__}: {str(exc)[:200]}")

    # ---------- Task 3.1 ----------
    for m in MONTHS:
        key = f"{m:02d}"
        if info["t3img"].get(key) in ("ok", "none"):
            continue
        check_stop()
        try:
            night_img = get_viirs_monthly_composite(YEAR, m, commune_geom)
            if night_img is None:
                info["t3img"][key] = "none"
                continue
            path = L(ctx["rel_t3_dir"], t3_name(ctx, YEAR, m))
            download_tif(night_img.toDouble(), commune_geom, 500, path, f"[{gid3}] T3 {YEAR}-{key}")
            ok, _empty, note = inspect_tif(path, 2)
            if not ok:
                raise RuntimeError(f"file kiểm tra lỗi: {note}")
            info["t3img"][key] = "ok"
        except StopRequested:
            raise
        except PermanentError:
            raise
        except Exception as exc:
            info["t3img"][key] = "fail"
            info["errors"].append(f"T3img {key}: {type(exc).__name__}: {str(exc)[:200]}")

    # ---------- Task 3.2 ----------
    if info["t3csv"] != "ok":
        try:
            df_ntl = compute_ntl_indices(commune_geom, ctx["gid1"], ctx["name1"], gid3, ctx["cname_full"])
            _write_csv(df_ntl, L(ctx["rel_t3csv"]))
            info["t3csv"] = "ok"
            info["t3_months"] = int(len(df_ntl))
        except StopRequested:
            raise
        except Exception as exc:
            info["t3csv"] = "fail"
            info["errors"].append(f"T3csv: {type(exc).__name__}: {str(exc)[:200]}")

    core_ok = (info["t1"] == "ok" and info["t3csv"] == "ok"
               and all(info["t2"].get(f"{m:02d}") in ("ok", "none") for m in MONTHS)
               and all(info["t3img"].get(f"{m:02d}") in ("ok", "none") for m in MONTHS))
    info["status"] = "done" if core_ok else "partial"
    info["seconds"] = round(time.time() - t_start, 1)
    info["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return info


# =====================================================================================
# 8. TRẠNG THÁI (mỗi lượt một file .jsonl trong _control/status, bản ghi sau cùng thắng)
# =====================================================================================
_status_lock = threading.Lock()
STATUS_FILE = None


def write_status(info):
    line = json.dumps(info, ensure_ascii=False, default=str)
    with _status_lock:
        os.makedirs(L(D_STATUS), exist_ok=True)
        with open(STATUS_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def load_all_status():
    out = {}
    for p in sorted(glob.glob(L(D_STATUS, "status_*.jsonl"))):
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(d, dict) and "gid_3" in d:
                        out[d["gid_3"]] = d
        except OSError:
            continue
    return out


def core_finished(st):
    return bool(st) and (st.get("status") == "done" or st.get("status") == "not_in_asset"
                         or int(st.get("attempts", 0)) >= MAX_ATTEMPTS)



# =====================================================================================
# 9. RCLONE
# =====================================================================================
RCLONE_COMMON = ["--transfers", "4", "--checkers", "8", "--tpslimit", "8",
                 "--retries", "5", "--low-level-retries", "20", "--stats-log-level", "NOTICE"]


def _rclone(args, timeout=6 * 3600, quiet=False):
    try:
        res = subprocess.run(["rclone", *args], capture_output=True, text=True, timeout=timeout)
    except Exception as exc:
        log.warning(f"rclone {' '.join(args[:2])} lỗi: {exc}")
        return False
    if res.returncode != 0 and not quiet:
        log.warning(f"rclone {' '.join(args[:3])} lỗi: {res.stderr.strip()[-400:]}")
    return res.returncode == 0


_sync_lock = threading.Lock()


def rclone_sync_once(final=False):
    """TIF: move (rclone chỉ xóa bản trên máy sau khi đã kiểm tra kích thước/hash bản trên Drive).
    CSV, trạng thái, log: copy."""
    if not os.path.isdir(LOCAL_ROOT):
        return
    with _sync_lock:
        age = [] if final else ["--min-age", "2m"]
        for d in (D_T2, D_T3IMG):
            src = L(d)
            if os.path.isdir(src):
                _rclone(["move", src, f"{REMOTE_BASE}/{d}", "--filter", "- *.part", "--filter", "+ *.tif",
                         "--filter", "- *", *age, *RCLONE_COMMON])
        for d in (D_T1, D_T3CSV, D_MERGED, D_CONTROL):
            src = L(d)
            if os.path.isdir(src):
                _rclone(["copy", src, f"{REMOTE_BASE}/{d}", "--filter", "- *.part", *RCLONE_COMMON])


class Uploader(threading.Thread):
    def __init__(self):
        super().__init__(name="uploader", daemon=True)
        self.stop_event = threading.Event()

    def run(self):
        last_stop_check = 0
        while not self.stop_event.wait(min(UPLOAD_EVERY_SEC, 60)):
            now = time.time()
            if now - last_stop_check >= DRIVE_STOP_POLL_SEC:
                last_stop_check = now
                if drive_stop_exists():
                    request_stop("drive_stop")
            if now - getattr(self, "_last_sync", 0) >= UPLOAD_EVERY_SEC:
                self._last_sync = now
                try:
                    rclone_sync_once()
                    log.info("Đã đồng bộ lên Drive (định kỳ).")
                except Exception as exc:
                    log.warning(f"Đồng bộ định kỳ lỗi: {exc}")


def drive_stop_exists():
    try:
        res = subprocess.run(["rclone", "lsf", f"{REMOTE_BASE}/{D_CONTROL}", "--files-only"],
                             capture_output=True, text=True, timeout=120)
        return res.returncode == 0 and "STOP" in [x.strip() for x in res.stdout.splitlines()]
    except Exception:
        return False


def init_storage():
    for d in (LOCAL_ROOT, L(D_STATUS), L(D_LOGS), CACHE_DIR):
        os.makedirs(d, exist_ok=True)
    if shutil.which("rclone") is None:
        raise RuntimeError("Chưa cài rclone.")
    if not _rclone(["mkdir", f"{REMOTE_BASE}/{D_STATUS}"], timeout=180):
        raise RuntimeError(f"rclone không ghi được vào '{REMOTE_BASE}'. Kiểm tra secret RCLONE_CONF.")
    res = subprocess.run(["rclone", "copy", f"{REMOTE_BASE}/{D_STATUS}", L(D_STATUS), "--update"],
                         capture_output=True, text=True, timeout=3600)
    if res.returncode != 0:
        raise RuntimeError(f"Không kéo được trạng thái từ Drive: {res.stderr.strip()[-300:]}")
    # Cảnh báo nếu đích còn cấu trúc của bản pipeline cũ
    old = subprocess.run(["rclone", "lsf", REMOTE_BASE, "--dirs-only"], capture_output=True, text=True, timeout=120)
    if any(x.strip("/") in ("03_Provinces", "04_Status", "01_Index") for x in old.stdout.splitlines()):
        log.warning(f"Thư mục '{DRIVE_FOLDER}' trên Drive còn dữ liệu của bản pipeline cũ (03_Provinces, 04_Status...). "
                    f"Nên xóa thư mục cũ rồi chạy lại để không lẫn dữ liệu.")


# =====================================================================================
# 10. DANH SÁCH XÃ (GADM 4.1, giống notebook cell 3)
# =====================================================================================
def build_admin_table():
    idx_csv = os.path.join(CACHE_DIR, "gadm41_VNM_3_admin.csv")
    if os.path.isfile(idx_csv):
        return pd.read_csv(idx_csv, dtype=str, keep_default_na=False)
    import geopandas as gpd
    zip_path = os.path.join(CACHE_DIR, "gadm41_VNM_shp.zip")
    ext = os.path.join(CACHE_DIR, "gadm41_VNM")
    if not os.path.isfile(os.path.join(ext, "gadm41_VNM_3.shp")):
        if not os.path.isfile(zip_path):
            log.info("Tải ranh giới GADM 4.1...")
            r = requests.get(GADM_VNM_URL, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=900)
            r.raise_for_status()
            with open(zip_path + ".part", "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(zip_path + ".part", zip_path)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(ext)
    g = gpd.read_file(os.path.join(ext, "gadm41_VNM_3.shp"))
    tbl = pd.DataFrame(g[ADM_COLS]).astype(str).drop_duplicates("GID_3")
    tbl.to_csv(idx_csv, index=False, encoding="utf-8-sig")
    return pd.read_csv(idx_csv, dtype=str, keep_default_na=False)


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
# 11. PREFLIGHT
# =====================================================================================
class PreflightError(RuntimeError):
    pass


PREFLIGHT_HINT = (
    "Gợi ý: (1) HTTP 401/403 hoặc 'permission': cấp role 'Earth Engine Resource Writer' (roles/earthengine.writer) "
    "và 'Service Usage Consumer' (roles/serviceusage.serviceUsageConsumer) cho service account, đăng ký project "
    "với Earth Engine; (2) thử VNGIS_EE_HIGH_VOLUME=true nếu standard bị chặn; (3) lỗi rclone: kiểm tra RCLONE_CONF.")


def preflight(row):
    gid3 = row["GID_3"]
    log.info(f"[preflight] 1/4 Asset ranh giới: {ASSET_ID}")
    fc = communes_fc.filter(ee.Filter.eq("GID_3", gid3))
    n = fc.size().getInfo()
    if n == 0:
        raise PreflightError(f"Asset không có xã {gid3}. Kiểm tra GID_3 trong asset có khớp GADM 4.1 không.")
    geom = fc.geometry()
    region = geom.centroid(maxError=1).buffer(1500)

    log.info(f"[preflight] 2/4 Tải thử ảnh ngày Sentinel-2 (xã {gid3}, vùng nhỏ quanh tâm xã)")
    comp = None
    for m in MONTHS:
        comp = get_adaptive_monthly_composite(YEAR, m, geom, "COPERNICUS/S2_SR_HARMONIZED")
        if comp is not None:
            break
    if comp is None:
        raise PreflightError("Không tìm thấy ảnh Sentinel-2 nào cho xã thử.")
    img = add_indices(comp).clip(region)
    try:
        whole = fetch_geotiff_bytes(img, region, 20)
    except Exception as exc:
        raise PreflightError(f"Tải ảnh ngày thất bại: {exc}")
    import rasterio
    with rasterio.MemoryFile(whole) as mf, mf.open() as src:
        if src.count != 10:
            raise PreflightError(f"Ảnh ngày có {src.count} kênh, cần 10.")
        a_whole, tr_whole = src.read(), src.transform
    log.info(f"[preflight] Ảnh ngày OK: {len(whole)/1024:.0f} KB, 10 kênh, pixel {tr_whole.a:.8f} độ")

    log.info("[preflight] 3/4 Kiểm tra cách chia ô (ảnh lớn) cho ra đúng lưới pixel như tải nguyên")
    try:
        parts = [fetch_geotiff_bytes(img, rect, 20) for rect in _split_bbox(_bbox(region), 2)]
        a_tiled, tr_tiled, *_ = mosaic_tiles(parts)
        compare_on_grid(a_whole, tr_whole, a_tiled, tr_tiled)
        log.info("[preflight] Chia ô OK: ảnh ghép trùng khớp từng pixel với ảnh tải nguyên.")
    except Exception as exc:
        TILING_OK[0] = False
        log.error(f"[preflight] Kiểm tra chia ô KHÔNG ĐẠT ({exc}). Pipeline vẫn chạy; xã nào quá lớn cần chia ô "
                  f"sẽ được đánh dấu lỗi thay vì ghép sai.")

    vimg = get_viirs_monthly_composite(YEAR, 1, geom)
    if vimg is None:
        raise PreflightError("Không có ảnh VIIRS tháng 01/2024.")
    try:
        nb = fetch_geotiff_bytes(vimg.toDouble().clip(region), region, 500)
    except Exception as exc:
        raise PreflightError(f"Tải ảnh đêm thất bại: {exc}")
    log.info(f"[preflight] Ảnh đêm OK: {len(nb)/1024:.0f} KB")

    log.info("[preflight] 4/4 Ghi và đọc lại file thử trên Drive")
    probe = L(D_CONTROL, "preflight_probe.txt")
    stamp = f"{RUN_ID} {datetime.now(timezone.utc).isoformat()}"
    with open(probe, "w", encoding="utf-8") as f:
        f.write(stamp)
    if not _rclone(["copyto", probe, f"{REMOTE_BASE}/{D_CONTROL}/preflight_probe.txt"], timeout=300):
        raise PreflightError("rclone không ghi được lên Drive.")
    res = subprocess.run(["rclone", "cat", f"{REMOTE_BASE}/{D_CONTROL}/preflight_probe.txt"],
                         capture_output=True, text=True, timeout=300)
    if res.returncode != 0 or res.stdout.strip() != stamp:
        raise PreflightError(f"Đọc lại file thử trên Drive không khớp: {res.stderr.strip()[-200:]}")
    log.info("[preflight] ĐẠT: Earth Engine, quyền tải ảnh, chia ô và Google Drive đều hoạt động.")


# =====================================================================================
# 12. GỘP CSV
# =====================================================================================
INT_COLS = ("YEAR", "MONTH", "LIT_PIXELS")


def _read_many(pattern):
    frames = []
    for p in sorted(glob.glob(pattern, recursive=True)):
        try:
            f = pd.read_csv(p, dtype={c: str for c in ADM_COLS}, keep_default_na=True)
            if not f.empty:
                frames.append(f)
        except Exception as exc:
            log.warning(f"Không đọc được {p}: {exc}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    for c in INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df


def build_merged(pull=True):
    if pull:
        for d in (D_T1, D_T3CSV):
            _rclone(["copy", f"{REMOTE_BASE}/{d}", L(d), "--filter", "+ *.csv", "--filter", "- *", *RCLONE_COMMON])
    specs = [(D_T1, "Task1_Spectral_Indices"), (D_T3CSV, "Task3_Economic_Indices")]
    out_dir = L(D_MERGED)
    os.makedirs(os.path.join(out_dir, "by_province"), exist_ok=True)
    for d, name in specs:
        df = _read_many(L(d, "**", "*.csv"))
        if df.empty:
            continue
        _write_csv(df, os.path.join(out_dir, f"{name}_{YEAR}_ALL.csv"))
        for gid1, part in df.groupby("GID_1"):
            _write_csv(part, os.path.join(out_dir, "by_province", f"{name}_{gid1}.csv"))
        log.info(f"Gộp {name}: {len(df):,} dòng")


def write_progress(targets, statuses):
    rows = []
    for r in targets.itertuples():
        st = statuses.get(r.GID_3) or {}
        t2 = st.get("t2") or {}
        t3 = st.get("t3img") or {}
        rows.append({"GID_1": r.GID_1, "NAME_1": r.NAME_1, "GID_3": r.GID_3, "NAME_3": r.NAME_3,
                     "status": st.get("status", "pending"), "attempts": st.get("attempts", 0),
                     "t1": st.get("t1"), "t2_ok": sum(v == "ok" for v in t2.values()),
                     "t2_none": sum(v == "none" for v in t2.values()),
                     "t3img_ok": sum(v == "ok" for v in t3.values()), "t3csv": st.get("t3csv"),
                     "seconds": st.get("seconds"), "finished_at": st.get("finished_at"),
                     "errors": " | ".join(st.get("errors") or [])[:500]})
    df = pd.DataFrame(rows)
    _write_csv(df, L(D_CONTROL, "progress.csv"))
    return df


# =====================================================================================
# 13. VÒNG CHẠY
# =====================================================================================
OUTAGE_STREAK = 10
OUTAGE_SLEEP_SEC = 900
MAX_OUTAGES = 8


def run_round(jobs, statuses):
    """jobs: list (gid, row, kind)."""
    t0 = time.time()
    done_now = 0
    pending_fail = []
    outage = False
    pool = ThreadPoolExecutor(max_workers=N_WORKERS, thread_name_prefix="w")
    futures = {}
    for gid, row, kind in jobs:
        futures[pool.submit(process_commune, row, statuses.get(gid))] = (gid, kind)
    handled = set()

    def consume(fut):
        nonlocal done_now
        handled.add(fut)
        if fut.cancelled():
            return
        gid, kind = futures[fut]
        prev = statuses.get(gid) or {}
        attempts = int(prev.get("attempts", 0)) + 1
        try:
            info = fut.result()
        except StopRequested:
            return
        except NotInAsset as exc:
            info = {"gid_3": gid, "status": "not_in_asset", "attempts": MAX_ATTEMPTS, "errors": [str(exc)],
                    "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            statuses[gid] = info
            write_status(info)
            log.error(f"[{gid}] {exc}")
            return
        except PermanentError as exc:
            log.error(f"[{gid}] lỗi quyền truy cập: {exc}")
            _dl_record(False, exc)
            pending_fail.append((gid, exc, attempts))
            return
        except Exception as exc:
            pending_fail.append((gid, exc, attempts))
            log.warning(f"[{gid}] lỗi: {type(exc).__name__}: {str(exc)[:200]}")
            return
        for g, e, a in pending_fail:
            statuses[g] = _fail_info(g, e, a, statuses.get(g))
            write_status(statuses[g])
        pending_fail.clear()
        info["attempts"] = attempts
        statuses[gid] = info
        write_status(info)
        done_now += 1
        errs = f" | lỗi: {info['errors'][:2]}" if info.get("errors") else ""
        t2 = info.get("t2") or {}
        log.info(f"[{gid}] {kind} -> {info.get('status')} | T1={info.get('t1')} "
                 f"T2={sum(v == 'ok' for v in t2.values())}/12 T3img={sum(v == 'ok' for v in (info.get('t3img') or {}).values())}/12 "
                 f"T3csv={info.get('t3csv')} | {info.get('seconds', 0)}s | lần {attempts}{errs}")
        if done_now % 20 == 0:
            rate = done_now / max(time.time() - t0, 1)
            log.info(f"Tiến độ vòng: {done_now}/{len(jobs)} | {rate*3600:.0f} việc/giờ")

    try:
        for fut in as_completed(futures):
            consume(fut)
            if len(pending_fail) >= OUTAGE_STREAK:
                log.error(f"{OUTAGE_STREAK} xã lỗi liên tiếp: nghi sự cố chung, tạm dừng vòng.")
                outage = True
                break
    except BaseException:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown(wait=True, cancel_futures=True)
    for fut in futures:
        if fut not in handled and fut.done():
            consume(fut)
    if outage:
        return "outage"
    for g, e, a in pending_fail:
        statuses[g] = _fail_info(g, e, a, statuses.get(g))
        write_status(statuses[g])
    return "stop" if STOP_EVENT.is_set() else "ok"


def _fail_info(gid, exc, attempts, prev):
    info = dict(prev or {})
    info.update({"gid_3": gid, "status": "failed", "attempts": attempts, "run_id": RUN_ID,
                 "errors": [f"{type(exc).__name__}: {str(exc)[:300]}"],
                 "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    return info


def main():
    global STATUS_FILE, ADMIN_DF
    os.makedirs(L(D_STATUS), exist_ok=True)
    STATUS_FILE = L(D_STATUS, f"status_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{RUN_ID}.jsonl")
    setup_logging()
    log.info(f"VNGISDash {YEAR} | chế độ {MODE} | đích {REMOTE_BASE} | {N_WORKERS} luồng")
    try:
        init_storage()
        ADMIN_DF = build_admin_table()
        targets = load_targets(ADMIN_DF)
        init_earth_engine()
    except Exception as exc:
        log.error(f"Khởi tạo thất bại: {type(exc).__name__}: {exc}")
        log.error(PREFLIGHT_HINT)
        return 1
    rows = {r["GID_3"]: r for r in targets.to_dict("records")}
    gids = list(rows)
    log.info(f"Danh sách: {len(gids):,} xã, {targets['GID_1'].nunique()} tỉnh"
             + (f" (thí điểm: {', '.join(gids)})" if MODE == "pilot" else ""))

    statuses = load_all_status()
    if PREFLIGHT:
        pend = [g for g in gids if not core_finished(statuses.get(g))] or gids
        for attempt in (1, 2, 3):
            try:
                preflight(rows[pend[0]])
                break
            except Exception as exc:
                log.error(f"[preflight] THẤT BẠI (lần {attempt}/3): {type(exc).__name__}: {exc}")
                if isinstance(exc, PreflightError) or _classify(str(exc)) == "permanent":
                    log.error(PREFLIGHT_HINT)
                    rclone_sync_once(final=True)
                    return 1
                if attempt == 3:
                    log.error(PREFLIGHT_HINT)
                    rclone_sync_once(final=True)
                    return 1
                time.sleep(20 * attempt)

    install_signal_handlers()
    if MAX_RUNTIME_SEC > 0:
        t = threading.Timer(MAX_RUNTIME_SEC, request_stop, args=("deadline",))
        t.daemon = True
        t.start()
        log.info(f"Thời gian tối đa của lượt: {MAX_RUNTIME_SEC/3600:.2f} giờ")
    if drive_stop_exists():
        log.warning("Có file _control/STOP trên Drive: không chạy.")
        return 130

    uploader = Uploader()
    uploader.start()
    outages, code = 0, 0
    try:
        while not STOP_EVENT.is_set():
            statuses = load_all_status()
            write_progress(targets, statuses)
            jobs = [(g, rows[g], "full") for g in gids if not core_finished(statuses.get(g))]
            if not jobs:
                break
            log.info(f"Vòng mới: {len(jobs):,} xã cần xử lý")
            result = run_round(jobs, statuses)
            if result == "stop":
                break
            if result == "outage":
                outages += 1
                if outages >= MAX_OUTAGES:
                    code = 2
                    break
                STOP_EVENT.wait(OUTAGE_SLEEP_SEC)
            else:
                outages = 0
    except KeyboardInterrupt:
        code = 130
    finally:
        uploader.stop_event.set()
        uploader.join(timeout=60)

    statuses = load_all_status()
    progress = write_progress(targets, statuses)
    unfinished = [g for g in gids if not core_finished(statuses.get(g))]
    rclone_sync_once(final=True)

    if not unfinished and not STOP_EVENT.is_set():
        build_merged(pull=True)
        rclone_sync_once(final=True)
        failed = progress[progress["status"] != "done"]
        log.info(f"HOÀN TẤT: {progress['status'].value_counts().to_dict()}")
        if len(failed):
            log.warning(f"{len(failed)} xã không đạt sau {MAX_ATTEMPTS} lần, xem _control/progress.csv")
        return code or 0

    log.info(f"Chưa xong: còn {len(unfinished):,} xã | {progress['status'].value_counts().to_dict()}")
    if code:
        return code
    if STOP_REASON[0] == "fatal":
        log.error(f"Dừng vì lỗi tải ảnh. Lỗi gần nhất: {_dl_stats['last_err']}")
        log.error(PREFLIGHT_HINT)
        return 1
    if STOP_REASON[0] in ("signal", "drive_stop"):
        return 130
    return 3


if __name__ == "__main__" and os.environ.get("VNGIS_SKIP_MAIN") != "1":
    if "--sync-only" in sys.argv:
        setup_logging()
        rclone_sync_once(final=True)
        log.info("Đồng bộ nốt xong.")
        sys.exit(0)
    if "--merge-only" in sys.argv:
        setup_logging()
        build_merged(pull=True)
        rclone_sync_once(final=True)
        sys.exit(0)
    sys.exit(main())
