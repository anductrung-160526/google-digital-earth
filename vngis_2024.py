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

Nền khoa học v6 được giữ nguyên. Điều phối trong v6_runtime.py kiểm kê dữ liệu thật,
hoàn tất toàn bộ ngày rồi mới xử lý đêm. Chỉ tải ảnh thiếu/lỗi; chỉ số cũ hợp lệ được giữ.

Chạy:
    python vngis_2024.py               chạy pipeline
    python vngis_2024.py --sync-only   đẩy nốt dữ liệu trên máy lên Drive

Mã thoát: 0 bước yêu cầu hoàn tất | 1 lỗi/hết giới hạn thử | 3 hết giờ (nối lượt) | 130 dừng tay
"""

import os, io, re, sys, json, time, glob, math, shutil, zipfile, signal, logging, calendar, struct
import threading, subprocess, unicodedata, warnings
sys.modules.setdefault("vngis_2024", sys.modules[__name__])
import data_contract as D
from request_control import RequestGate, retry_delay, status_code
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
    from v6_runtime import rclone_run
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
    from v6_runtime import main as run
    parser = argparse.ArgumentParser()
    parser.add_argument("step", nargs="?", choices=["run", "inventory"], default="run")
    parser.add_argument("--sync-only", action="store_true")
    args = parser.parse_args()
    return run(sys.modules[__name__], step=args.step, sync_only=args.sync_only)


if __name__ == "__main__" and os.environ.get("VNGIS_SKIP_MAIN") != "1":
    sys.exit(main())
