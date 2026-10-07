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

Tăng tốc so với notebook (không đổi công thức, tham số): Task 1 và Task 3.2 gom 12 tháng thành 1 lần gọi
Earth Engine; việc kiểm tra "tháng có ảnh không" của Task 2/3.1 gom thành 1 lần gọi; 24 ảnh của một xã
tải song song.

Chạy:
    python vngis_2024.py               chạy pipeline
    python vngis_2024.py --sync-only   đẩy nốt dữ liệu trên máy lên Drive

Mã thoát: 0 xong toàn bộ | 1 lỗi cấu hình hoặc preflight | 2 sự cố EE kéo dài | 3 hết giờ (nối lượt) | 130 dừng tay
"""

import os, io, re, sys, json, time, glob, math, shutil, zipfile, signal, logging, calendar, struct
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

PROJECT_ID = "digital-vietnam-earth"                               # notebook cell 11
ASSET_ID = f"projects/{PROJECT_ID}/assets/communes_l3"             # notebook cell 11

MODE = _env("VNGIS_MODE", "pilot").lower()                        # pilot | full
if MODE not in ("pilot", "full"):
    raise SystemExit(f"VNGIS_MODE phải là 'pilot' hoặc 'full', đang là '{MODE}'")
PILOT_N = _env("VNGIS_PILOT_N", 2, int)
if MODE == "pilot" and PILOT_N < 1:                   # chế độ full không dùng số này
    raise SystemExit("VNGIS_PILOT_N phải >= 1")

# Ảnh ngày: số kênh lưu vào tif. 6 = BLUE..SWIR2 (NDVI, NDBI, MNDWI, BSI tính lại được từ 6 kênh này);
# 10 = đủ 10 kênh như notebook (file lớn gần gấp đôi).
DAY_BANDS_ALL = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "NDVI", "NDBI", "MNDWI", "BSI"]
DAY_BANDS = _env("VNGIS_DAY_BANDS", 10, int)
if DAY_BANDS not in (6, 10):
    raise SystemExit("VNGIS_DAY_BANDS phải là 6 hoặc 10")
# Kiểu lưu ảnh ngày: float = giữ nguyên giá trị Earth Engine trả về, nén DEFLATE không mất dữ liệu (như bản 4);
# int16 = số nguyên round(giá trị × VNGIS_DAY_SCALE), file nhỏ hơn ~3-4 lần nhưng nhiều trình xem không mở được.
DAY_FORMAT = _env("VNGIS_DAY_FORMAT", "float").lower()
if DAY_FORMAT not in ("float", "int16"):
    raise SystemExit("VNGIS_DAY_FORMAT phải là float hoặc int16")
DAY_SCALE_INV = _env("VNGIS_DAY_SCALE", 10000, int)  # int16 = round(giá trị × 10000); 1000 cho file nhỏ hơn ~40%
if DAY_SCALE_INV not in (1000, 10000):
    raise SystemExit("VNGIS_DAY_SCALE phải là 10000 hoặc 1000")
DAY_NODATA = -32768

N_WORKERS = _env("VNGIS_WORKERS", 8, int)                         # số xã chạy song song
if MODE == "pilot":
    N_WORKERS = max(N_WORKERS, min(PILOT_N, 16))                  # thí điểm: mọi xã chạy cùng lúc
MONTH_THREADS = _env("VNGIS_MONTH_THREADS", 12, int)              # số ảnh tải song song trong 1 xã
EE_CONCURRENCY = _env("VNGIS_EE_CONCURRENCY", 24, int)            # tổng số lệnh gọi EE cùng lúc
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

class AdaptiveLimiter:
    """Giới hạn số lệnh gọi Earth Engine cùng lúc, tự hạ khi bị 429 (vượt hạn mức) và tăng dần lại khi ổn."""

    def __init__(self, maximum):
        self.max = max(1, maximum)
        self.limit = self.max
        self.active = 0
        self.ok_streak = 0
        self.cv = threading.Condition()

    def __enter__(self):
        with self.cv:
            while self.active >= self.limit:
                self.cv.wait(1)
            self.active += 1
        return self

    def __exit__(self, *exc):
        with self.cv:
            self.active -= 1
            self.cv.notify_all()
        return False

    def on_rate_limited(self):
        with self.cv:
            new = max(1, int(self.limit * 0.7))
            if new < self.limit:
                log.warning(f"Earth Engine báo vượt hạn mức (429): hạ số lệnh gọi cùng lúc {self.limit} -> {new}")
            self.limit, self.ok_streak = new, 0

    def on_success(self):
        with self.cv:
            self.ok_streak += 1
            if self.ok_streak >= 50 and self.limit < self.max:
                self.limit += 1
                self.ok_streak = 0
                self.cv.notify_all()


EE_SEM = AdaptiveLimiter(EE_CONCURRENCY)
DL_SEM = EE_SEM
RATE_LIMIT_MAX_WAIT_SEC = _env("VNGIS_RATE_LIMIT_MAX_WAIT_SEC", 900, int)   # chờ tối đa 15 phút cho mỗi lệnh bị 429
_RATE_ERR = ("429", "too many requests", "concurrency limit", "rate limit", "quota exceeded",
             "resource_exhausted", "resource exhausted")


def is_rate_limited(msg):
    low = str(msg).lower()
    return any(k in low for k in _RATE_ERR)


def rate_limit_sleep(waited, attempt):
    """Chờ tăng dần (5s, 10s, 20s ... tối đa 120s, có ngẫu nhiên). Trả tổng thời gian đã chờ."""
    import random
    EE_SEM.on_rate_limited()
    d = min(120, 5 * (2 ** min(attempt, 5))) * random.uniform(0.7, 1.3)
    deadline = time.time() + d
    while time.time() < deadline:
        check_stop()
        time.sleep(1)
    return waited + d

DRIVE_FOLDER = _env("VNGIS_DRIVE_FOLDER", "VNGISDash_2024_PILOT" if MODE == "pilot" else "VNGISDash_2024")
RCLONE_REMOTE = _env("VNGIS_RCLONE_REMOTE", "gdrive")
REMOTE_BASE = f"{RCLONE_REMOTE}:{DRIVE_FOLDER}"
LOCAL_ROOT = _env("VNGIS_LOCAL_ROOT", os.path.expanduser(f"~/vngis_2024/{DRIVE_FOLDER}"))
CACHE_DIR = _env("VNGIS_CACHE_DIR", os.path.expanduser("~/vngis_2024/_cache"))
UPLOAD_EVERY_SEC = _env("VNGIS_UPLOAD_EVERY_SEC", 60, int)
MIN_FREE_GB = _env("VNGIS_MIN_FREE_GB", 4.0, float)          # ổ máy chạy còn ít hơn mức này: tạm dừng tải, đẩy lên Drive trước
RCLONE_TRANSFERS = _env("VNGIS_RCLONE_TRANSFERS", 16, int)
UPLOAD_NOW = threading.Event()                                # yêu cầu uploader đồng bộ ngay
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
DAY_COLUMNS = ADM_COLS + T1_FEATURES + ["YEAR", "MONTH"]
NIGHT_COLUMNS = ["GID_1", "NAME_1", "GID_3", "NAME_3", "YEAR", "MONTH", "TIME", "COMMUNE_AREA_HA", "TNL",
                 "MEAN_RAD", "STD_RAD", "MIN_RAD", "MAX_RAD", "SPATIAL_CV", "LIT_PIXELS", "LIT_AREA_HA",
                 "ELECTRIFICATION_RATIO_PCT", "LIT_POP_PROXY", "CLOUD_FREE_OBS", "TNL_MA3",
                 "TNL_MOM_GROWTH_PCT"]                                               # notebook cell 38

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
  """getInfo có xử lý 429: chờ rồi thử lại, không tính là lỗi của xã."""
  waited, attempt = 0.0, 0
  while True:
    check_stop()
    try:
      with EE_SEM:
        out = obj.getInfo()
      EE_SEM.on_success()
      return out
    except ee.EEException as exc:
      if not is_rate_limited(exc) or waited > RATE_LIMIT_MAX_WAIT_SEC:
        raise
      waited = rate_limit_sleep(waited, attempt)
      attempt += 1


# ---------- Kế hoạch tải: 1 lần gọi cho cả 12 tháng ----------
def fetch_plan(commune_fc, commune_geom):
  """Số cảnh ảnh của từng cửa sổ (Task 2) và từng bộ VIIRS (Task 3.1) cho 12 tháng, cùng số xã khớp trong asset.
  Python dùng các con số này để chọn nhánh y hệt các câu lệnh if trong notebook cell 18 và 34."""
  months = []
  for m in MONTHS:
    s2 = [ee.ImageCollection(S2_COLLECTION).filterBounds(commune_geom).filterDate(s, e).size()
          for s, e in _s2_windows(YEAR, m)]
    s_date, e_date = _month_dates(YEAR, m)
    va = ee.ImageCollection(VIIRS_A).filterBounds(commune_geom).filterDate(s_date, e_date).size()
    vb = ee.ImageCollection(VIIRS_B).filterBounds(commune_geom).filterDate(s_date, e_date).size()
    months.append(ee.List(s2 + [va, vb]))
  out = ee_getinfo(ee.Dictionary({"n_fc": commune_fc.size(), "months": ee.List(months)}))
  plan = {}
  for m, row in zip(MONTHS, out["months"]):
    c0, c1, c2, va, vb = row
    win = 0 if c0 > 0 else (1 if c1 > 0 else (2 if c2 > 0 else None))
    viirs = VIIRS_A if va > 0 else (VIIRS_B if vb > 0 else None)
    plan[m] = {"s2_window": win, "viirs": viirs}
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
def task1_all_months(commune_fc):
  """Mỗi tháng: lọc CLOUDY_PIXEL_PERCENTAGE < 85, nếu rỗng thì dùng toàn bộ cảnh; median; add_indices;
  reduceRegions(mean + stdDev, scale 50, tileScale 4, EPSG:4326). Tháng không có cảnh nào: bỏ (như notebook).
  Nhánh if/else của notebook chuyển thành ee.Algorithms.If phía máy chủ, cùng điều kiện, cùng kết quả."""
  bands = DAY_BANDS_ALL
  reducers = ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True)
  selected_cols = ["GID_3"] + T1_FEATURES
  per_month = []
  for m in MONTHS:
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
    attempt, waited, rl = 0, 0.0, 0
    while attempt < max_retry:
        check_stop()
        try:
            with EE_SEM:
                url = img.getDownloadURL({"region": region, "scale": scale, "crs": "EPSG:4326",
                                          "format": "GEO_TIFF", "filePerBand": False})
        except ee.EEException as exc:
            if is_rate_limited(exc) and waited <= RATE_LIMIT_MAX_WAIT_SEC:
                waited = rate_limit_sleep(waited, rl)
                rl += 1
                last = f"429: {exc}"
                continue
            kind = _classify(str(exc))
            if kind == "too_large":
                raise TooLargeError(str(exc))
            if kind == "permanent":
                raise PermanentError(f"getDownloadURL bị từ chối: {exc}")
            last = f"getDownloadURL: {exc}"
            time.sleep(5 * (attempt + 1))
            attempt += 1
            continue
        try:
            with DL_SEM:
                r = _http_get(url)
        except requests.RequestException as exc:
            last = f"mạng: {exc}"
            time.sleep(5 * (attempt + 1))
            attempt += 1
            continue
        if r.status_code >= 400:
            body = (r.text or "")[:400].replace("\n", " ")
            msg = f"HTTP {r.status_code}: {body}"
            if (r.status_code == 429 or is_rate_limited(body)) and waited <= RATE_LIMIT_MAX_WAIT_SEC:
                waited = rate_limit_sleep(waited, rl)
                rl += 1
                last = msg
                continue
            kind = _classify(msg) if r.status_code not in (401, 403) else "permanent"
            if kind == "too_large":
                raise TooLargeError(msg)
            if kind == "permanent":
                raise PermanentError(msg)
            last = msg
            time.sleep(5 * (attempt + 1))
            attempt += 1
            continue
        data = r.content
        if data[:2] == b"PK":                       # đôi khi EE trả về file zip (notebook cell 21)
            z = zipfile.ZipFile(io.BytesIO(data))
            data = z.read([n for n in z.namelist() if n.lower().endswith(".tif")][0])
        if len(data) < 200:
            last = f"file tải về chỉ {len(data)} byte"
            time.sleep(5 * (attempt + 1))
            attempt += 1
            continue
        EE_SEM.on_success()
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
    coords = region.bounds(maxError=1).getInfo()["coordinates"][0]
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
# 7. XỬ LÝ MỘT XÃ
# =====================================================================================
class NotInAsset(RuntimeError):
    pass


ADMIN_DF = None      # bảng hành chính GADM (vai trò gdf_cleaned trong notebook)
ADMIN_BY_GID = {}
_parts_lock = threading.Lock()
PARTS_STAMP = None


def append_parts(kind, records):
    """Ghi chỉ số của 1 xã vào file .jsonl của lượt này; CSV toàn quốc được dựng lại từ các file này."""
    with _parts_lock:
        os.makedirs(L(D_PARTS), exist_ok=True)
        with open(L(D_PARTS, f"{kind}_{PARTS_STAMP}.jsonl"), "a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False, default=lambda o: None) + "\n")


def _clean(v):
    if isinstance(v, (np.floating, float)) and (v != v):
        return None
    if isinstance(v, np.generic):
        return v.item()
    return v


def free_gb():
    try:
        return shutil.disk_usage(LOCAL_ROOT).free / 1e9
    except OSError:
        return 1e9


def wait_for_disk():
    """Hãm tốc độ: ổ máy chạy sắp đầy thì không tải thêm, yêu cầu đẩy ảnh lên Drive và chờ có chỗ."""
    warned = False
    while free_gb() < MIN_FREE_GB:
        check_stop()
        UPLOAD_NOW.set()
        if not warned:
            log.warning(f"Ổ máy chạy còn {free_gb():.1f} GB (< {MIN_FREE_GB} GB): tạm dừng tải, chờ đẩy ảnh lên Drive.")
            warned = True
        time.sleep(5)


def _download_month(kind, ctx, m, img, path):
    wait_for_disk()
    label = f"[{ctx['gid3']}] {kind} {YEAR}-{m:02d}"
    if kind == "day":
        n = download_tif(img, img_region(ctx), 20, path, label,
                         writer=write_day_int16 if DAY_FORMAT == "int16" else write_tif)
        ok, empty, note = inspect_tif(path, DAY_BANDS)
    else:
        n = download_tif(img, img_region(ctx), 500, path, label, writer=write_tif)
        ok, empty, note = inspect_tif(path, 2)
    if not ok:
        raise RuntimeError(f"file kiểm tra lỗi: {note}")
    return n, empty


def img_region(ctx):
    return ctx["geom"]


def process_commune(row, prev):
    check_stop()
    t_start = time.time()
    ctx = build_ctx(row)
    gid3 = ctx["gid3"]
    prev = prev or {}
    info = {"gid_3": gid3, "gid_1": ctx["gid1"], "run_id": RUN_ID,
            "t1": prev.get("t1", "pending"), "t3csv": prev.get("t3csv", "pending"),
            "t2": dict(prev.get("t2") or {}), "t3img": dict(prev.get("t3img") or {}),
            "empty_months_t2": list(prev.get("empty_months_t2") or []),
            "tiles_used": dict(prev.get("tiles_used") or {}), "day_bands": DAY_BANDS, "errors": []}

    commune_fc = communes_fc.filter(ee.Filter.eq("GID_3", gid3))
    commune_geom = commune_fc.geometry()
    ctx["geom"] = commune_geom

    # 3 lệnh gọi chạy song song: kế hoạch tải, Task 1 (12 tháng), Task 3.2 (12 tháng)
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix=f"{gid3}-q") as qp:
        f_plan = qp.submit(fetch_plan, commune_fc, commune_geom)
        f_t1 = qp.submit(task1_all_months, commune_fc) if info["t1"] != "ok" else None
        f_t3 = (qp.submit(task3_all_months, commune_geom, ctx["gid1"], ctx["name1"], gid3, ctx["cname_full"])
                if info["t3csv"] != "ok" else None)
        n_fc, plan = f_plan.result()
        if n_fc == 0:
            for f in (f_t1, f_t3):
                if f:
                    f.cancel()
            raise NotInAsset(f"GID_3 {gid3} không có trong asset {ASSET_ID}")

        # ---------- Task 2 + Task 3.1: tải song song tối đa MONTH_THREADS ảnh ----------
        jobs = []
        for m in MONTHS:
            key = f"{m:02d}"
            if info["t2"].get(key) not in ("ok", "none"):
                w = plan[m]["s2_window"]
                if w is None:
                    info["t2"][key] = "none"           # notebook: "Bỏ qua tháng: không tìm thấy ảnh"
                else:
                    jobs.append(("day", m, day_image(m, w, commune_geom).select(DAY_BANDS_ALL[:DAY_BANDS]),
                                 L(ctx["rel_day_dir"], day_name(ctx, m))))
            if info["t3img"].get(key) not in ("ok", "none"):
                c = plan[m]["viirs"]
                if c is None:
                    info["t3img"][key] = "none"
                else:
                    jobs.append(("night", m, night_image(m, c, commune_geom),
                                 L(ctx["rel_night_dir"], night_name(ctx, m))))
        with ThreadPoolExecutor(max_workers=max(1, MONTH_THREADS), thread_name_prefix=f"{gid3}-m") as mp:
            futs = {mp.submit(_download_month, k, ctx, m, img, p): (k, m) for k, m, img, p in jobs}
            for fut in as_completed(futs):
                k, m = futs[fut]
                key = f"{m:02d}"
                slot = info["t2"] if k == "day" else info["t3img"]
                try:
                    n, empty = fut.result()
                    slot[key] = "ok"
                    if k == "day" and empty and key not in info["empty_months_t2"]:
                        info["empty_months_t2"].append(key)
                    if n > 1:
                        info["tiles_used"][f"{k}_{key}"] = n
                except StopRequested:
                    raise
                except PermanentError:
                    raise
                except Exception as exc:
                    slot[key] = "fail"
                    info["errors"].append(f"{k} {key}: {type(exc).__name__}: {str(exc)[:200]}")

        # ---------- Task 1 ----------
        if f_t1 is not None:
            try:
                props = f_t1.result()
                if not props:
                    raise RuntimeError("Task 1: không tháng nào có cảnh Sentinel-2")
                adm = ADMIN_BY_GID.get(gid3)
                if adm is None:
                    raise RuntimeError("Task 1: GID_3 không có trong bảng GADM")
                recs = []
                for p in sorted(props, key=lambda p: p["MONTH"]):
                    r = {k: adm[k] for k in ADM_COLS}           # = merge(lookup, df_s2, on="GID_3") của notebook
                    r.update({k: _clean(p.get(k)) for k in T1_FEATURES})
                    r.update({"YEAR": YEAR, "MONTH": int(p["MONTH"])})
                    recs.append(r)
                append_parts("day", recs)
                info["t1"] = "ok"
                info["t1_months"] = len(recs)
            except StopRequested:
                raise
            except Exception as exc:
                info["t1"] = "fail"
                info["errors"].append(f"T1: {type(exc).__name__}: {str(exc)[:200]}")

        # ---------- Task 3.2 ----------
        if f_t3 is not None:
            try:
                df_ntl = f_t3.result()
                append_parts("night", [{k: _clean(v) for k, v in r.items()} for r in df_ntl.to_dict("records")])
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
# 7b. CSV TOÀN QUỐC (dựng lại từ _control/parts sau mỗi lượt)
# =====================================================================================
def _read_parts(kind):
    rows = []
    for p in sorted(glob.glob(L(D_PARTS, f"{kind}_*.jsonl"))):
        with open(p, encoding="utf-8") as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    return rows


def build_national_csv():
    os.makedirs(L(D_CSV), exist_ok=True)
    for kind, rel, cols in (("day", DAY_CSV, DAY_COLUMNS), ("night", NIGHT_CSV, NIGHT_COLUMNS)):
        rows = _read_parts(kind)
        if not rows:
            continue
        df = pd.DataFrame(rows).reindex(columns=cols)
        df = df.drop_duplicates(subset=["GID_3", "MONTH"], keep="last")
        for c in ("YEAR", "MONTH", "LIT_PIXELS"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        df["_k"] = df["GID_3"].map(lambda g: tuple(natural_sort_key(g)))
        df = df.sort_values(["_k", "MONTH"]).drop(columns="_k")
        path = L(rel)
        df.to_csv(path + ".part", index=False, encoding="utf-8-sig")
        os.replace(path + ".part", path)
        log.info(f"{rel}: {len(df):,} dòng, {df['GID_3'].nunique():,} xã")


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


def reconcile_with_drive(statuses, rows):
    """Xã ghi 'ok' cho một tháng mà ảnh không có trên Drive (ví dụ lượt trước chết khi ảnh chưa kịp đẩy lên):
    đặt lại tháng đó để làm lại. Liệt kê tên file trên Drive một lần cho cả Day và Night."""
    names = set()
    for d in (D_DAY, D_NIGHT):
        res = subprocess.run(["rclone", "lsf", "-R", "--files-only", "--fast-list", "--include", "*.tif",
                              f"{REMOTE_BASE}/{d}"], capture_output=True, text=True, timeout=3600)
        if res.returncode != 0 and "directory not found" not in res.stderr:
            raise RuntimeError(f"Không liệt kê được {d} trên Drive: {res.stderr.strip()[-300:]}")
        names.update(os.path.basename(x.strip()) for x in res.stdout.splitlines() if x.strip())
    fixed = 0
    for gid, st in list(statuses.items()):
        if gid not in rows or not st:
            continue
        ctx = build_ctx(rows[gid])
        missing = []
        for key_slot, namer, tag in (("t2", day_name, "day"), ("t3img", night_name, "night")):
            slot = dict(st.get(key_slot) or {})
            for key, v in slot.items():
                if v == "ok" and namer(ctx, int(key)) not in names and not os.path.isfile(
                        L(ctx["rel_day_dir" if tag == "day" else "rel_night_dir"], namer(ctx, int(key)))):
                    slot[key] = "missing"
                    missing.append(f"{tag} {key}")
            st[key_slot] = slot
        if missing:
            st["status"] = "partial"
            st["attempts"] = min(int(st.get("attempts", 0)), MAX_ATTEMPTS - 1)
            st["errors"] = [f"Thiếu trên Drive, làm lại: {', '.join(missing)}"]
            st["run_id"] = RUN_ID
            write_status(st)
            fixed += 1
    log.info(f"Đối chiếu Drive: {len(names):,} ảnh có trên Drive; {fixed:,} xã thiếu ảnh được đặt làm lại.")
    return fixed


def core_finished(st):
    if not st:
        return False
    if st.get("status") in ("done", "not_in_asset"):
        return True
    # Lỗi do vượt hạn mức Earth Engine (429) là lỗi tạm thời: không tính vào số lần thử, luôn làm lại
    if is_rate_limited(" ".join(st.get("errors") or [])):
        return False
    return int(st.get("attempts", 0)) >= MAX_ATTEMPTS



# =====================================================================================
# 9. RCLONE
# =====================================================================================
RCLONE_COMMON = ["--transfers", str(RCLONE_TRANSFERS), "--checkers", "16", "--tpslimit", "12",
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
        age = [] if final else ["--min-age", "30s"]
        for d in (D_DAY, D_NIGHT):
            src = L(d)
            if os.path.isdir(src):
                _rclone(["move", src, f"{REMOTE_BASE}/{d}", "--filter", "- *.part", "--filter", "+ *.tif",
                         "--filter", "- *", *age, *RCLONE_COMMON])
        for d in (D_CSV, D_CONTROL):
            src = L(d)
            if os.path.isdir(src):
                _rclone(["copy", src, f"{REMOTE_BASE}/{d}", "--filter", "- *.part", *RCLONE_COMMON])


class Uploader(threading.Thread):
    def __init__(self):
        super().__init__(name="uploader", daemon=True)
        self.stop_event = threading.Event()

    def run(self):
        last_stop_check = 0
        while not self.stop_event.wait(5):
            now = time.time()
            if now - last_stop_check >= DRIVE_STOP_POLL_SEC:
                last_stop_check = now
                if drive_stop_exists():
                    request_stop("drive_stop")
            if UPLOAD_NOW.is_set() or now - getattr(self, "_last_sync", 0) >= UPLOAD_EVERY_SEC:
                UPLOAD_NOW.clear()
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
    for d in (LOCAL_ROOT, L(D_STATUS), L(D_PARTS), L(D_LOGS), CACHE_DIR):
        os.makedirs(d, exist_ok=True)
    if shutil.which("rclone") is None:
        raise RuntimeError("Chưa cài rclone.")
    if not _rclone(["mkdir", f"{REMOTE_BASE}/{D_STATUS}"], timeout=180):
        raise RuntimeError(f"rclone không ghi được vào '{REMOTE_BASE}'. Kiểm tra secret RCLONE_CONF.")
    for d in (D_STATUS, D_PARTS):        # vài file nhỏ: trạng thái và chỉ số của các lượt trước
        res = subprocess.run(["rclone", "copy", f"{REMOTE_BASE}/{d}", L(d), "--update"],
                             capture_output=True, text=True, timeout=3600)
        if res.returncode != 0 and "directory not found" not in res.stderr:
            raise RuntimeError(f"Không kéo được {d} từ Drive: {res.stderr.strip()[-300:]}")
    # Cảnh báo nếu đích còn cấu trúc của bản pipeline cũ
    old = subprocess.run(["rclone", "lsf", REMOTE_BASE, "--dirs-only"], capture_output=True, text=True, timeout=120)
    if any(x.strip("/") in ("03_Provinces", "04_Status", "1_Task1_Spectral_Indices", "2_Task2_Day_S2")
           for x in old.stdout.splitlines()):
        log.warning(f"Thư mục '{DRIVE_FOLDER}' trên Drive còn dữ liệu của bản pipeline cũ. "
                    f"Nên xóa thư mục cũ rồi chạy lại để không lẫn dữ liệu.")


# =====================================================================================
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
        return pd.read_csv(idx_csv, dtype=str, keep_default_na=False)
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
    tbl = read_dbf(os.path.join(CACHE_DIR, "gadm41_VNM_3.dbf"), enc)[ADM_COLS].drop_duplicates("GID_3")
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
# =====================================================================================
# 11. PREFLIGHT
# =====================================================================================
class PreflightError(RuntimeError):
    pass


PREFLIGHT_HINT = (
    "Gợi ý: (1) HTTP 401/403 hoặc 'permission': cấp role 'Earth Engine Resource Writer' (roles/earthengine.writer) "
    "và 'Service Usage Consumer' (roles/serviceusage.serviceUsageConsumer) cho service account, đăng ký project "
    "với Earth Engine; (2) thử VNGIS_EE_HIGH_VOLUME=true nếu standard bị chặn; (3) lỗi rclone: kiểm tra RCLONE_CONF.")


def _probe_drive():
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
    return "Drive OK"


def _probe_ee(row):
    gid3 = row["GID_3"]
    fc = communes_fc.filter(ee.Filter.eq("GID_3", gid3))
    geom = fc.geometry()
    n_fc, plan = fetch_plan(fc, geom)
    if n_fc == 0:
        raise PreflightError(f"Asset không có xã {gid3}. Kiểm tra GID_3 trong asset có khớp GADM 4.1 không.")
    region = geom.centroid(maxError=1).buffer(1500)
    m_day = next((m for m in MONTHS if plan[m]["s2_window"] is not None), None)
    m_night = next((m for m in MONTHS if plan[m]["viirs"] is not None), None)
    if m_day is None or m_night is None:
        raise PreflightError(f"Xã thử {gid3} không có ảnh Sentinel-2 hoặc VIIRS nào trong năm {YEAR}.")
    img = day_image(m_day, plan[m_day]["s2_window"], geom).select(DAY_BANDS_ALL[:DAY_BANDS]).clip(region)
    try:
        whole = fetch_geotiff_bytes(img, region, 20)
        night = fetch_geotiff_bytes(night_image(m_night, plan[m_night]["viirs"], geom).clip(region), region, 500)
    except Exception as exc:
        raise PreflightError(f"Tải ảnh thử thất bại: {exc}")
    import rasterio
    with rasterio.MemoryFile(whole) as mf, mf.open() as src:
        if src.count != DAY_BANDS:
            raise PreflightError(f"Ảnh ngày có {src.count} kênh, cần {DAY_BANDS}.")
        a_whole, tr_whole = src.read(), src.transform
    msg = f"ảnh ngày {len(whole)/1024:.0f} KB, ảnh đêm {len(night)/1024:.0f} KB"
    if PREFLIGHT_TILE_TEST:
        try:
            parts = [fetch_geotiff_bytes(img, rect, 20) for rect in _split_bbox(_bbox(region), 2)]
            a_tiled, tr_tiled, *_ = mosaic_tiles(parts)
            compare_on_grid(a_whole, tr_whole, a_tiled, tr_tiled)
            msg += "; chia ô trùng khớp từng pixel"
        except Exception as exc:
            TILING_OK[0] = False
            log.error(f"[preflight] Kiểm tra chia ô KHÔNG ĐẠT ({exc}). Xã cần chia ô sẽ báo lỗi thay vì ghép sai.")
    else:
        msg += "; bỏ qua kiểm tra chia ô (chế độ thí điểm)"
    return msg


def preflight(row):
    """Kiểm tra Earth Engine và Drive song song."""
    log.info(f"[preflight] Kiểm tra Earth Engine (xã {row['GID_3']}) và Google Drive...")
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_ee, f_dr = ex.submit(_probe_ee, row), ex.submit(_probe_drive)
        msg_ee, msg_dr = f_ee.result(), f_dr.result()
    log.info(f"[preflight] ĐẠT: {msg_ee}; {msg_dr}.")


# =====================================================================================
# 12. TIẾN ĐỘ
# =====================================================================================
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
    global STATUS_FILE, ADMIN_DF, ADMIN_BY_GID, PARTS_STAMP
    os.makedirs(L(D_STATUS), exist_ok=True)
    PARTS_STAMP = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{RUN_ID}"
    STATUS_FILE = L(D_STATUS, f"status_{PARTS_STAMP}.jsonl")
    setup_logging()
    log.info(f"VNGISDash {YEAR} | chế độ {MODE} | đích {REMOTE_BASE} | {N_WORKERS} xã x {MONTH_THREADS} ảnh song song"
             f" | ảnh ngày {DAY_BANDS} kênh {DAY_FORMAT}")
    try:
        # Khởi tạo Earth Engine song song với việc kéo trạng thái từ Drive
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_ee = ex.submit(init_earth_engine)
            init_storage()
            ADMIN_DF = build_admin_table()
            f_ee.result()
        ADMIN_BY_GID = {r["GID_3"]: r for r in ADMIN_DF.to_dict("records")}
        targets = load_targets(ADMIN_DF)
    except Exception as exc:
        log.error(f"Khởi tạo thất bại: {type(exc).__name__}: {exc}")
        log.error(PREFLIGHT_HINT)
        return 1
    rows = {r["GID_3"]: r for r in targets.to_dict("records")}
    gids = list(rows)
    log.info(f"Danh sách: {len(gids):,} xã, {targets['GID_1'].nunique()} tỉnh"
             + (f" (thí điểm: {', '.join(gids)})" if MODE == "pilot" else ""))

    statuses = load_all_status()
    try:
        if statuses:
            reconcile_with_drive(statuses, rows)
            statuses = load_all_status()
    except Exception as exc:
        log.error(f"Đối chiếu ảnh trên Drive thất bại: {exc}. Dừng để không bỏ sót xã thiếu ảnh.")
        return 1
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
    try:
        build_national_csv()
    except Exception as exc:
        log.error(f"Dựng CSV toàn quốc lỗi: {exc}")
    rclone_sync_once(final=True)

    if not unfinished and not STOP_EVENT.is_set():
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
    sys.exit(main())
