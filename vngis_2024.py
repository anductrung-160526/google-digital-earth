# -*- coding: utf-8 -*-
# Sinh tự động từ V12_6_batch_2024_all_communes.ipynb. Chạy: python vngis_2024.py

import importlib, importlib.util, subprocess, sys

_REQUIRED = {  # tên module: tên gói pip
    "ee": "earthengine-api", "geopandas": "geopandas", "rasterio": "rasterio",
    "pyproj": "pyproj", "shapely": "shapely", "requests": "requests",
    "cv2": "opencv-python-headless", "skimage": "scikit-image",
    "pandas": "pandas", "numpy": "numpy",
}
_missing = [pkg for mod, pkg in _REQUIRED.items() if importlib.util.find_spec(mod) is None]
if _missing:
    print("[*] Cài thêm:", " ".join(_missing))
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *_missing])
print("[+] Thư viện sẵn sàng.")

import os, io, re, json, time, math, shutil, zipfile, logging, threading, unicodedata, warnings, glob
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")


def _env(name, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    if cast is bool:
        return raw.strip().lower() in ("1", "true", "yes", "y")
    return cast(raw)


try:
    import google.colab  # noqa: F401
    IN_COLAB = True
except ImportError:
    IN_COLAB = False

# ---------------- Khung thời gian ----------------
YEAR = 2024
START_YEAR, START_MONTH = YEAR, 1
END_YEAR, END_MONTH = YEAR, 12
TIME_TAG = str(YEAR)                     # V12.6 dùng "<năm đầu>-<năm cuối>"; một năm thì ghi "2024"

# ---------------- Tham số trích xuất (giữ nguyên V12.6) ----------------
PROJECT_ID = "digital-vietnam-earth"
ASSET_ID   = f"projects/{PROJECT_ID}/assets/communes_l3"
SCALE      = 30                          # Landsat 30 m/pixel
SCALE_FALLBACK = 60                      # hạ xuống 60 m nếu ảnh 30 m vượt hạn mức tải
MAX_CLOUD  = 70                          # % mây tối đa của 1 scene
DAY_SOURCE_POLICY = "YEAR_BLOCKS_L5_L7_L8"
FETCH_OSM_REFERENCE = _env("VNGIS_FETCH_OSM", True, bool)   # False nếu Overpass chặn IP máy chạy
OVERWRITE  = False
KEEP_EMPTY_MONTHS = False
VIIRS_FIRST_DATE    = (2012, 4)
DMSP_LAST_YEAR      = 2012
VIIRS_LIT_THRESHOLD = 1.5
DMSP_LIT_THRESHOLD  = 5
VIIRS_SCALE         = 500
DMSP_SCALE          = 927
BAND_ORDER = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2",
              "NDVI", "NDBI", "MNDWI", "BSI"]

# ---------------- Chạy hàng loạt ----------------
N_WORKERS        = _env("VNGIS_WORKERS", 8, int)       # số xã xử lý song song
RUN_ID           = _env("VNGIS_RUN_ID", "local")       # GitHub Actions truyền github.run_id vào đây
MAX_RUNTIME_SEC  = _env("VNGIS_MAX_RUNTIME_SEC", 0, int)  # >0: hết ngân sách giờ thì dừng có trật tự (mã thoát 3)
EE_KEY_FILE      = _env("VNGIS_EE_KEY_FILE", "")       # đường dẫn khóa JSON của service account (chạy không cần người)
MAX_ATTEMPTS     = _env("VNGIS_MAX_ATTEMPTS", 3, int)  # số lần thử tối đa cho mỗi xã
TEST_LIMIT       = _env("VNGIS_TEST_LIMIT", 0, int) or None   # None = chạy tất cả
PROVINCE_FILTER  = [g for g in _env("VNGIS_PROVINCES", "").split(",") if g]  # ví dụ "VNM.4_1,VNM.27_1"
OSM_MIN_INTERVAL_SEC = 2.0               # giãn cách giữa hai truy vấn Overpass (dùng chung mọi luồng)
EE_HIGH_VOLUME   = True                  # endpoint dành cho nhiều request song song
EE_DEADLINE_SEC  = 300

# ---------------- Lưu trữ ----------------
DRIVE_FOLDER_NAME = _env("VNGIS_DRIVE_FOLDER", "VNGISDash_Communes_2024")
STORAGE_MODE  = _env("VNGIS_STORAGE_MODE", "colab" if IN_COLAB else "rclone")  # colab | rclone | path
RCLONE_REMOTE = _env("VNGIS_RCLONE_REMOTE", "gdrive")   # tên remote đã tạo bằng `rclone config`
SYNC_PATH     = _env("VNGIS_SYNC_PATH", "")             # chế độ path: thư mục đích có sẵn trên máy
LOCAL_ROOT    = _env("VNGIS_LOCAL_ROOT",
                     "/content/vngis_2024" if IN_COLAB else os.path.expanduser("~/vngis_2024"))
UPLOAD_EVERY_SEC    = _env("VNGIS_UPLOAD_EVERY_SEC", 600, int)  # chế độ rclone: đẩy lên Drive mỗi 10 phút
UPLOAD_SIDECAR_JSON = _env("VNGIS_UPLOAD_JSON", False, bool)    # .json chỉ phục vụ cache, mặc định không đẩy
KEEP_LOCAL_TIF      = _env("VNGIS_KEEP_LOCAL_TIF", False, bool) # False = xóa TIF trên máy sau khi đã lên Drive

# ---------------- Cấu trúc thư mục trên Drive ----------------
SUB_README   = "00_README.txt"
SUB_INDEX    = "01_Index"       # danh mục xã + trạng thái
SUB_COMBINED = "02_Combined"    # CSV gộp toàn quốc
SUB_PROV     = "03_Provinces"   # dữ liệu từng xã, nhóm theo tỉnh
SUB_STATUS   = "04_Status"      # trạng thái từng xã, phục vụ chạy tiếp
SUB_LOGS     = "05_Logs"

if STORAGE_MODE == "colab":
    DEST_BASE = f"/content/drive/MyDrive/{DRIVE_FOLDER_NAME}"
elif STORAGE_MODE == "path":
    if not SYNC_PATH:
        raise ValueError("Chế độ path cần VNGIS_SYNC_PATH.")
    DEST_BASE = os.path.join(SYNC_PATH, DRIVE_FOLDER_NAME)
elif STORAGE_MODE == "rclone":
    DEST_BASE = None                     # đích là rclone remote, xem ô đồng bộ
else:
    raise ValueError(f"STORAGE_MODE không hợp lệ: {STORAGE_MODE}")

# META_ROOT chứa index, trạng thái, log, CSV gộp. Ở Colab/path nằm thẳng trên đích
# để phiên mới đọc lại được; ở máy ảo nằm trên ổ cục bộ rồi được rclone đẩy lên.
META_ROOT = DEST_BASE if DEST_BASE else LOCAL_ROOT
WORK_PROV_ROOT = os.path.join(LOCAL_ROOT, SUB_PROV)   # nơi tải và xử lý ảnh
# Trạng thái ghi theo "lượt chạy": mỗi lượt một file .jsonl, nối thêm một dòng cho mỗi xã xong.
# Một lượt = một lần chạy script (một phiên Colab, một lượt GitHub Actions...). Nhờ vậy 11 nghìn xã
# chỉ cần kéo về vài file nhỏ, không phải 11 nghìn file riêng lẻ từ Drive.
LEASE_DIR = os.path.join(META_ROOT, SUB_STATUS, "leases")
LEASE_FILE = os.path.join(LEASE_DIR, f"lease_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{RUN_ID}.jsonl")
# Bộ nhớ đệm OSM: ở chế độ rclone chỉ giữ trên máy (xã đã xong không cần lại).
OSM_CACHE_DIR = (os.path.join(LOCAL_ROOT, "osm_cache") if STORAGE_MODE == "rclone"
                 else os.path.join(META_ROOT, SUB_STATUS, "osm_cache"))


def _in_notebook():
    return "ipykernel" in sys.modules
LOG_DIR = os.path.join(META_ROOT, SUB_LOGS)

DATA_DIR    = os.path.join(LOCAL_ROOT, "data_spatial")
ZIP_PATH    = os.path.join(DATA_DIR, "gadm41_VNM_shp.zip")
EXTRACT_DIR = os.path.join(DATA_DIR, "gadm41_VNM")
SHP_L3_PATH = os.path.join(EXTRACT_DIR, "gadm41_VNM_3.shp")
INDEX_CSV   = os.path.join(DATA_DIR, "commune_index.csv")
GADM_VNM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_VNM_shp.zip"
ADM_COLS = ["GID_1", "NAME_1", "GID_2", "NAME_2", "GID_3", "NAME_3", "TYPE_3"]

print(f"[+] Môi trường: {'Colab' if IN_COLAB else 'máy chủ'} | lưu trữ: {STORAGE_MODE} | "
      f"luồng: {N_WORKERS} | giới hạn thử: {TEST_LIMIT}")

def slugify_vn(text):
    """Bỏ dấu tiếng Việt và viết hoa đầu mỗi chữ (giữ nguyên V12.6)."""
    if text is None:
        return "NA"
    text = str(text).replace("Đ", "D").replace("đ", "d")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    parts = re.findall(r"[A-Za-z0-9]+", text)
    return "".join(p.capitalize() for p in parts) or "NA"


def province_folder(commune):
    return f"{commune['GID_1']}_{slugify_vn(commune['NAME_1'])}"


def build_commune_paths(commune, prov_root=None, time_tag=TIME_TAG):
    """Như V12.6, thêm một cấp thư mục tỉnh để 11 nghìn xã không dồn vào một chỗ."""
    prov_root = prov_root or WORK_PROV_ROOT
    prov_code = commune["GID_1"]
    prov_name = slugify_vn(commune["NAME_1"])
    ward_name = slugify_vn(f"{commune.get('TYPE_3', '')} {commune['NAME_3']}".strip())
    gid_3 = commune["GID_3"]

    dir_name = f"{prov_code}_{ward_name}_{gid_3}_{time_tag}"
    prov_dir = province_folder(commune)
    rel_dir = os.path.join(SUB_PROV, prov_dir, dir_name)
    commune_dir = os.path.join(prov_root, prov_dir, dir_name)
    file_stem = f"{prov_name}_{ward_name}_{gid_3}"
    return {
        "dir_name": dir_name, "prov_dir": prov_dir, "rel_dir": rel_dir,
        "commune_dir": commune_dir,
        "img_dir": os.path.join(commune_dir, "Day", DAY_SOURCE_POLICY),
        "img_dir_dmsp": os.path.join(commune_dir, "Night", "DMSP"),
        "img_dir_viirs": os.path.join(commune_dir, "Night", "VIIRS"),
        "csv_dir": os.path.join(commune_dir, "CSV"),
        "csv_day": os.path.join(commune_dir, "CSV", f"{file_stem}_Day_Monthly_{DAY_SOURCE_POLICY}.csv"),
        "csv_dmsp": os.path.join(commune_dir, "CSV", f"{file_stem}_Night_DMSP_Annual.csv"),
        "csv_viirs": os.path.join(commune_dir, "CSV", f"{file_stem}_Night_VIIRS_Monthly.csv"),
        "csv_night": os.path.join(commune_dir, "CSV", f"{file_stem}_Night_All.csv"),
        "file_stem": file_stem,
    }


def make_image_name(file_stem, year, month, kind):
    return f"{file_stem}_{year}{month:02d}_{kind}.tif"


README_TEXT = f"""VNGISDash - Dữ liệu vệ tinh cấp xã Việt Nam, năm {YEAR}
Sinh bởi pipeline V12.6 chạy hàng loạt. Ranh giới: GADM 4.1 cấp xã (trước sắp xếp 01/7/2025).

00_README.txt   Tệp này.
01_Index/       commune_index_{YEAR}.csv: mọi xã, thư mục tương ứng và trạng thái xử lý.
02_Combined/    CSV gộp toàn quốc, mỗi dòng có gid_3 để lọc:
                  Day_Monthly_{DAY_SOURCE_POLICY}_{YEAR}_ALL.csv
                  Night_VIIRS_Monthly_{YEAR}_ALL.csv
                  Night_DMSP_Annual_{YEAR}_ALL.csv   (rỗng: DMSP chỉ có đến 2012)
                  Night_All_{YEAR}_ALL.csv
03_Provinces/   <GID_1>_<Tỉnh>/<GID_1>_<Xã>_<GID_3>_{YEAR}/
                  Day/{DAY_SOURCE_POLICY}/  ảnh ngày Landsat 8, 10 kênh, 30 m, theo tháng
                  Night/VIIRS/              ảnh đêm VIIRS, 2 kênh (avg_rad, cf_cvg), theo tháng
                  Night/DMSP/               trống với năm {YEAR}
                  CSV/                      4 CSV của xã, cùng định dạng V12.6
04_Status/      Trạng thái từng xã (dùng để chạy tiếp), progress.csv tổng hợp.
05_Logs/        Nhật ký chạy.

Tháng không có ảnh Landsat đạt ngưỡng mây {MAX_CLOUD}% sẽ không có dòng trong CSV ảnh ngày.
"""
print("[+] Ví dụ thư mục xã:",
      build_commune_paths({"GID_1": "VNM.27_1", "NAME_1": "Hà Nội", "TYPE_3": "Phường",
                           "NAME_3": "Thành Công", "GID_3": "VNM.27.3.1_1"})["rel_dir"])

log = logging.getLogger("vngis")


# ============ DỪNG CÓ TRẬT TỰ ============
class StopRequested(BaseException):
    """Báo hiệu dừng có trật tự (hết ngân sách giờ hoặc nhận tín hiệu).
    Kế thừa BaseException để các khối `except Exception` của xã không nuốt mất."""


STOP_EVENT = threading.Event()
STOP_REASON = [None]            # "deadline" hoặc "signal"


def request_stop(reason):
    if not STOP_EVENT.is_set():
        STOP_REASON[0] = reason
        STOP_EVENT.set()
        why = "hết ngân sách thời gian của lượt này" if reason == "deadline" else "nhận tín hiệu dừng"
        log.warning(f"Dừng có trật tự ({why}): hoàn tất bước đang chạy, đồng bộ rồi thoát.")


def _check_stop():
    if STOP_EVENT.is_set():
        raise StopRequested()


def install_signal_handlers():
    """Ctrl+C hoặc SIGTERM = dừng có trật tự; tín hiệu lần hai = thoát ngay."""
    import signal

    def handler(signum, _frame):
        if STOP_EVENT.is_set():
            os._exit(130)
        request_stop("signal")
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname).1s [%(threadName)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); log.addHandler(sh)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"run_{stamp}.log"), encoding="utf-8")
    fh.setFormatter(fmt); log.addHandler(fh)
    log.propagate = False


import ee
communes_fc = None


def init_earth_engine():
    global communes_fc
    kwargs = {"project": PROJECT_ID}
    if EE_HIGH_VOLUME:
        kwargs["opt_url"] = "https://earthengine-highvolume.googleapis.com"
    if EE_KEY_FILE:
        # Chạy tự động: xác thực bằng service account, không cần người bấm.
        with open(EE_KEY_FILE, encoding="utf-8") as f:
            email = json.load(f)["client_email"]
        ee.Initialize(credentials=ee.ServiceAccountCredentials(email, EE_KEY_FILE), **kwargs)
        log.info(f"Earth Engine sẵn sàng bằng service account {email}.")
    else:
        try:
            ee.Initialize(**kwargs)
        except Exception:
            if not (IN_COLAB or sys.stdin.isatty()):
                raise RuntimeError("Chưa xác thực Earth Engine. Chạy một lần trong terminal: "
                                   "python -c \"import ee; ee.Authenticate(auth_mode='notebook')\"")
            ee.Authenticate(auth_mode="notebook")
            ee.Initialize(**kwargs)
        log.info(f"Earth Engine sẵn sàng (high-volume={EE_HIGH_VOLUME}).")
    ee.data.setDeadline(EE_DEADLINE_SEC * 1000)
    communes_fc = ee.FeatureCollection(ASSET_ID)


def build_commune_index():
    """Bảng xã từ GADM 4.1, giữ nguyên cách làm của V12.6 (tải một lần, sau đó đọc cache)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(INDEX_CSV):
        return pd.read_csv(INDEX_CSV, dtype=str)
    import geopandas as gpd
    if not os.path.exists(SHP_L3_PATH):
        if not os.path.exists(ZIP_PATH):
            log.info("Tải ranh giới GADM 4.1 (~70 MB)...")
            r = requests.get(GADM_VNM_URL, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=600)
            r.raise_for_status()
            with open(ZIP_PATH + ".part", "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(ZIP_PATH + ".part", ZIP_PATH)
        with zipfile.ZipFile(ZIP_PATH, "r") as z:
            z.extractall(EXTRACT_DIR)
    try:
        g = gpd.read_file(SHP_L3_PATH, columns=ADM_COLS, ignore_geometry=True)
    except TypeError:
        g = gpd.read_file(SHP_L3_PATH)
    tbl = pd.DataFrame(g[ADM_COLS]).astype(str).drop_duplicates("GID_3")
    tbl.to_csv(INDEX_CSV, index=False, encoding="utf-8-sig")
    return tbl


def load_targets():
    idx = build_commune_index()
    if PROVINCE_FILTER:
        idx = idx[idx["GID_1"].isin(PROVINCE_FILTER)]
    idx = idx.sort_values(["GID_1", "GID_3"]).reset_index(drop=True)
    if TEST_LIMIT:
        idx = idx.head(TEST_LIMIT)
    return idx

def _gid_variants(gid):
    g = gid.strip()
    base = re.sub(r"_\d+$", "", g)
    return list(dict.fromkeys([g, base, base + "_1"]))


def resolve_gid(gid_3):
    for cand in _gid_variants(gid_3):
        fc = communes_fc.filter(ee.Filter.eq("GID_3", cand))
        if fc.size().getInfo() > 0:
            return cand, fc
    base = re.sub(r"_\d+$", "", gid_3.strip())
    fc = communes_fc.filter(ee.Filter.stringStartsWith("GID_3", base))
    if fc.size().getInfo() > 0:
        found = fc.aggregate_array("GID_3").getInfo()
        exact = [x for x in found if re.sub(r"_\d+$", "", x) == base]
        if exact:
            return exact[0], communes_fc.filter(ee.Filter.eq("GID_3", exact[0]))
    raise ValueError(f"Không tìm thấy mã xã '{gid_3}' trong asset {ASSET_ID}.")


def get_commune_info(gid_3):
    asset_gid, fc = resolve_gid(gid_3)
    feat = fc.first().getInfo()
    props = feat.get("properties", {})
    geom = fc.geometry()
    area_ha = geom.area(maxError=1).getInfo() / 1e4
    return {
        "GID_3": gid_3.strip(), "GID_3_ASSET": asset_gid,
        "NAME_3": props.get("NAME_3", ""), "TYPE_3": props.get("TYPE_3", ""),
        "GID_2": props.get("GID_2", ""), "NAME_2": props.get("NAME_2", ""),
        "GID_1": props.get("GID_1", ""), "NAME_1": props.get("NAME_1", ""),
        "area_ha": area_ha, "geometry": geom, "geojson": feat.get("geometry"),
    }


def add_indices(img):
    ndvi = img.normalizedDifference(["NIR", "RED"]).rename("NDVI")
    ndbi = img.normalizedDifference(["SWIR1", "NIR"]).rename("NDBI")
    mndwi = img.normalizedDifference(["GREEN", "SWIR1"]).rename("MNDWI")
    bsi = img.expression(
        "((SWIR1 + RED) - (NIR + BLUE)) / ((SWIR1 + RED) + (NIR + BLUE))",
        {"SWIR1": img.select("SWIR1"), "RED": img.select("RED"),
         "NIR": img.select("NIR"), "BLUE": img.select("BLUE")},
    ).rename("BSI")
    return img.addBands([ndvi, ndbi, mndwi, bsi])


def _month_range(year, month):
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    return f"{year}-{month:02d}-01", f"{ny}-{nm:02d}-01"


L5_ID = "LANDSAT/LT05/C02/T1_L2"
L7_ID = "LANDSAT/LE07/C02/T1_L2"
L8_ID = "LANDSAT/LC08/C02/T1_L2"


def _mask_landsat_c2(img):
    qa = img.select("QA_PIXEL")
    clear = (qa.bitwiseAnd(1 << 0).eq(0).And(qa.bitwiseAnd(1 << 1).eq(0))
             .And(qa.bitwiseAnd(1 << 3).eq(0)).And(qa.bitwiseAnd(1 << 4).eq(0)))
    return img.select("SR_B.").multiply(0.0000275).add(-0.2).updateMask(clear)


def prep_l8(img):
    return _mask_landsat_c2(img).select(
        ["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7"],
        ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])


def prep_l5(img):
    return _mask_landsat_c2(img).select(
        ["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7"],
        ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2"])


prep_l7 = prep_l5


def get_landsat_collection(collection_id, geom, year, month):
    s_date, e_date = _month_range(year, month)
    return (ee.ImageCollection(collection_id).filterBounds(geom)
            .filterDate(s_date, e_date).filter(ee.Filter.lt("CLOUD_COVER", MAX_CLOUD)))


DAY_SENSOR_COLLECTIONS = {"Landsat-5": L5_ID, "Landsat-7(SLC-off)": L7_ID, "Landsat-8": L8_ID}


def assigned_day_source(year):
    if 2004 <= year <= 2011:
        return "Landsat-5", L5_ID, prep_l5
    if year == 2012:
        return "Landsat-7(SLC-off)", L7_ID, prep_l7
    if 2013 <= year <= 2026:
        return "Landsat-8", L8_ID, prep_l8
    raise ValueError(f"Năm {year} nằm ngoài kế hoạch nguồn Day 2004–2026")


def build_monthly_composite(geom, year, month):
    sensor, collection_id, prepare = assigned_day_source(year)
    col = get_landsat_collection(collection_id, geom, year, month)
    n_scenes = col.size().getInfo()
    if n_scenes == 0:
        return None, sensor, 0
    return add_indices(col.map(prepare).median()), sensor, n_scenes


VIIRS_ID_PRIMARY  = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"
VIIRS_ID_FALLBACK = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG"
DMSP_ID = "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS"


def get_viirs_image(geom, year, month):
    s_date, e_date = _month_range(year, month)
    source_id = VIIRS_ID_PRIMARY
    col = ee.ImageCollection(source_id).filterBounds(geom).filterDate(s_date, e_date)
    if col.size().getInfo() == 0:
        source_id = VIIRS_ID_FALLBACK
        col = ee.ImageCollection(source_id).filterBounds(geom).filterDate(s_date, e_date)
    if col.size().getInfo() == 0:
        return None, None
    return col.select(["avg_rad", "cf_cvg"]).mean().clip(geom), source_id


def get_dmsp_image(geom, year):
    col = (ee.ImageCollection(DMSP_ID).filterBounds(geom)
           .filterDate(f"{year}-01-01", f"{year+1}-01-01"))
    if col.size().getInfo() == 0:
        return None
    return col.select(["stable_lights", "cf_cvg"]).mean().clip(geom)


def iter_months(y0, m0, y1, m1):
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


_today_vn = datetime.now(timezone(timedelta(hours=7)))
ACTUAL_END = min((END_YEAR, END_MONTH), (_today_vn.year, _today_vn.month))
ALL_MONTHS = list(iter_months(START_YEAR, START_MONTH, *ACTUAL_END))
VIIRS_MONTHS = [ym for ym in ALL_MONTHS if ym >= VIIRS_FIRST_DATE]
DMSP_YEARS = list(range(START_YEAR, min(DMSP_LAST_YEAR, ACTUAL_END[0]) + 1))
print(f"[+] Ảnh ngày: {len(ALL_MONTHS)} tháng | VIIRS: {len(VIIRS_MONTHS)} tháng | DMSP: {len(DMSP_YEARS)} năm")

def download_month_image(gid_3, geom, year, month, out_dir, file_stem, scale=SCALE, max_retry=3):
    """Trả (path, sensor, n_scenes). path=None và n_scenes>0 nghĩa là tải hỏng."""
    fname = make_image_name(file_stem, year, month, "Day")
    path = os.path.join(out_dir, fname)
    if (not OVERWRITE) and os.path.exists(path) and os.path.getsize(path) > 2000:
        sidecar = path + ".json"
        if os.path.isfile(sidecar):
            with open(sidecar, encoding="utf-8") as f:
                meta = json.load(f)
            return path, meta["sensor"], meta["n_scenes"]
        _, sensor, n_scenes = build_monthly_composite(geom, year, month)
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump({"sensor": sensor, "n_scenes": n_scenes}, f)
        return path, sensor, n_scenes

    img, sensor, n_scenes = build_monthly_composite(geom, year, month)
    if img is None:
        return None, None, 0
    export_img = img.select(BAND_ORDER).toFloat().clip(geom)
    scales = [scale] if scale == SCALE_FALLBACK else [scale, SCALE_FALLBACK]
    last_err = None
    for cur_scale in scales:
        for attempt in range(max_retry):
            tmp = path + ".part"
            try:
                url = export_img.getDownloadURL({"scale": cur_scale, "crs": "EPSG:4326", "region": geom,
                                                 "format": "GEO_TIFF", "filePerBand": False})
                r = requests.get(url, stream=True, timeout=600)
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
                if os.path.getsize(tmp) < 2000:
                    os.remove(tmp)
                    raise IOError("file tải về rỗng / quá nhỏ")
                os.replace(tmp, path)
                with open(path + ".json", "w", encoding="utf-8") as f:
                    json.dump({"sensor": sensor, "n_scenes": n_scenes}, f)
                if cur_scale != scale:
                    log.info(f"[{gid_3}] {year}-{month:02d}: hạ xuống {cur_scale} m do quá hạn mức ở {scale} m")
                return path, sensor, n_scenes
            except Exception as e:
                last_err = e
                if os.path.exists(tmp):
                    os.remove(tmp)
                msg = str(e).lower()
                if "too large" in msg or "request size" in msg or "limit" in msg:
                    break
                time.sleep(3 * (attempt + 1))
    log.warning(f"[{gid_3}] Day {year}-{month:02d}: tải thất bại ({last_err})")
    return None, sensor, n_scenes


def _download_night_image(img, path, geom, scale, label, max_retry=3):
    if img is None:
        return None
    last_err = None
    for attempt in range(max_retry):
        tmp = path + ".part"
        try:
            url = img.toFloat().clip(geom).getDownloadURL({"scale": scale, "crs": "EPSG:4326", "region": geom,
                                                           "format": "GEO_TIFF", "filePerBand": False})
            with requests.get(url, stream=True, timeout=300) as response:
                response.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in response.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
            if os.path.getsize(tmp) < 500:
                raise IOError("File ảnh rỗng hoặc quá nhỏ")
            os.replace(tmp, path)
            return path
        except Exception as exc:
            last_err = exc
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt < max_retry - 1:
                time.sleep(3 * (attempt + 1))
    log.warning(f"{label}: tải ảnh đêm thất bại ({last_err})")
    return None


def download_dmsp_annual_image(gid_3, geom, year, out_dir, file_stem, max_retry=3):
    path = os.path.join(out_dir, f"{file_stem}_{year}_DMSP.tif")
    if not OVERWRITE and os.path.exists(path) and os.path.getsize(path) > 500:
        return path
    return _download_night_image(get_dmsp_image(geom, year), path, geom, DMSP_SCALE,
                                 f"[{gid_3}] DMSP {year}", max_retry)


def download_viirs_month_image(gid_3, geom, year, month, out_dir, file_stem, max_retry=3):
    """Trả (path, collection). path=None và collection khác None nghĩa là tải hỏng."""
    path = os.path.join(out_dir, f"{file_stem}_{year}{month:02d}_VIIRS.tif")
    if not OVERWRITE and os.path.exists(path) and os.path.getsize(path) > 500:
        sidecar = path + ".json"
        if os.path.isfile(sidecar):
            with open(sidecar, encoding="utf-8") as f:
                return path, json.load(f)["source_collection"]
        _, collection_id = get_viirs_image(geom, year, month)
        with open(sidecar, "w", encoding="utf-8") as f:
            json.dump({"source_collection": collection_id}, f)
        return path, collection_id
    image, collection_id = get_viirs_image(geom, year, month)
    path = _download_night_image(image, path, geom, VIIRS_SCALE,
                                 f"[{gid_3}] VIIRS {year}-{month:02d}", max_retry)
    if path is not None:
        with open(path + ".json", "w", encoding="utf-8") as f:
            json.dump({"source_collection": collection_id}, f)
    return path, collection_id

import rasterio
import cv2
import pyproj
from shapely.geometry import box as shp_box
from shapely.ops import transform as shp_transform
from skimage.morphology import remove_small_objects, skeletonize
from skimage.measure import label, regionprops


def read_stack(image_path):
    with rasterio.open(image_path) as src:
        arr = src.read().astype(float)
        names = list(src.descriptions or [])
        if names and all(names) and set(BAND_ORDER).issubset(set(names)):
            idx = {n: names.index(n) for n in BAND_ORDER}
        else:
            idx = {n: i for i, n in enumerate(BAND_ORDER) if i < src.count}
        bands = {n: arr[i] for n, i in idx.items()}
        red, green, blue = bands["RED"], bands["GREEN"], bands["BLUE"]
        finite = np.isfinite(red) & np.isfinite(green) & np.isfinite(blue)
        commune_mask = finite & ((red > 0) | (green > 0) | (blue > 0))
        if src.nodata is not None:
            commune_mask &= (red != src.nodata)
        tr, crs = src.transform, src.crs
        if crs is not None and crs.is_projected:
            res_x, res_y = abs(tr[0]), abs(tr[4])
            pixel_area_m2 = res_x * res_y
        else:
            b = src.bounds
            lon_c, lat_c = (b.left + b.right) / 2, (b.bottom + b.top) / 2
            utm_zone = int((lon_c + 180) / 6) + 1
            hemi = "" if lat_c >= 0 else " +south"
            utm_crs = f"+proj=utm +zone={utm_zone}{hemi} +datum=WGS84 +units=m +no_defs"
            project = pyproj.Transformer.from_crs(crs, utm_crs, always_xy=True).transform
            cell = shp_box(b.left, b.bottom, b.left + abs(tr[0]), b.bottom + abs(tr[4]))
            pixel_area_m2 = shp_transform(project, cell).area
            res_x = res_y = math.sqrt(pixel_area_m2)
        meta = {"height": src.height, "width": src.width, "n_bands": src.count,
                "res_m": res_x, "pixel_area_m2": pixel_area_m2}
    nir, swir1 = bands["NIR"], bands["SWIR1"]
    if "NDVI" not in bands:
        bands["NDVI"] = (nir - red) / (nir + red + 1e-6)
    if "NDBI" not in bands:
        bands["NDBI"] = (swir1 - nir) / (swir1 + nir + 1e-6)
    if "MNDWI" not in bands:
        bands["MNDWI"] = (green - swir1) / (green + swir1 + 1e-6)
    return bands, commune_mask, meta


def extract_road_metrics(bands, commune_mask, meta):
    ndvi = np.nan_to_num(bands["NDVI"], nan=1.0)
    ndbi = np.nan_to_num(bands["NDBI"], nan=-1.0)
    mndwi = np.nan_to_num(bands["MNDWI"], nan=1.0)
    road_spectral_mask = (ndvi < 0.25) & (mndwi < -0.05) & (ndbi > -0.10) & commune_mask
    binary_road = (road_spectral_mask * 255).astype(np.uint8)
    kernel_len = 5
    k_h = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_len, 1))
    k_v = cv2.getStructuringElement(cv2.MORPH_RECT, (1, kernel_len))
    linear_roads = cv2.bitwise_or(cv2.morphologyEx(binary_road, cv2.MORPH_OPEN, k_h),
                                  cv2.morphologyEx(binary_road, cv2.MORPH_OPEN, k_v))
    clean_roads = remove_small_objects(linear_roads > 0, min_size=10)
    road_skeleton = skeletonize(clean_roads)
    road_px, center_px = int(np.sum(clean_roads)), int(np.sum(road_skeleton))
    return {"road_corridor_pixels": road_px, "road_centerline_pixels": center_px,
            "road_length_km": center_px * meta["res_m"] / 1000.0,
            "road_area_ha": road_px * meta["pixel_area_m2"] / 1e4}


def extract_industrial_metrics(bands, commune_mask, meta):
    green, red, nir, swir1 = bands["GREEN"], bands["RED"], bands["NIR"], bands["SWIR1"]
    ndvi = np.nan_to_num((nir - red) / (nir + red + 1e-6), nan=1.0)
    mndwi = np.nan_to_num((green - swir1) / (green + swir1 + 1e-6), nan=1.0)
    ndmri = np.nan_to_num((swir1 - nir) / (swir1 + nir + 1e-6), nan=-1.0)
    swir1_f = np.nan_to_num(swir1, nan=0.0)
    industrial_spectral = ((swir1_f > 0.18) & (ndmri > 0.05) & (ndvi < 0.25)
                           & (mndwi < -0.05) & commune_mask)
    labeled_mask = label(industrial_spectral)
    clean = np.zeros_like(industrial_spectral, dtype=bool)
    n_clusters = 0
    for r in regionprops(labeled_mask):
        if r.area >= 5:
            clean[labeled_mask == r.label] = True
            n_clusters += 1
    ind_px = int(np.sum(clean))
    ind_m2 = ind_px * meta["pixel_area_m2"]
    return {"industrial_clusters": n_clusters, "industrial_pixels": ind_px,
            "industrial_area_ha": ind_m2 / 1e4, "industrial_area_m2": ind_m2}


def extract_water_metrics(bands, commune_mask, meta):
    green, red, nir, swir1 = bands["GREEN"], bands["RED"], bands["NIR"], bands["SWIR1"]
    green_f = np.nan_to_num(green, nan=0.0)
    swir1_f = np.nan_to_num(swir1, nan=1.0)
    mndwi = np.nan_to_num((green - swir1) / (green + swir1 + 1e-6), nan=-1.0)
    ndvi = np.nan_to_num((nir - red) / (nir + red + 1e-6), nan=1.0)
    water_mask = (mndwi > 0.12) & (ndvi < 0.15) & (green_f > 0.04) & (swir1_f < 0.10) & commune_mask
    clean_water = remove_small_objects(water_mask, min_size=4)
    labeled_water = label(clean_water)
    natural = np.zeros_like(clean_water, dtype=bool)
    aqua = np.zeros_like(clean_water, dtype=bool)
    pixel_area_m2 = meta["pixel_area_m2"]
    n_natural = n_aqua = 0
    for r in regionprops(labeled_water):
        area_m2 = r.area * pixel_area_m2
        aspect_ratio = r.major_axis_length / (r.minor_axis_length + 1e-6)
        if area_m2 >= 12000 or (aspect_ratio >= 4.0 and area_m2 >= 8000):
            natural[labeled_water == r.label] = True; n_natural += 1
        else:
            aqua[labeled_water == r.label] = True; n_aqua += 1
    total_px = int(np.sum(clean_water))
    return {"water_pixels": total_px, "water_area_ha": total_px * pixel_area_m2 / 1e4,
            "water_natural_ha": int(np.sum(natural)) * pixel_area_m2 / 1e4,
            "water_aquaculture_ha": int(np.sum(aqua)) * pixel_area_m2 / 1e4,
            "water_natural_bodies": n_natural, "water_aquaculture_bodies": n_aqua}


def extract_spectral_stats(bands, commune_mask):
    out = {}
    for name in BAND_ORDER:
        vals = bands[name][commune_mask] if name in bands else np.array([])
        vals = vals[np.isfinite(vals)]
        out[f"{name}_mean"] = round(float(np.mean(vals)), 6) if vals.size else np.nan
        out[f"{name}_stdDev"] = round(float(np.std(vals)), 6) if vals.size else np.nan
    return out


def analyze_image(image_path):
    bands, commune_mask, meta = read_stack(image_path)
    valid_px = int(np.sum(commune_mask))
    row = {"img_height": meta["height"], "img_width": meta["width"], "n_bands": meta["n_bands"],
           "pixel_size_m": round(meta["res_m"], 2), "valid_pixels": valid_px,
           "valid_area_ha": valid_px * meta["pixel_area_m2"] / 1e4}
    row.update(extract_road_metrics(bands, commune_mask, meta))
    row.update(extract_industrial_metrics(bands, commune_mask, meta))
    row.update(extract_water_metrics(bands, commune_mask, meta))
    row.update(extract_spectral_stats(bands, commune_mask))
    if row["valid_area_ha"] > 0:
        km2 = row["valid_area_ha"] / 100.0
        row["road_density_km_per_km2"] = row["road_length_km"] / km2
        row["industrial_share_pct"] = 100.0 * row["industrial_area_ha"] / row["valid_area_ha"]
        row["water_share_pct"] = 100.0 * row["water_area_ha"] / row["valid_area_ha"]
    else:
        row["road_density_km_per_km2"] = row["industrial_share_pct"] = row["water_share_pct"] = np.nan
    return row


def analyze_night_image(image_path, sensor, band, scale_m, lit_threshold):
    with rasterio.open(image_path) as src:
        main = src.read(1, masked=True).astype(float).filled(np.nan).astype(float)
        cf_cvg = src.read(2, masked=True).astype(float).filled(np.nan) if src.count >= 2 else None
        nodata = src.nodata
        transform, crs, height, width = src.transform, src.crs, src.height, src.width
    valid = np.isfinite(main)
    if cf_cvg is not None:
        valid &= np.isfinite(cf_cvg) & (cf_cvg > 0)
    if nodata is not None:
        valid &= (main != nodata)
    n_valid = int(valid.sum())
    if n_valid == 0:
        return {}
    vals = main[valid]
    lit_mask = valid & (main >= lit_threshold)
    lit_vals = main[lit_mask]
    n_lit = int(lit_mask.sum())
    from pyproj import Geod, Transformer
    geod = Geod(ellps="WGS84")
    to_ll = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    row_areas = np.zeros(height, dtype=float)
    for rr in range(height):
        corners = [transform * (cc, r) for cc, r in [(width/2, rr), (width/2+1, rr),
                                                     (width/2+1, rr+1), (width/2, rr+1)]]
        lon, lat = zip(*(to_ll.transform(x, y) for x, y in corners))
        row_areas[rr] = abs(geod.polygon_area_perimeter(lon, lat)[0])
    lit_area_ha = float(np.dot(lit_mask.sum(axis=1), row_areas) / 1e4)
    mean_v, std_v = float(np.mean(vals)), float(np.std(vals))
    row = {
        "night_img_width_px": width, "night_img_height_px": height, "night_valid_pixels": n_valid,
        "night_sensor": sensor, "night_band": band, "night_scale_m": scale_m,
        "night_tnl": round(float(np.sum(vals)), 4), "night_mean": round(mean_v, 4),
        "night_std": round(std_v, 4), "night_min": round(float(np.min(vals)), 4),
        "night_max": round(float(np.max(vals)), 4),
        "night_spatial_cv": round(std_v / mean_v, 4) if mean_v > 0 else 0.0,
        "night_lit_pixels": n_lit, "night_lit_area_ha": round(lit_area_ha, 2),
        "night_electrification_pct": round(100.0 * n_lit / n_valid, 2),
        "night_lit_pop_proxy": round(float(np.sum(lit_vals)), 4),
    }
    if cf_cvg is not None:
        cf_valid = cf_cvg[valid]
        row["night_cloud_free_obs"] = round(float(np.nanmean(cf_valid)), 2) if cf_valid.size else np.nan
    else:
        row["night_cloud_free_obs"] = np.nan
    return row


def get_night_lit_threshold(sensor):
    return VIIRS_LIT_THRESHOLD if sensor == "VIIRS" else DMSP_LIT_THRESHOLD

import geopandas as gpd
from shapely.geometry import LineString, shape as shp_shape

OVERPASS_MIRRORS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
MAIN_ROAD_TYPES = {"motorway", "trunk", "primary", "secondary", "tertiary",
                   "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link"}
_osm_lock = threading.Lock()
_osm_last = [0.0]


def _overpass_post(url, query, headers):
    """Mọi luồng xếp hàng qua một khóa để không dồn truy vấn lên máy chủ Overpass công cộng."""
    with _osm_lock:
        wait = OSM_MIN_INTERVAL_SEC - (time.time() - _osm_last[0])
        if wait > 0:
            time.sleep(wait)
        _osm_last[0] = time.time()
    return requests.post(url, data={"data": query}, headers=headers, timeout=90)


def fetch_osm_road_length(commune):
    poly = shp_shape(commune["geojson"])
    west, south, east, north = poly.bounds
    query = f"""
[out:json][timeout:60];
(
  way["highway"]({south},{west},{north},{east});
);
out geom;
"""
    headers = {"User-Agent": "ResearchSpatialEcon/1.0 (contact@research.edu)"}
    elements = None
    for url in OVERPASS_MIRRORS:
        try:
            resp = _overpass_post(url, query, headers)
            if resp.status_code == 200:
                elements = resp.json().get("elements", [])
                break
        except Exception:
            continue
    if elements is None:
        return {"osm_road_km_total": np.nan, "osm_road_km_main": np.nan, "osm_road_segments": 0}
    lines, types = [], []
    for el in elements:
        if "geometry" in el and len(el["geometry"]) >= 2:
            lines.append(LineString([(p["lon"], p["lat"]) for p in el["geometry"]]))
            types.append(el.get("tags", {}).get("highway", "residential"))
    if not lines:
        return {"osm_road_km_total": 0.0, "osm_road_km_main": 0.0, "osm_road_segments": 0}
    gdf = gpd.GeoDataFrame({"highway": types, "geometry": lines}, crs="EPSG:4326").clip(poly)
    if gdf.empty:
        return {"osm_road_km_total": 0.0, "osm_road_km_main": 0.0, "osm_road_segments": 0}
    utm_epsg = 32600 + int(((west + east) / 2 + 180) / 6) + 1
    gdf_utm = gdf.to_crs(f"EPSG:{utm_epsg}")
    total_km = gdf_utm.geometry.length.sum() / 1000.0
    main_km = gdf_utm[gdf_utm["highway"].isin(MAIN_ROAD_TYPES)].geometry.length.sum() / 1000.0
    return {"osm_road_km_total": round(total_km, 4), "osm_road_km_main": round(main_km, 4),
            "osm_road_segments": int(len(gdf))}


def get_osm_cached(commune):
    """Trả (dict OSM, ok). Chỉ lưu đệm khi tải thành công nên lần thử lại không truy vấn lại."""
    empty = {"osm_road_km_total": np.nan, "osm_road_km_main": np.nan, "osm_road_segments": np.nan}
    if not FETCH_OSM_REFERENCE:
        return empty, True
    cache = os.path.join(OSM_CACHE_DIR, f"{commune['GID_3']}.json")
    if os.path.isfile(cache):
        with open(cache, encoding="utf-8") as f:
            return json.load(f), True
    try:
        osm = fetch_osm_road_length(commune)
    except Exception as exc:
        log.warning(f"[{commune['GID_3']}] OSM lỗi: {exc}")
        return empty, False
    if pd.isna(osm["osm_road_km_total"]):
        return osm, False
    _atomic_json(cache, osm)
    return osm, True

ADMIN_COLUMNS = ["gid_3", "name_3", "type_3", "gid_2", "name_2", "gid_1", "name_1", "commune_area_ha"]
DAY_COLUMNS = ADMIN_COLUMNS + [
    "date", "year", "month", "sensor", "source_collection", "source_policy",
    "n_scenes", "image_file",
    "img_height", "img_width", "n_bands", "pixel_size_m", "valid_pixels", "valid_area_ha",
    "road_corridor_pixels", "road_centerline_pixels", "road_length_km", "road_area_ha",
    "road_density_km_per_km2", "industrial_clusters", "industrial_pixels",
    "industrial_area_ha", "industrial_area_m2", "industrial_share_pct",
    "water_pixels", "water_area_ha", "water_natural_ha", "water_aquaculture_ha",
    "water_natural_bodies", "water_aquaculture_bodies", "water_share_pct",
    "osm_road_km_total", "osm_road_km_main", "osm_road_segments",
] + [f"{b}_{stat}" for b in BAND_ORDER for stat in ("mean", "stdDev")]
NIGHT_COLUMNS = ADMIN_COLUMNS + [
    "date", "year", "month", "sensor", "temporal_resolution", "value_unit",
    "source_band", "source_collection", "image_file", "scale_m",
    "raster_width_px", "raster_height_px", "valid_pixel_count", "ntl_sum",
    "ntl_mean", "ntl_std", "ntl_min", "ntl_max", "spatial_cv",
    "lit_pixels", "lit_area_ha", "lit_ratio_pct", "lit_pop_proxy",
    "cloud_free_obs", "ntl_ma3", "ntl_mom_growth_pct",
]
NIGHT_METRIC_MAP = {
    "night_img_width_px": "raster_width_px", "night_img_height_px": "raster_height_px",
    "night_valid_pixels": "valid_pixel_count", "night_tnl": "ntl_sum", "night_mean": "ntl_mean",
    "night_std": "ntl_std", "night_min": "ntl_min", "night_max": "ntl_max",
    "night_spatial_cv": "spatial_cv", "night_lit_pixels": "lit_pixels",
    "night_lit_area_ha": "lit_area_ha", "night_electrification_pct": "lit_ratio_pct",
    "night_lit_pop_proxy": "lit_pop_proxy", "night_cloud_free_obs": "cloud_free_obs",
}


def _admin(commune):
    return {"gid_3": commune["GID_3"], "name_3": commune["NAME_3"], "type_3": commune["TYPE_3"],
            "gid_2": commune["GID_2"], "name_2": commune["NAME_2"], "gid_1": commune["GID_1"],
            "name_1": commune["NAME_1"], "commune_area_ha": round(commune["area_ha"], 4)}


def _write_csv(rows, path, columns):
    df = pd.DataFrame(rows).reindex(columns=columns).round(4)
    tmp = path + ".part"
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    os.replace(tmp, path)
    return df


def _add_viirs_trends(df):
    df = df.sort_values(["year", "month"]).reset_index(drop=True).copy()
    if df.empty:
        return df
    months_id = df["year"].astype(int) * 12 + df["month"].astype(int)
    contiguous = months_id.diff().eq(1)
    valid = df["ntl_sum"].notna()
    df["ntl_ma3"] = df["ntl_sum"].rolling(3, min_periods=3).mean().where(
        contiguous & contiguous.shift(1, fill_value=False))
    previous = df["ntl_sum"].shift()
    df["ntl_mom_growth_pct"] = (100 * (df["ntl_sum"] / previous - 1)).where(
        contiguous & valid & previous.notna() & previous.ne(0))
    return df


def _night_row(commune, paths, year, month, sensor, image_path, collection_id=None):
    dmsp = sensor == "DMSP-OLS"
    band = "stable_lights" if dmsp else "avg_rad"
    scale = DMSP_SCALE if dmsp else VIIRS_SCALE
    row = {**_admin(commune), "year": year, "month": np.nan if dmsp else month,
           "date": f"{year}-01-01" if dmsp else f"{year}-{month:02d}-01",
           "sensor": sensor, "temporal_resolution": "annual" if dmsp else "monthly",
           "value_unit": "DN" if dmsp else "nW/cm²/sr", "source_band": band,
           "source_collection": DMSP_ID if dmsp else collection_id, "scale_m": scale}
    if image_path is not None:
        raw = analyze_night_image(image_path, sensor, band, scale, get_night_lit_threshold(sensor))
        if not raw:
            return None
        row["image_file"] = os.path.relpath(image_path, paths["commune_dir"])
        row.update({target: raw.get(source, np.nan) for source, target in NIGHT_METRIC_MAP.items()})
    return row

def process_commune(gid):
    _check_stop()
    t0 = time.time()
    commune = get_commune_info(gid)
    paths = build_commune_paths(commune)
    for key in ("img_dir", "img_dir_dmsp", "img_dir_viirs", "csv_dir"):
        os.makedirs(paths[key], exist_ok=True)
    osm, osm_ok = get_osm_cached(commune)

    # ---- Ảnh ngày ----
    day_rows, day_failed = [], 0
    for y, m in ALL_MONTHS:
        _check_stop()
        planned_sensor, planned_collection, _ = assigned_day_source(y)
        row = {**_admin(commune), **osm, "date": f"{y}-{m:02d}-01", "year": y, "month": m,
               "sensor": planned_sensor, "source_collection": planned_collection,
               "source_policy": DAY_SOURCE_POLICY}
        try:
            path, sensor, n_scenes = download_month_image(commune["GID_3"], commune["geometry"], y, m,
                                                           paths["img_dir"], paths["file_stem"])
            if path is not None:
                row.update({"sensor": sensor, "source_collection": DAY_SENSOR_COLLECTIONS[sensor],
                            "source_policy": DAY_SOURCE_POLICY, "n_scenes": n_scenes,
                            "image_file": os.path.relpath(path, paths["commune_dir"]),
                            **analyze_image(path)})
                day_rows.append(row)
            else:
                if n_scenes and n_scenes > 0:
                    day_failed += 1
                if KEEP_EMPTY_MONTHS:
                    row.update({"n_scenes": 0, "image_file": ""}); day_rows.append(row)
        except Exception as exc:
            day_failed += 1
            log.warning(f"[{gid}] Day {y}-{m:02d}: {exc}")
            if KEEP_EMPTY_MONTHS:
                day_rows.append(row)
    day = _write_csv(day_rows, paths["csv_day"], DAY_COLUMNS)

    # ---- DMSP theo năm (rỗng với 2024, vẫn ghi CSV để giữ cấu trúc) ----
    dmsp_rows = []
    for y in DMSP_YEARS:
        _check_stop()
        try:
            path = download_dmsp_annual_image(commune["GID_3"], commune["geometry"], y,
                                              paths["img_dir_dmsp"], paths["file_stem"])
            if path is not None or KEEP_EMPTY_MONTHS:
                r = _night_row(commune, paths, y, None, "DMSP-OLS", path)
                if r is not None:
                    dmsp_rows.append(r)
        except Exception as exc:
            log.warning(f"[{gid}] DMSP {y}: {exc}")
    dmsp = _write_csv(dmsp_rows, paths["csv_dmsp"], NIGHT_COLUMNS)

    # ---- VIIRS theo tháng ----
    viirs_rows, viirs_failed = [], 0
    for y, m in VIIRS_MONTHS:
        _check_stop()
        try:
            path, collection_id = download_viirs_month_image(commune["GID_3"], commune["geometry"], y, m,
                                                             paths["img_dir_viirs"], paths["file_stem"])
            if path is None and collection_id is not None:
                viirs_failed += 1
            if path is not None or KEEP_EMPTY_MONTHS:
                r = _night_row(commune, paths, y, m, "VIIRS", path, collection_id)
                if r is not None:
                    viirs_rows.append(r)
        except Exception as exc:
            viirs_failed += 1
            log.warning(f"[{gid}] VIIRS {y}-{m:02d}: {exc}")
    viirs = _write_csv(_add_viirs_trends(pd.DataFrame(viirs_rows).reindex(columns=NIGHT_COLUMNS)),
                       paths["csv_viirs"], NIGHT_COLUMNS)

    # ---- Night_All ----
    frames = [f for f in (dmsp, viirs) if not f.empty]
    night = (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()).reindex(columns=NIGHT_COLUMNS)
    if not night.empty:
        night = night.sort_values(["year", "month", "sensor"], na_position="first").reset_index(drop=True)
    _write_csv(night, paths["csv_night"], NIGHT_COLUMNS)

    push_commune(paths)          # Colab/path: chép ngay; rclone: luồng nền tự đẩy
    complete = day_failed == 0 and viirs_failed == 0 and osm_ok
    return {
        "gid_3": gid, "status": "done" if complete else "partial",
        "name_1": commune["NAME_1"], "name_3": commune["NAME_3"], "rel_dir": paths["rel_dir"],
        "day_rows": len(day), "day_failed": day_failed,
        "viirs_rows": len(viirs), "viirs_failed": viirs_failed, "osm_ok": osm_ok,
        "seconds": round(time.time() - t0, 1),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }

import subprocess

_status_lock = threading.Lock()


def _atomic_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, default=lambda o: None if pd.isna(o) else str(o))
    os.replace(tmp, path)


def write_status(gid, info):
    """Nối một dòng vào file của lượt hiện tại."""
    line = json.dumps(info, ensure_ascii=False, default=lambda o: None if pd.isna(o) else str(o))
    with _status_lock:
        os.makedirs(LEASE_DIR, exist_ok=True)
        with open(LEASE_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def load_all_status():
    """Phát lại mọi lượt theo thứ tự thời gian; bản ghi sau cùng của mỗi xã thắng.
    Dòng cụt (file đang được ghi dở khi đồng bộ) bị bỏ qua."""
    out = {}
    for p in sorted(glob.glob(os.path.join(LEASE_DIR, "lease_*.jsonl"))):
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


def is_finished(st):
    return bool(st) and (st.get("status") == "done" or int(st.get("attempts", 0)) >= MAX_ATTEMPTS)


# ---------------- Chế độ Colab / path: chép từng xã ----------------
def _copy_tree(src_dir, dst_dir):
    for cur, _dirs, files in os.walk(src_dir):
        rel = os.path.relpath(cur, src_dir)
        tdir = dst_dir if rel == "." else os.path.join(dst_dir, rel)
        os.makedirs(tdir, exist_ok=True)
        for name in files:
            if name.endswith(".part") or (name.endswith(".json") and not UPLOAD_SIDECAR_JSON):
                continue
            s, t = os.path.join(cur, name), os.path.join(tdir, name)
            if name.lower().endswith(".tif") and os.path.isfile(t) and os.path.getsize(t) == os.path.getsize(s):
                continue
            shutil.copy2(s, t)


def _drop_local_tifs(commune_dir):
    for p in glob.glob(os.path.join(commune_dir, "**", "*.tif"), recursive=True):
        os.remove(p)
        if os.path.exists(p + ".json"):
            os.remove(p + ".json")


def push_commune(paths):
    if STORAGE_MODE == "rclone":
        return
    _copy_tree(paths["commune_dir"], os.path.join(DEST_BASE, paths["rel_dir"]))
    if not KEEP_LOCAL_TIF:
        _drop_local_tifs(paths["commune_dir"])


# ---------------- Chế độ rclone: luồng nền ----------------
REMOTE_BASE = f"{RCLONE_REMOTE}:{DRIVE_FOLDER_NAME}"
RCLONE_COMMON = ["--transfers", "4", "--checkers", "8", "--tpslimit", "8",
                 "--retries", "5", "--low-level-retries", "20", "--stats-log-level", "NOTICE"]


def _rclone(args, timeout=6 * 3600):
    res = subprocess.run(["rclone", *args], capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        log.warning(f"rclone {' '.join(args[:3])} lỗi: {res.stderr.strip()[-400:]}")
    return res.returncode == 0


def rclone_sync_once(final=False):
    """TIF: chuyển lên Drive (xóa bản trên máy nếu KEEP_LOCAL_TIF=False). CSV, trạng thái, log: chép."""
    prov_local = os.path.join(LOCAL_ROOT, SUB_PROV)
    if os.path.isdir(prov_local):
        verb = "copy" if KEEP_LOCAL_TIF else "move"
        tif_filters = ["--filter", "- *.part", "--filter", "+ *.tif", "--filter", "- *"]
        age = [] if final else ["--min-age", "2m"]
        _rclone([verb, prov_local, f"{REMOTE_BASE}/{SUB_PROV}", *tif_filters, *age, *RCLONE_COMMON])
        csv_filters = ["--filter", "- *.part", "--filter", "+ *.csv"]
        if UPLOAD_SIDECAR_JSON:
            csv_filters += ["--filter", "+ *.json"]
        csv_filters += ["--filter", "- *"]
        _rclone(["copy", prov_local, f"{REMOTE_BASE}/{SUB_PROV}", *csv_filters,
                 "--create-empty-src-dirs", *RCLONE_COMMON])
    for sub in (SUB_INDEX, SUB_COMBINED, SUB_STATUS, SUB_LOGS):
        src = os.path.join(LOCAL_ROOT, sub)
        if os.path.isdir(src):
            _rclone(["copy", src, f"{REMOTE_BASE}/{sub}", "--filter", "- *.part", *RCLONE_COMMON])
    readme = os.path.join(LOCAL_ROOT, SUB_README)
    if os.path.isfile(readme):
        _rclone(["copyto", readme, f"{REMOTE_BASE}/{SUB_README}"])


class Uploader(threading.Thread):
    def __init__(self):
        super().__init__(name="uploader", daemon=True)
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.wait(UPLOAD_EVERY_SEC):
            try:
                rclone_sync_once()
                log.info("Đã đồng bộ lên Drive (lượt định kỳ).")
            except Exception as exc:
                log.warning(f"Đồng bộ định kỳ lỗi: {exc}")


def mount_drive_if_colab():
    """Phải chạy TRƯỚC khi tạo bất kỳ thư mục nào dưới /content/drive."""
    if STORAGE_MODE == "colab":
        from google.colab import drive
        if not os.path.isdir("/content/drive/MyDrive"):
            drive.mount("/content/drive")
        os.makedirs(DEST_BASE, exist_ok=True)


def init_storage():
    for d in (LOCAL_ROOT, WORK_PROV_ROOT, os.path.join(META_ROOT, SUB_INDEX),
              os.path.join(META_ROOT, SUB_COMBINED), LEASE_DIR, OSM_CACHE_DIR, LOG_DIR):
        os.makedirs(d, exist_ok=True)
    if STORAGE_MODE == "rclone":
        if shutil.which("rclone") is None:
            raise RuntimeError("Chưa cài rclone. Xem hướng dẫn ở ô cuối notebook.")
        if not _rclone(["lsd", f"{RCLONE_REMOTE}:"], timeout=120):
            raise RuntimeError(f"rclone chưa kết nối được remote '{RCLONE_REMOTE}'. Chạy `rclone config`.")
        _rclone(["mkdir", REMOTE_BASE], timeout=120)
        # Kéo trạng thái từ Drive về: máy ảo mới vẫn biết xã nào đã xong.
        res = subprocess.run(["rclone", "copy", f"{REMOTE_BASE}/{SUB_STATUS}/leases", LEASE_DIR,
                              "--update", "--retries", "1"],
                             capture_output=True, text=True, timeout=3600)
        if res.returncode == 0:
            log.info(f"Đã kéo trạng thái từ Drive về máy: {len(load_all_status()):,} xã có bản ghi.")
        elif "directory not found" in res.stderr:
            log.info("Drive chưa có trạng thái: bắt đầu lần chạy mới.")
        else:
            log.warning(f"Không kéo được trạng thái từ Drive: {res.stderr.strip()[-300:]}")
    with open(os.path.join(META_ROOT, SUB_README), "w", encoding="utf-8") as f:
        f.write(README_TEXT)

def write_progress(targets, statuses):
    rows = []
    for r in targets.itertuples():
        st = statuses.get(r.GID_3) or {}
        rows.append({"GID_1": r.GID_1, "NAME_1": r.NAME_1, "GID_2": r.GID_2, "NAME_2": r.NAME_2,
                     "GID_3": r.GID_3, "NAME_3": r.NAME_3, "TYPE_3": r.TYPE_3,
                     "status": st.get("status", "pending"), "attempts": st.get("attempts", 0),
                     "day_rows": st.get("day_rows"), "day_failed": st.get("day_failed"),
                     "viirs_rows": st.get("viirs_rows"), "viirs_failed": st.get("viirs_failed"),
                     "osm_ok": st.get("osm_ok"), "folder": st.get("rel_dir", ""),
                     "seconds": st.get("seconds"), "finished_at": st.get("finished_at"),
                     "error": st.get("error", "")})
    df = pd.DataFrame(rows)
    for path in (os.path.join(META_ROOT, SUB_STATUS, "progress.csv"),
                 os.path.join(META_ROOT, SUB_INDEX, f"commune_index_{YEAR}.csv")):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path + ".part", index=False, encoding="utf-8-sig")
        os.replace(path + ".part", path)
    return df


COMBINED_SPECS = [  # (đuôi tên CSV của xã, tên file gộp, cột)
    (f"_Day_Monthly_{DAY_SOURCE_POLICY}.csv", f"Day_Monthly_{DAY_SOURCE_POLICY}_{YEAR}_ALL.csv", "DAY"),
    ("_Night_VIIRS_Monthly.csv", f"Night_VIIRS_Monthly_{YEAR}_ALL.csv", "NIGHT"),
    ("_Night_DMSP_Annual.csv", f"Night_DMSP_Annual_{YEAR}_ALL.csv", "NIGHT"),
    ("_Night_All.csv", f"Night_All_{YEAR}_ALL.csv", "NIGHT"),
]


def build_combined(statuses):
    """Gộp CSV của mọi xã đã xử lý. Ưu tiên bản trên máy, thiếu thì đọc ở đích (Colab/path)."""
    out_dir = os.path.join(META_ROOT, SUB_COMBINED)
    os.makedirs(out_dir, exist_ok=True)
    dirs = []
    for st in statuses.values():
        rel = st.get("rel_dir")
        if not rel:
            continue
        local_csv = os.path.join(LOCAL_ROOT, rel, "CSV")
        dest_csv = os.path.join(DEST_BASE, rel, "CSV") if DEST_BASE else None
        dirs.append(local_csv if os.path.isdir(local_csv) else dest_csv)
    for suffix, out_name, kind in COMBINED_SPECS:
        cols = DAY_COLUMNS if kind == "DAY" else NIGHT_COLUMNS
        frames = []
        for d in dirs:
            if not d or not os.path.isdir(d):
                continue
            for p in glob.glob(os.path.join(d, f"*{suffix}")):
                try:
                    f = pd.read_csv(p, dtype={"gid_3": str, "gid_2": str, "gid_1": str})
                    if not f.empty:
                        frames.append(f)
                except Exception as exc:
                    log.warning(f"Không đọc được {p}: {exc}")
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        df = df.reindex(columns=cols)
        path = os.path.join(out_dir, out_name)
        df.to_csv(path + ".part", index=False, encoding="utf-8-sig")
        os.replace(path + ".part", path)
        log.info(f"Gộp {out_name}: {len(df):,} dòng")

OUTAGE_STREAK = 10          # số xã lỗi liên tiếp coi là sự cố chung
OUTAGE_SLEEP_SEC = 900      # nghỉ 15 phút
MAX_OUTAGES = 8             # quá 8 lần liên tiếp (~2 giờ) thì thoát mã 2


def _fail_info(gid, exc, attempts):
    return {"gid_3": gid, "status": "failed", "attempts": attempts,
            "error": f"{type(exc).__name__}: {str(exc)[:300]}",
            "finished_at": datetime.now().isoformat(timespec="seconds")}


def run_round(todo, statuses, total_targets):
    t0 = time.time()
    done_now = 0
    pending_fail = []       # lỗi chưa ghi, chờ xem có phải sự cố chung không
    handled = set()
    outage = False
    pool = ThreadPoolExecutor(max_workers=N_WORKERS, thread_name_prefix="w")
    futures = {pool.submit(process_commune, gid): gid for gid in todo}

    def consume(fut):
        nonlocal done_now
        handled.add(fut)
        if fut.cancelled():
            return
        gid = futures[fut]
        attempts = int((statuses.get(gid) or {}).get("attempts", 0)) + 1
        try:
            info = fut.result()
        except StopRequested:
            return                                   # dừng có trật tự: không phải lỗi của xã
        except Exception as exc:
            pending_fail.append((gid, exc, attempts))
            log.warning(f"[{gid}] lỗi: {type(exc).__name__}: {str(exc)[:200]}")
            return
        # Có một xã thành công: các lỗi đang chờ là lỗi riêng, ghi nhận và trừ lượt.
        for g, e, a in pending_fail:
            statuses[g] = _fail_info(g, e, a); write_status(g, statuses[g])
        pending_fail.clear()
        info["attempts"] = attempts
        statuses[gid] = info
        write_status(gid, info)
        done_now += 1
        if done_now % 20 == 0:
            finished = sum(is_finished(statuses.get(g)) for g in statuses)
            rate = done_now / max(time.time() - t0, 1)
            eta_h = (len(todo) - done_now) / rate / 3600
            log.info(f"{done_now}/{len(todo)} xã trong vòng | hoàn tất chung {finished}/{total_targets} "
                     f"| {rate*3600:.0f} xã/giờ | còn khoảng {eta_h:.1f} giờ")

    try:
        for fut in as_completed(futures):
            consume(fut)
            if len(pending_fail) >= OUTAGE_STREAK:
                log.error(f"{OUTAGE_STREAK} xã lỗi liên tiếp: nghi sự cố chung, tạm dừng vòng này.")
                outage = True
                break
    except BaseException:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown(wait=True, cancel_futures=True)    # chờ các xã đang chạy dở xong hoặc bỏ cuộc
    for fut in futures:                              # ghi nốt kết quả của xã vừa kịp xong
        if fut not in handled and fut.done():
            consume(fut)
    if outage:
        return "outage"                              # lỗi trong sự cố chung không bị trừ lượt
    for g, e, a in pending_fail:
        statuses[g] = _fail_info(g, e, a); write_status(g, statuses[g])
    return "stop" if STOP_EVENT.is_set() else "ok"


def main():
    mount_drive_if_colab()
    setup_logging()
    init_storage()
    init_earth_engine()
    targets = load_targets()
    gids = targets["GID_3"].tolist()
    log.info(f"Danh mục: {len(gids):,} xã thuộc {targets['GID_1'].nunique()} tỉnh | năm {YEAR} | "
             f"lưu vào '{DRIVE_FOLDER_NAME}' | lượt {RUN_ID}")

    if not _in_notebook():
        install_signal_handlers()
    if MAX_RUNTIME_SEC > 0:
        timer = threading.Timer(MAX_RUNTIME_SEC, request_stop, args=("deadline",))
        timer.daemon = True
        timer.start()
        log.info(f"Ngân sách thời gian lượt này: {MAX_RUNTIME_SEC/3600:.2f} giờ")

    uploader = Uploader() if STORAGE_MODE == "rclone" else None
    if uploader:
        uploader.start()
    outages, code = 0, 0
    try:
        while True:
            statuses = load_all_status()
            write_progress(targets, statuses)
            todo = [g for g in gids if not is_finished(statuses.get(g))]
            if not todo or STOP_EVENT.is_set():
                break
            log.info(f"Còn {len(todo):,} xã cần xử lý")
            result = run_round(todo, statuses, len(gids))
            if result == "stop":
                break
            if result == "outage":
                outages += 1
                if outages >= MAX_OUTAGES:
                    log.error("Sự cố kéo dài, thoát mã 2.")
                    code = 2
                    break
                STOP_EVENT.wait(OUTAGE_SLEEP_SEC)    # nghỉ, nhưng vẫn nhận tín hiệu dừng
            else:
                outages = 0
    except KeyboardInterrupt:                        # notebook hoặc terminal không cài bộ bắt tín hiệu
        log.warning("Đã dừng tay. Chạy lại để tiếp tục từ chỗ dừng.")
        code = 130
    finally:
        if uploader:
            uploader.stop_event.set()
            uploader.join(timeout=900)

    statuses = load_all_status()
    progress = write_progress(targets, statuses)
    unfinished = [g for g in gids if not is_finished(statuses.get(g))]

    if not unfinished:
        if STORAGE_MODE == "rclone":
            # Máy mới hoặc ổ đã dọn: kéo CSV của các xã từ Drive về trước khi gộp,
            # tránh ghi file gộp rỗng đè lên bản đúng trên Drive.
            missing = [st for st in statuses.values() if st.get("rel_dir")
                       and not os.path.isdir(os.path.join(LOCAL_ROOT, st["rel_dir"], "CSV"))]
            if missing:
                log.info(f"Kéo CSV của {len(missing):,} xã từ Drive về để gộp...")
                _rclone(["copy", f"{REMOTE_BASE}/{SUB_PROV}", WORK_PROV_ROOT,
                         "--filter", "+ *.csv", "--filter", "- *", *RCLONE_COMMON])
        build_combined({g: statuses[g] for g in gids if g in statuses})
        if STORAGE_MODE == "rclone":
            log.info("Đồng bộ lượt cuối lên Drive...")
            rclone_sync_once(final=True)
        log.info(f"HOÀN TẤT TOÀN BỘ: {progress['status'].value_counts().to_dict()}")
        return 0

    if STORAGE_MODE == "rclone":
        log.info("Đồng bộ phần đã làm lên Drive trước khi thoát...")
        rclone_sync_once(final=True)
    log.info(f"Chưa xong: còn {len(unfinished):,} xã | {progress['status'].value_counts().to_dict()}")
    if code:
        return code
    return 3 if STOP_REASON[0] == "deadline" else 130

if __name__ == "__main__" and os.environ.get("VNGIS_SKIP_MAIN") != "1":
    if "--sync-only" in sys.argv:
        # Dùng ở bước cuối của workflow: đẩy nốt những gì còn trên máy lên Drive.
        setup_logging()
        if STORAGE_MODE == "rclone":
            rclone_sync_once(final=True)
            log.info("Đồng bộ nốt xong.")
        sys.exit(0)
    exit_code = main()
    if not _in_notebook():
        sys.exit(exit_code)
