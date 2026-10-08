# -*- coding: utf-8 -*-
"""Gửi tác vụ Export lên Earth Engine bằng tài khoản của bạn (chạy trong Google Colab).

Earth Engine tự tính trên máy chủ rồi ghi thẳng vào Drive (thư mục VNGIS_EXPORT_2024), không tốn
lượt gọi tương tác nên không bị lỗi 429. Công thức, tham số giữ đúng notebook.

Dùng trong Colab (xem HUONG_DAN_BATCH.md):
    import colab_export as cx
    cx.init()                    # đăng nhập, mount Drive
    cx.submit_communes()         # 1 tác vụ: ranh giới xã (để cắt ảnh)
    cx.compute_day_plan()        # ~63 lệnh gọi nhỏ: chọn cửa sổ thời gian từng xã, từng tháng
    cx.submit_day_csv()          # 63 tác vụ: CSV chỉ số ảnh ngày theo tỉnh
    cx.submit_day_images()       # ~12 tác vụ: ảnh ngày toàn quốc theo tháng
    cx.status()                  # xem tiến độ
    # Sau khi phần ngày xong:
    cx.submit_night_csv(); cx.submit_night_images()
"""

import os, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import ee

import vngis_2024 as V
import batch_config as B

YEAR = V.YEAR
MONTHS = V.MONTHS
DRIVE_DIR = f"/content/drive/MyDrive/{B.EXPORT_FOLDER}"
COMM = None
ADMIN = None


# ------------------------------------------------------------------------------------
# Khởi tạo
# ------------------------------------------------------------------------------------
def init(project=B.PROJECT_ID, mount_drive=True):
    global COMM, ADMIN
    try:
        ee.Initialize(project=project)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=project)
    if mount_drive:
        try:
            from google.colab import drive
            drive.mount("/content/drive")
        except ImportError:
            print("Không chạy trong Colab: bỏ qua mount Drive, plan_day.json ghi ở thư mục hiện tại.")
    os.makedirs(_out_dir(), exist_ok=True)
    COMM = ee.FeatureCollection(B.ASSET_ID)
    V.CACHE_DIR = os.path.expanduser("~/vngis_cache")
    ADMIN = V.build_admin_table()
    n_asset = COMM.size().getInfo()
    print(f"Earth Engine: project {project} | asset {B.ASSET_ID}: {n_asset:,} xã | GADM: {len(ADMIN):,} xã, "
          f"{ADMIN['GID_1'].nunique()} tỉnh")


def _out_dir():
    return DRIVE_DIR if os.path.isdir("/content/drive/MyDrive") else os.path.abspath(B.EXPORT_FOLDER)


def provinces():
    out = {}
    for r in ADMIN.itertuples():
        out.setdefault(r.GID_1, []).append(r.GID_3)
    return dict(sorted(out.items(), key=lambda kv: V.natural_sort_key(kv[0])))


def _prov_fc(gids):
    return COMM.filter(ee.Filter.inList("GID_3", ee.List(gids)))


# ------------------------------------------------------------------------------------
# Quản lý tác vụ: không gửi trùng tác vụ đang chờ, đang chạy hoặc đã xong
# ------------------------------------------------------------------------------------
_TASKS_CACHE = {"t": 0, "map": {}}


def _existing_tasks(refresh=False):
    if refresh or time.time() - _TASKS_CACHE["t"] > 60:
        m = {}
        for t in ee.data.getTaskList():
            d = t.get("description")
            st = t.get("state")
            if d and (d not in m or st in ("READY", "RUNNING", "COMPLETED")):
                m[d] = st
        _TASKS_CACHE.update(t=time.time(), map=m)
    return _TASKS_CACHE["map"]


def _start(task, desc, force=False):
    st = _existing_tasks().get(desc)
    if st in ("READY", "RUNNING", "COMPLETED") and not force:
        return f"bỏ qua ({st})"
    task.start()
    _existing_tasks()[desc] = "READY"
    return "đã gửi"


def status(prefix=None):
    """In số tác vụ theo trạng thái và các tác vụ lỗi."""
    tasks = ee.data.getTaskList()
    if prefix:
        tasks = [t for t in tasks if t.get("description", "").startswith(prefix)]
    latest = {}
    for t in tasks:                                 # getTaskList trả mới nhất trước
        latest.setdefault(t.get("description"), t)
    groups = {}
    for d, t in latest.items():
        kind = "_".join(d.split("_")[:2]) if d else "?"
        groups.setdefault(kind, {}).setdefault(t["state"], 0)
        groups[kind][t["state"]] += 1
    for k, v in sorted(groups.items()):
        print(f"{k:12s} {v}")
    bad = [t for t in latest.values() if t["state"] == "FAILED"]
    for t in bad[:20]:
        print("LỖI:", t["description"], "|", t.get("error_message", "")[:300])
    return latest


# ------------------------------------------------------------------------------------
# A. Ranh giới xã (để cắt ảnh trên GitHub)
# ------------------------------------------------------------------------------------
def submit_communes():
    task = ee.batch.Export.table.toDrive(collection=COMM.select(["GID_3"]), description="communes_l3",
                                         folder=B.EXPORT_FOLDER, fileNamePrefix=B.COMMUNES_GEOJSON,
                                         fileFormat="GeoJSON")
    print("communes_l3:", _start(task, "communes_l3"))


# ------------------------------------------------------------------------------------
# B. Kế hoạch ảnh ngày: số cảnh của 3 cửa sổ (tháng, ±15, ±30 ngày) cho từng xã, từng tháng
#    = đúng các câu lệnh if của get_adaptive_monthly_composite (notebook cell 18)
# ------------------------------------------------------------------------------------
def _plan_fc(fc):
    cols = []
    for m in MONTHS:
        for s, e in V._s2_windows(YEAR, m):
            # lọc theo tỉnh trước cho nhanh; cảnh giao với xã thì chắc chắn giao với tỉnh nên kết quả không đổi
            cols.append(ee.ImageCollection(V.S2_COLLECTION).filterDate(s, e).filterBounds(fc))

    def f(feat):
        g = feat.geometry()
        return ee.Feature(None, {"GID_3": feat.get("GID_3"), "c": ee.List([c.filterBounds(g).size() for c in cols])})
    return fc.map(f)


def _plan_one(gids, depth=0):
    try:
        feats = _plan_fc(_prov_fc(gids)).getInfo()["features"]
        out = {}
        for ft in feats:
            c = ft["properties"]["c"]
            out[ft["properties"]["GID_3"]] = [c[i:i + 3] for i in range(0, 36, 3)]
        return out
    except Exception as exc:
        if depth >= 3 or len(gids) < 2:
            raise
        if "429" in str(exc) or "oo many" in str(exc):
            time.sleep(30)
            return _plan_one(gids, depth + 1)
        half = len(gids) // 2                       # tỉnh lớn quá hạn thời gian: chia đôi
        a = _plan_one(gids[:half], depth + 1)
        a.update(_plan_one(gids[half:], depth + 1))
        return a


def compute_day_plan(workers=4, only=None):
    path = os.path.join(_out_dir(), B.PLAN_DAY)
    plan = json.load(open(path, encoding="utf-8")) if os.path.isfile(path) else {}
    provs = provinces()
    todo = {g1: [g for g in gids if g not in plan] for g1, gids in provs.items() if not only or g1 in only}
    todo = {k: v for k, v in todo.items() if v}
    print(f"Kế hoạch ảnh ngày: đã có {len(plan):,} xã, cần tính {sum(map(len, todo.values())):,} xã ở {len(todo)} tỉnh")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_plan_one, gids): g1 for g1, gids in todo.items()}
        for i, fut in enumerate(as_completed(futs), 1):
            g1 = futs[fut]
            try:
                plan.update(fut.result())
                print(f"  [{i}/{len(todo)}] {g1}: xong")
            except Exception as exc:
                print(f"  [{i}/{len(todo)}] {g1}: LỖI {str(exc)[:200]} (chạy lại compute_day_plan để thử tiếp)")
            if i % 5 == 0 or i == len(todo):
                _save_json(plan, path)
    missing = [g for g in ADMIN["GID_3"] if g not in plan]
    print(f"Xong: {len(plan):,} xã có kế hoạch; {len(missing):,} xã chưa có (không có trong asset hoặc lỗi).")
    summ = {}
    for g, rows in plan.items():
        for m, c in zip(MONTHS, rows):
            k = B.window_class(c)
            summ[k] = summ.get(k, 0) + 1
    print("Số (xã, tháng) theo cửa sổ: tháng=", summ.get(0, 0), "| ±15 ngày=", summ.get(1, 0),
          "| ±30 ngày=", summ.get(2, 0), "| không có ảnh=", summ.get(None, 0))
    return plan


def _save_json(obj, path):
    with open(path + ".part", "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(path + ".part", path)


def load_plan():
    return json.load(open(os.path.join(_out_dir(), B.PLAN_DAY), encoding="utf-8"))


# ------------------------------------------------------------------------------------
# C. CSV chỉ số ảnh ngày (Task 1) theo tỉnh, đúng export_province_s2_local của notebook cell 15
# ------------------------------------------------------------------------------------
def _task1_fc(fc):
    """Giống V.task1_all_months nhưng cho cả tỉnh (đúng phạm vi notebook: lọc mây, nhánh 'rỗng thì dùng toàn bộ
    cảnh' và median tính trên bộ ảnh của tỉnh)."""
    bands = V.DAY_BANDS_ALL
    reducers = ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True)
    selected_cols = ["GID_3"] + V.T1_FEATURES
    per_month = []
    for m in MONTHS:
        s_date, e_date = V._month_dates(YEAR, m)
        raw_col = ee.ImageCollection(V.S2_COLLECTION).filterBounds(fc).filterDate(s_date, e_date)
        filtered_col = raw_col.filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 85))
        composite = ee.Image(ee.Algorithms.If(filtered_col.size().eq(0),
                                              raw_col.map(V.mask_s2_sr).median(),
                                              filtered_col.map(V.mask_s2_sr).median()))
        tensor = V.add_indices(composite)
        stats = tensor.select(bands).reduceRegions(
            collection=fc, reducer=reducers, scale=50, tileScale=4, crs="EPSG:4326")
        stats = stats.select(selected_cols).map(lambda f, m=m: f.set("MONTH", m))
        per_month.append(ee.FeatureCollection(ee.Algorithms.If(raw_col.size().gt(0), stats,
                                                               ee.FeatureCollection([]))))
    return ee.FeatureCollection(per_month).flatten()


def submit_day_csv(only=None, force=False):
    n = 0
    for g1, gids in provinces().items():
        if only and g1 not in only:
            continue
        desc = B.day_csv_prefix(g1)
        task = ee.batch.Export.table.toDrive(
            collection=_task1_fc(_prov_fc(gids)), description=desc, folder=B.EXPORT_FOLDER,
            fileNamePrefix=desc, fileFormat="CSV", selectors=["GID_3", "MONTH"] + V.T1_FEATURES)
        r = _start(task, desc, force)
        n += r == "đã gửi"
        print(f"{desc}: {r}")
    print(f"Đã gửi {n} tác vụ CSV ảnh ngày.")


# ------------------------------------------------------------------------------------
# D. Ảnh ngày (Task 2) toàn quốc theo tháng. Mỗi xã dùng đúng cửa sổ thời gian của nó (theo plan):
#    nhóm cửa sổ "tháng" xuất 1 ảnh toàn quốc; các xã cần ±15/±30 ngày (hiếm) xuất thêm ảnh riêng.
#    Giá trị pixel bên trong xã giống hệt add_indices(get_adaptive_monthly_composite(...)) của notebook,
#    vì median từng pixel chỉ dùng các cảnh phủ pixel đó.
# ------------------------------------------------------------------------------------
def _day_image(fc, month, window, region):
    s, e = V._s2_windows(YEAR, month)[window]
    # Lọc theo khung bao: cảnh nào phủ pixel của xã thì chắc chắn giao khung bao, nên median từng pixel không đổi
    col = ee.ImageCollection(V.S2_COLLECTION).filterDate(s, e).filterBounds(region)
    img = V.add_indices(col.map(V.mask_s2_clean).median()).select(V.DAY_BANDS_ALL)
    # Chỉ tính trong phạm vi các xã (nới 1 pixel ra ngoài ranh giới để không mất pixel viền)
    keep = (ee.Image(0).byte().paint(fc, 1).reproject(crs="EPSG:4326", crsTransform=B.T20)
            .focalMax(radius=1, kernelType="square", units="pixels"))
    return img.updateMask(keep)


def submit_day_images(months=None, force=False):
    plan = load_plan()
    months = months or MONTHS
    for m in months:
        groups = {}
        for g, rows in plan.items():
            k = B.window_class(rows[m - 1])
            if k is not None:
                groups.setdefault(k, []).append(g)
        for w, gids in sorted(groups.items()):
            desc = B.day_img_prefix(m, w)
            fc = _prov_fc(gids)
            region = (ee.Geometry.Rectangle(B.VN_BBOX, "EPSG:4326", False) if w == 0
                      else fc.geometry().bounds(maxError=1))
            task = ee.batch.Export.image.toDrive(
                image=_day_image(fc, m, w, region), description=desc, folder=B.EXPORT_FOLDER, fileNamePrefix=desc,
                region=region, crs="EPSG:4326", crsTransform=B.T20, maxPixels=1e13,
                fileDimensions=B.FILE_DIMENSIONS, fileFormat="GeoTIFF")
            print(f"{desc}: {len(gids):,} xã | {_start(task, desc, force)}")


# ------------------------------------------------------------------------------------
# E. Ảnh đêm (Task 3.1) toàn quốc theo tháng: get_viirs_monthly_composite + toDouble (notebook cell 34, 36)
# ------------------------------------------------------------------------------------
def _viirs_col(m):
    s_date, e_date = V._month_dates(YEAR, m)
    ca = ee.ImageCollection(V.VIIRS_A).filterDate(s_date, e_date)
    cb = ee.ImageCollection(V.VIIRS_B).filterDate(s_date, e_date)
    return ee.ImageCollection(ee.Algorithms.If(ca.size().eq(0), cb, ca))


def submit_night_images(months=None, force=False):
    region = ee.Geometry.Rectangle(B.VN_BBOX, "EPSG:4326", False)
    for m in months or MONTHS:
        desc = B.night_img_prefix(m)
        img = _viirs_col(m).select(["avg_rad", "cf_cvg"]).mean().toDouble()
        task = ee.batch.Export.image.toDrive(
            image=img, description=desc, folder=B.EXPORT_FOLDER, fileNamePrefix=desc, region=region,
            crs="EPSG:4326", crsTransform=B.T500, maxPixels=1e13, fileFormat="GeoTIFF")
        print(f"{desc}: {_start(task, desc, force)}")


# ------------------------------------------------------------------------------------
# F. CSV chỉ số ảnh đêm (Task 3.2) theo tỉnh: mỗi xã clip + 4 phép reduceRegion như notebook cell 38.
#    Phần tính CV, MA3, tăng trưởng làm ở process_exports.py (chép nguyên văn notebook).
# ------------------------------------------------------------------------------------
NIGHT_KEYS = ["avg_rad_sum", "avg_rad_mean", "avg_rad_stdDev", "avg_rad_min", "avg_rad_max", "avg_rad_count",
              "lit_rad", "is_lit", "cf_cvg"]


def _suffixer(mm):
    def f(k):                                       # đúng 1 tham số: ee.List.map đếm số tham số của hàm
        return ee.String(k).cat("_" + mm)
    return f


def _task3_fc(fc):
    reducers = (ee.Reducer.sum().combine(ee.Reducer.mean(), sharedInputs=True)
                .combine(ee.Reducer.stdDev(), sharedInputs=True).combine(ee.Reducer.min(), sharedInputs=True)
                .combine(ee.Reducer.max(), sharedInputs=True).combine(ee.Reducer.count(), sharedInputs=True))
    cols = [(m, _viirs_col(m)) for m in MONTHS]

    def per(feat):
        g = feat.geometry()
        d = ee.Dictionary({"GID_3": feat.get("GID_3"), "area_ha": g.area(maxError=1).divide(10000)})
        for m, col in cols:
            mm = f"{m:02d}"
            img = col.mean().clip(g)
            rad = img.select("avg_rad")
            cf_cvg = img.select("cf_cvg")
            lit_mask = rad.gte(1.5).rename("is_lit")
            lit_rad = rad.updateMask(lit_mask).rename("lit_rad")
            kw = dict(geometry=g, scale=500, maxPixels=1e9, crs="EPSG:4326")
            vals = (rad.reduceRegion(reducer=reducers, **kw)
                    .combine(lit_rad.reduceRegion(reducer=ee.Reducer.sum(), **kw))
                    .combine(lit_mask.reduceRegion(reducer=ee.Reducer.sum(), **kw))
                    .combine(cf_cvg.reduceRegion(reducer=ee.Reducer.mean(), **kw)))
            keys = vals.keys()
            vals = vals.rename(keys, keys.map(_suffixer(mm)))
            d = d.set(f"n_{mm}", col.size())
            d = ee.Dictionary(ee.Algorithms.If(col.size().gt(0), d.combine(vals), d))
        return ee.Feature(None, d)
    return fc.map(per)


def submit_night_csv(only=None, force=False):
    sel = ["GID_3", "area_ha"] + [f"n_{m:02d}" for m in MONTHS] + \
          [f"{k}_{m:02d}" for m in MONTHS for k in NIGHT_KEYS]
    n = 0
    for g1, gids in provinces().items():
        if only and g1 not in only:
            continue
        desc = B.night_csv_prefix(g1)
        task = ee.batch.Export.table.toDrive(collection=_task3_fc(_prov_fc(gids)), description=desc,
                                             folder=B.EXPORT_FOLDER, fileNamePrefix=desc, fileFormat="CSV",
                                             selectors=sel)
        r = _start(task, desc, force)
        n += r == "đã gửi"
        print(f"{desc}: {r}")
    print(f"Đã gửi {n} tác vụ CSV ảnh đêm.")


# ------------------------------------------------------------------------------------
# Thử nhanh 1 tỉnh trước khi gửi toàn bộ
# ------------------------------------------------------------------------------------
def quick_check(gid1=None):
    """Tính trực tiếp (không export) vài con số của 1 tỉnh để chắc công thức chạy được."""
    provs = provinces()
    gid1 = gid1 or next(iter(provs))
    gids = provs[gid1][:3]
    fc = _prov_fc(gids)
    t1 = _task1_fc(fc).limit(3).getInfo()["features"]
    t3 = _task3_fc(fc.limit(1)).getInfo()["features"]
    print(f"{gid1}: Task1 mẫu: {[f['properties'] for f in t1][:1]}")
    print(f"{gid1}: Task3.2 mẫu (tháng 01): "
          f"{ {k: v for k, v in t3[0]['properties'].items() if k.endswith('_01') or k in ('GID_3', 'area_ha')} }")
