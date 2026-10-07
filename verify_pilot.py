# -*- coding: utf-8 -*-
"""
Kiểm tra kết quả chạy thí điểm (và đối chiếu với notebook nếu có file của notebook).

Cách dùng:
  # Trên GitHub Actions hoặc máy có rclone:
  python verify_pilot.py --remote gdrive:VNGISDash_PILOT_2024
  # Trên Colab đã mount Drive:
  python verify_pilot.py --root /content/drive/MyDrive/VNGISDash_PILOT_2024
  # Đối chiếu với file do notebook tạo cho 1 xã (đặt chung một thư mục):
  python verify_pilot.py --root ... --notebook-dir /content/nb_out --notebook-gid VNM.4.1.10_1

Mã thoát: 0 nếu không có FAIL, 1 nếu có ít nhất một FAIL.
"""
import argparse, glob, json, os, subprocess, sys, tempfile

import numpy as np
import pandas as pd

M_PER_DEG = 111319.49            # EE quy đổi scale (m) sang độ ở EPSG:4326
T1_FEATS = [f"{b}_{s}" for b in ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "NDVI", "NDBI", "MNDWI", "BSI"]
            for s in ("mean", "stdDev")]
T3_COLS = ["GID_1", "NAME_1", "GID_3", "NAME_3", "YEAR", "MONTH", "TIME", "COMMUNE_AREA_HA", "TNL", "MEAN_RAD",
           "STD_RAD", "MIN_RAD", "MAX_RAD", "SPATIAL_CV", "LIT_PIXELS", "LIT_AREA_HA", "ELECTRIFICATION_RATIO_PCT",
           "LIT_POP_PROXY", "CLOUD_FREE_OBS", "TNL_MA3", "TNL_MOM_GROWTH_PCT"]


def load_status(root):
    out = {}
    for p in sorted(glob.glob(os.path.join(root, "_control", "status", "status_*.jsonl"))):
        with open(p, encoding="utf-8") as f:
            for line in f:
                try:
                    d = json.loads(line)
                    out[d["gid_3"]] = d
                except Exception:
                    pass
    return out


def check_tif(path, bands, scale_m, dtype=None):
    import rasterio
    with rasterio.open(path) as s:
        if s.count != bands:
            return "FAIL", f"{s.count} kênh (cần {bands})"
        if s.crs is None or s.crs.to_epsg() != 4326:
            return "FAIL", f"CRS {s.crs}"
        res_m = abs(s.transform.a) * M_PER_DEG
        if abs(res_m - scale_m) > 0.01 * scale_m:
            return "FAIL", f"pixel {res_m:.2f} m (cần {scale_m} m)"
        if dtype and s.dtypes[0] != dtype:
            return "FAIL", f"kiểu {s.dtypes[0]} (cần {dtype})"
        a = s.read(masked=True).astype("float64")
        a = np.ma.masked_invalid(a)
        if a.count() == 0 or not np.any(a.filled(0) != 0):
            return "WARN", "toàn NoData/0 (tháng mây phủ kín?)"
    return "PASS", ""


def worst(*levels):
    for lv in ("FAIL", "WARN"):
        if lv in levels:
            return lv
    return "PASS"


def verify_commune(root, gid, st):
    safe = gid.replace(".", "_")
    res = []          # (mục, kết quả, ghi chú)
    status = (st or {}).get("status", "không có bản ghi")
    res.append(("Trạng thái", "PASS" if status == "done" else "FAIL", status))

    t2 = (st or {}).get("t2") or {}
    none_m = sorted(k for k, v in t2.items() if v == "none")
    day = sorted(glob.glob(os.path.join(root, "2_Task2_Day_S2", "*", f"*_{safe}_*", f"*_{safe}_2024??.tif")))
    need = 12 - len(none_m)
    lv, notes = ("PASS" if len(day) == need else "FAIL"), [f"{len(day)}/{need} file"]
    if none_m:
        notes.append(f"tháng không có ảnh kể cả khi nới biên: {','.join(none_m)}")
    empties = []
    for p in day:
        r, n = check_tif(p, 10, 20)
        if r == "FAIL":
            lv = "FAIL"; notes.append(f"{os.path.basename(p)}: {n}")
        elif r == "WARN":
            empties.append(p[-10:-4])
    if empties:
        lv = worst(lv, "WARN" if len(empties) < len(day) else "FAIL")
        notes.append(f"tháng toàn NoData: {','.join(empties)}")
    res.append(("Task 2 ảnh ngày (10 kênh, 20 m)", lv, "; ".join(notes)))

    night = sorted(glob.glob(os.path.join(root, "3_Task3_Night_VIIRS", "*", f"*_{safe}_*", f"*_{safe}_2024??.tif")))
    t3 = (st or {}).get("t3img") or {}
    need3 = 12 - sum(v == "none" for v in t3.values())
    lv, notes = ("PASS" if len(night) == need3 else "FAIL"), [f"{len(night)}/{need3} file"]
    for p in night:
        r, n = check_tif(p, 2, 500, "float64")
        if r != "PASS":
            lv = worst(lv, r); notes.append(f"{os.path.basename(p)}: {n}")
    res.append(("Task 3.1 ảnh đêm (2 kênh, 500 m, Float64)", lv, "; ".join(notes)))

    f1 = glob.glob(os.path.join(root, "1_Task1_Spectral_Indices", "*", f"s2_*_{safe}_2024_Spectral_Indices.csv"))
    if len(f1) != 1:
        res.append(("Task 1 CSV", "FAIL", f"tìm thấy {len(f1)} file"))
    else:
        d = pd.read_csv(f1[0])
        miss = [c for c in T1_FEATS if c not in d.columns]
        allnan = [c for c in T1_FEATS if c in d.columns and d[c].isna().all()]
        lv = "FAIL" if (miss or allnan or len(d) == 0 or len(d) > 12 or d["MONTH"].duplicated().any()) else "PASS"
        if lv == "PASS" and d[T1_FEATS].isna().any(axis=1).any():
            lv = "WARN"
        res.append(("Task 1 CSV (20 cột đặc trưng)", lv,
                    f"{len(d)} tháng" + (f"; thiếu cột {miss}" if miss else "") + (f"; cột rỗng {allnan}" if allnan else "")))

    f3 = glob.glob(os.path.join(root, "4_Task3_Economic_Indices", "*", f"*_{safe}_202401-202412_Economic_Indices.csv"))
    if len(f3) != 1:
        res.append(("Task 3.2 CSV", "FAIL", f"tìm thấy {len(f3)} file"))
    else:
        d = pd.read_csv(f3[0])
        ok_cols = list(d.columns) == T3_COLS
        lv = "PASS" if ok_cols and len(d) == need3 else "FAIL"
        res.append(("Task 3.2 CSV (21 cột như notebook)", lv,
                    f"{len(d)} dòng" + ("" if ok_cols else f"; cột lệch: {list(d.columns)}")))

    mb = sum(os.path.getsize(p) for p in day + night) / 1e6
    sec = (st or {}).get("seconds")
    res.append(("Dung lượng và thời gian", "PASS",
                f"{mb:.1f} MB ảnh; {sec if sec is not None else '?'} giây ở lần xử lý cuối"))
    return res


def _cmp_csv(a, b, key, tol):
    a, b = a.sort_values(key).reset_index(drop=True), b.sort_values(key).reset_index(drop=True)
    if len(a) != len(b):
        return "FAIL", f"số dòng {len(a)} vs {len(b)}"
    cols = [c for c in a.columns if c in b.columns and pd.api.types.is_numeric_dtype(a[c])
            and pd.api.types.is_numeric_dtype(b[c])]
    diffs = {c: float(np.nanmax(np.abs(a[c].astype(float) - b[c].astype(float)))) if len(a) else 0.0 for c in cols}
    nan_mismatch = [c for c in cols if not (a[c].isna() == b[c].isna()).all()]
    bad = {c: d for c, d in diffs.items() if d > tol}
    if bad or nan_mismatch:
        return "FAIL", f"lệch {bad} NaN khác ở {nan_mismatch}"
    return "PASS", f"{len(cols)} cột số, sai số lớn nhất {max(diffs.values()) if diffs else 0:.2e}"


def _cmp_tif(p_pipe, p_nb):
    import rasterio
    with rasterio.open(p_pipe) as s1, rasterio.open(p_nb) as s2:
        a1, a2 = s1.read().astype("float64"), s2.read().astype("float64")
        t1, t2 = s1.transform, s2.transform
        n1, n2 = s1.nodata, s2.nodata
    if a1.shape[0] != a2.shape[0]:
        return "FAIL", f"số kênh {a1.shape[0]} vs {a2.shape[0]}"
    if abs(t1.a - t2.a) > 1e-12 or abs(t1.e - t2.e) > 1e-12:
        return "FAIL", "khác kích thước pixel"
    dc, dr = (t2.c - t1.c) / t1.a, (t2.f - t1.f) / t1.e
    if abs(dc - round(dc)) > 1e-3 or abs(dr - round(dr)) > 1e-3:
        return "FAIL", f"lệch lưới pixel ({dc:.3f}, {dr:.3f})"
    dc, dr = int(round(dc)), int(round(dr))
    r0, c0 = max(0, dr), max(0, dc)
    r1, c1 = min(a1.shape[1], dr + a2.shape[1]), min(a1.shape[2], dc + a2.shape[2])
    w1 = a1[:, r0:r1, c0:c1]
    w2 = a2[:, r0 - dr:r1 - dr, c0 - dc:c1 - dc]
    for arr, nd in ((w1, n1), (w2, n2)):
        if nd is not None:
            arr[arr == nd] = np.nan
    both = ~np.isnan(w1) & ~np.isnan(w2)
    if not both.any():
        return "FAIL", "không có pixel chung"
    diff = float(np.max(np.abs(w1[both] - w2[both])))
    only = int((np.isnan(w1) ^ np.isnan(w2)).sum())
    lv = "PASS" if diff == 0 and only == 0 else ("WARN" if diff == 0 else "FAIL")
    return lv, (f"{int(both.sum()):,} giá trị chung, lệch lớn nhất {diff:.3g}, "
                f"{only} pixel chỉ có giá trị ở một bên; khung {a1.shape[1:]} vs {a2.shape[1:]}")


def compare_notebook(root, nb_dir, gid):
    safe = gid.replace(".", "_")
    out = []
    pairs = [
        ("Đối chiếu Task 1 CSV", "1_Task1_Spectral_Indices", f"s2_*_{safe}_2024_Spectral_Indices.csv", "csv", "MONTH"),
        ("Đối chiếu Task 3.2 CSV", "4_Task3_Economic_Indices", f"*_{safe}_202401-202412_Economic_Indices.csv", "csv", "MONTH"),
    ]
    for label, sub, pat, _k, key in pairs:
        mine = glob.glob(os.path.join(root, sub, "*", pat))
        ref = glob.glob(os.path.join(nb_dir, pat))
        if not mine or not ref:
            out.append((label, "WARN", "thiếu file để so (pipeline hoặc notebook)"))
            continue
        out.append((label, *_cmp_csv(pd.read_csv(mine[0]), pd.read_csv(ref[0]), key, 1e-6)))
    for p_nb in sorted(glob.glob(os.path.join(nb_dir, f"*_{safe}_2024??.tif"))):
        name = os.path.basename(p_nb)
        sub = "2_Task2_Day_S2" if name.startswith("S2_Day") else "3_Task3_Night_VIIRS"
        mine = glob.glob(os.path.join(root, sub, "*", "*", name))
        if not mine:
            out.append((f"Đối chiếu {name}", "WARN", "pipeline không có file cùng tên"))
            continue
        out.append((f"Đối chiếu {name}", *_cmp_tif(mine[0], p_nb)))
    if not out:
        out.append(("Đối chiếu notebook", "WARN", f"không tìm thấy file notebook của {gid} trong {nb_dir}"))
    return out


def _short(note, limit=3):
    parts = note.replace("|", "/").split("; ")
    if len(parts) > limit + 1:
        parts = parts[:limit + 1] + [f"... và {len(parts) - limit - 1} mục khác"]
    return "; ".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", help="thư mục pilot trên máy (vd. Drive đã mount)")
    ap.add_argument("--remote", help="đường dẫn rclone, vd. gdrive:VNGISDash_PILOT_2024")
    ap.add_argument("--gids", default="", help="danh sách GID_3, mặc định: mọi xã có trong trạng thái")
    ap.add_argument("--notebook-dir", default="")
    ap.add_argument("--notebook-gid", default="")
    ap.add_argument("--report", default="")
    a = ap.parse_args()

    root = a.root
    if a.remote:
        root = tempfile.mkdtemp(prefix="vngis_verify_")
        print(f"Kéo {a.remote} về {root} ...", file=sys.stderr)
        subprocess.run(["rclone", "copy", a.remote, root, "--exclude", "_control/logs/**"], check=True)
    if not root or not os.path.isdir(root):
        sys.exit("Cần --root hoặc --remote hợp lệ.")

    st = load_status(root)
    gids = [g.strip() for g in a.gids.split(",") if g.strip()] or sorted(st)
    lines = ["## Kết quả kiểm tra thí điểm VNGISDash 2024", ""]
    any_fail = False
    for gid in gids:
        rows = verify_commune(root, gid, st.get(gid))
        if a.notebook_dir and (not a.notebook_gid or a.notebook_gid == gid):
            rows += compare_notebook(root, a.notebook_dir, gid)
        overall = worst(*[r[1] for r in rows])
        any_fail |= overall == "FAIL"
        lines += [f"### {gid}: **{overall}**", "", "| Mục | Kết quả | Ghi chú |", "|---|---|---|"]
        lines += [f"| {m} | {r} | {_short(n)} |" for m, r, n in rows]
        lines.append("")
    lines.append("**Kết luận: " + ("CÓ MỤC FAIL, chưa chạy toàn quốc.**" if any_fail else
                                  "ĐẠT. Có thể chạy toàn quốc (xem các dòng WARN nếu có).**"))
    text = "\n".join(lines)
    print(text)
    report = a.report or os.path.join(root, "_control", "verify_report.md")
    os.makedirs(os.path.dirname(report), exist_ok=True)
    with open(report, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    if a.remote:
        subprocess.run(["rclone", "copyto", report, f"{a.remote}/_control/verify_report.md"])
    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()
