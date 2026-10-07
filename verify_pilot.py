# -*- coding: utf-8 -*-
"""
Kiểm tra kết quả chạy thí điểm (và đối chiếu với notebook nếu có file của notebook).

  python verify_pilot.py --remote gdrive:VNGISDash_2024_PILOT          (GitHub Actions / máy có rclone)
  python verify_pilot.py --root /content/drive/MyDrive/VNGISDash_2024_PILOT   (Colab đã mount Drive)
  # Đối chiếu với file notebook của 1 xã (đặt chung 1 thư mục):
  python verify_pilot.py --root ... --notebook-dir /content/nb_out --notebook-gid VNM.4.1.10_1

Mã thoát: 0 nếu không có FAIL, 1 nếu có FAIL.
"""
import argparse, glob, json, os, subprocess, sys, tempfile

import numpy as np
import pandas as pd

M_PER_DEG = 111319.49
DAY_BANDS_ALL = ["BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "NDVI", "NDBI", "MNDWI", "BSI"]
T1_FEATS = [f"{b}_{s}" for b in DAY_BANDS_ALL for s in ("mean", "stdDev")]
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


def read_scaled(path):
    """Đọc tif, áp scale (ảnh ngày int16) và NoData; trả mảng float64 với NaN ở NoData."""
    import rasterio
    with rasterio.open(path) as s:
        a = s.read().astype("float64")
        if s.nodata is not None:
            a[a == s.nodata] = np.nan
        a *= np.array(s.scales, dtype="float64")[:, None, None]
        return a, s.transform, s


def check_tif(path, bands, scale_m, dtype):
    import rasterio
    with rasterio.open(path) as s:
        if s.count != bands:
            return "FAIL", f"{s.count} kênh (cần {bands})"
        if s.crs is None or s.crs.to_epsg() != 4326:
            return "FAIL", f"CRS {s.crs}"
        res_m = abs(s.transform.a) * M_PER_DEG
        if abs(res_m - scale_m) > 0.01 * scale_m:
            return "FAIL", f"pixel {res_m:.2f} m (cần {scale_m} m)"
        if s.dtypes[0] != dtype:
            return "FAIL", f"kiểu {s.dtypes[0]} (cần {dtype})"
    a, _, _ = read_scaled(path)
    if np.all(np.isnan(a)) or not np.any(np.nan_to_num(a) != 0):
        return "WARN", "toàn NoData/0 (tháng mây phủ kín?)"
    return "PASS", ""


def worst(*levels):
    for lv in ("FAIL", "WARN"):
        if lv in levels:
            return lv
    return "PASS"


def verify_commune(root, gid, st, day_csv, night_csv):
    safe = gid.replace(".", "_")
    st = st or {}
    res = [("Trạng thái", "PASS" if st.get("status") == "done" else "FAIL", st.get("status", "không có bản ghi"))]
    day_bands = int(st.get("day_bands", 6))

    for kind, sub, key, bands, scale, dtype in (("Ảnh ngày", "Day", "t2", day_bands, 20, "int16"),
                                                ("Ảnh đêm", "Night", "t3img", 2, 500, "float64")):
        files = sorted(glob.glob(os.path.join(root, sub, "*", f"{gid}_*", f"{safe}_{sub.lower()}_2024??.tif")))
        none_m = sorted(k for k, v in (st.get(key) or {}).items() if v == "none")
        need = 12 - len(none_m)
        lv, notes = ("PASS" if len(files) == need else "FAIL"), [f"{len(files)}/{need} file"]
        if none_m:
            notes.append(f"tháng không có ảnh: {','.join(none_m)}")
        empties, sizes = [], []
        for p in files:
            sizes.append(os.path.getsize(p) / 1024)
            r, n = check_tif(p, bands, scale, dtype)
            if r == "FAIL":
                lv = "FAIL"
                notes.append(f"{os.path.basename(p)}: {n}")
            elif r == "WARN":
                empties.append(p[-10:-4])
        if empties:
            lv = worst(lv, "WARN" if len(empties) < len(files) else "FAIL")
            notes.append(f"tháng toàn NoData: {','.join(empties)}")
        if sizes:
            notes.append(f"dung lượng {min(sizes):.0f} đến {max(sizes):.0f} KB/ảnh, TB {np.mean(sizes):.0f} KB")
        res.append((f"{kind} ({bands} kênh, {scale} m, {dtype})", lv, "; ".join(notes)))

    d = day_csv[day_csv["GID_3"] == gid] if day_csv is not None else pd.DataFrame()
    if d.empty:
        res.append(("CSV/day_indices.csv", "FAIL", "không có dòng của xã này"))
    else:
        miss = [c for c in T1_FEATS if c not in d.columns]
        allnan = [c for c in T1_FEATS if c in d.columns and d[c].isna().all()]
        lv = "FAIL" if (miss or allnan or len(d) > 12 or d["MONTH"].duplicated().any()) else "PASS"
        if lv == "PASS" and d[T1_FEATS].isna().any(axis=1).any():
            lv = "WARN"
        res.append(("CSV/day_indices.csv (20 cột chỉ số)", lv, f"{len(d)} tháng"
                    + (f"; thiếu cột {miss}" if miss else "") + (f"; cột rỗng {allnan}" if allnan else "")))

    n = night_csv[night_csv["GID_3"] == gid] if night_csv is not None else pd.DataFrame()
    need3 = 12 - sum(v == "none" for v in (st.get("t3img") or {}).values())
    if n.empty:
        res.append(("CSV/night_indices.csv", "FAIL", "không có dòng của xã này"))
    else:
        ok_cols = list(night_csv.columns) == T3_COLS
        res.append(("CSV/night_indices.csv (21 cột như notebook)", "PASS" if ok_cols and len(n) == need3 else "FAIL",
                    f"{len(n)} dòng" + ("" if ok_cols else f"; cột lệch: {list(night_csv.columns)}")))
    res.append(("Thời gian xử lý", "PASS", f"{st.get('seconds', '?')} giây ở lần xử lý cuối"))
    return res


def _cmp_csv(a, b, tol):
    a, b = a.sort_values("MONTH").reset_index(drop=True), b.sort_values("MONTH").reset_index(drop=True)
    if len(a) != len(b):
        return "FAIL", f"số dòng {len(a)} vs {len(b)}"
    cols = [c for c in a.columns if c in b.columns and pd.api.types.is_numeric_dtype(a[c])
            and pd.api.types.is_numeric_dtype(b[c])]
    diffs = {c: float(np.nanmax(np.abs(a[c].astype(float) - b[c].astype(float)))) if a[c].notna().any() else 0.0
             for c in cols}
    nan_mismatch = [c for c in cols if not (a[c].isna() == b[c].isna()).all()]
    bad = {c: x for c, x in diffs.items() if x > tol}
    if bad or nan_mismatch:
        return "FAIL", f"lệch {bad} NaN khác ở {nan_mismatch}"
    return "PASS", f"{len(cols)} cột số, sai số lớn nhất {max(diffs.values()) if diffs else 0:.2e}"


def _cmp_tif(p_pipe, p_nb, tol):
    a1, t1, s1 = read_scaled(p_pipe)
    a2, t2, _ = read_scaled(p_nb)
    a2 = a2[:a1.shape[0]]                     # pipeline có thể chỉ lưu 6 kênh đầu
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
    both = ~np.isnan(w1) & ~np.isnan(w2)
    if not both.any():
        return "FAIL", "không có pixel chung"
    diff = float(np.max(np.abs(w1[both] - w2[both])))
    only = int((np.isnan(w1) ^ np.isnan(w2)).sum())
    lv = "PASS" if diff <= tol and only == 0 else ("WARN" if diff <= tol else "FAIL")
    return lv, f"{int(both.sum()):,} giá trị chung, lệch lớn nhất {diff:.2e} (cho phép {tol:.0e}), {only} pixel lệch NoData"


def compare_notebook(root, nb_dir, gid, day_csv, night_csv):
    safe = gid.replace(".", "_")
    out = []
    for label, df, pat, tol in (("Đối chiếu chỉ số ngày", day_csv, f"s2_*_{safe}_2024_Spectral_Indices.csv", 1e-6),
                                ("Đối chiếu chỉ số đêm", night_csv,
                                 f"*_{safe}_202401-202412_Economic_Indices.csv", 1e-6)):
        ref = glob.glob(os.path.join(nb_dir, pat))
        mine = df[df["GID_3"] == gid] if df is not None else pd.DataFrame()
        if not ref or mine.empty:
            out.append((label, "WARN", "thiếu file để so"))
            continue
        out.append((label, *_cmp_csv(mine, pd.read_csv(ref[0]), tol)))
    for p_nb in sorted(glob.glob(os.path.join(nb_dir, f"*_{safe}_2024??.tif"))):
        name = os.path.basename(p_nb)
        ym = name[-10:-4]
        day = name.startswith("S2_Day")
        sub, kind = ("Day", "day") if day else ("Night", "night")
        mine = glob.glob(os.path.join(root, sub, "*", f"{gid}_*", f"{safe}_{kind}_{ym}.tif"))
        if not mine:
            out.append((f"Đối chiếu {name}", "WARN", "pipeline không có ảnh tháng này"))
            continue
        tol = 0.5 / 1000 + 1e-9
        try:
            import rasterio
            with rasterio.open(mine[0]) as s:
                tol = (s.scales[0] / 2 + 1e-9) if day else 0.0
        except Exception:
            pass
        out.append((f"Đối chiếu {name}", *_cmp_tif(mine[0], p_nb, tol)))
    return out or [("Đối chiếu notebook", "WARN", f"không tìm thấy file notebook của {gid}")]


def _short(note, limit=3):
    parts = note.replace("|", "/").split("; ")
    if len(parts) > limit + 2:
        parts = parts[:limit + 1] + [f"... và {len(parts) - limit - 1} mục khác"]
    return "; ".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root")
    ap.add_argument("--remote")
    ap.add_argument("--gids", default="")
    ap.add_argument("--notebook-dir", default="")
    ap.add_argument("--notebook-gid", default="")
    ap.add_argument("--report", default="")
    a = ap.parse_args()
    root = a.root
    if a.remote:
        root = tempfile.mkdtemp(prefix="vngis_verify_")
        subprocess.run(["rclone", "copy", a.remote, root, "--exclude", "_control/logs/**",
                        "--exclude", "_control/parts/**", "--transfers", "16"], check=True)
    if not root or not os.path.isdir(root):
        sys.exit("Cần --root hoặc --remote hợp lệ.")

    def _csv(name):
        p = os.path.join(root, "CSV", name)
        return pd.read_csv(p, dtype={"GID_1": str, "GID_3": str}) if os.path.isfile(p) else None
    day_csv, night_csv = _csv("day_indices.csv"), _csv("night_indices.csv")
    st = load_status(root)
    gids = [g.strip() for g in a.gids.split(",") if g.strip()] or sorted(st)
    lines = ["## Kết quả kiểm tra thí điểm VNGISDash 2024", ""]
    any_fail = False
    for gid in gids:
        rows = verify_commune(root, gid, st.get(gid), day_csv, night_csv)
        if a.notebook_dir and (not a.notebook_gid or a.notebook_gid == gid):
            rows += compare_notebook(root, a.notebook_dir, gid, day_csv, night_csv)
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
