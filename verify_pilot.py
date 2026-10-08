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
from pathlib import Path

import vngis_2024 as V
D = V.D
rclone_run = V.rclone_run

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
        if dtype and s.dtypes[0] != dtype:
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
    """Verify actual monthly artifacts, not a legacy JSONL done/none flag."""
    frames = {}
    res = []
    for kind, source in [('day', day_csv), ('night', night_csv)]:
        if source is None or list(source) != D.COLUMNS[kind]:
            return [('Schema ' + kind, 'FAIL', 'CSV thiếu hoặc sai thứ tự cột')]
        frame = source.loc[source.gid_3.eq(gid)]
        if len(frame) != 12 or frame.duplicated(D.KEY).any() or not frame.year.eq(2024).all() or set(frame.month) != set(D.MONTHS):
            return [('Tháng ' + kind, 'FAIL', 'Phải có đúng 12 tháng năm 2024, không trùng')]
        frames[kind] = frame
    admin = frames['day'][D.ADMIN_COLUMNS].drop_duplicates()
    if len(admin) != 1:
        return [('Địa giới', 'FAIL', 'Thông tin hành chính không nhất quán theo GID')]
    if not D.same_table(admin.reset_index(drop=True), frames['night'][D.ADMIN_COLUMNS].drop_duplicates().reset_index(drop=True)):
        return [('Địa giới', 'FAIL', 'CSV ngày/đêm khác thông tin hành chính')]
    sources = D.read_sources(Path(root)/'_control/source_counts.csv')
    images = {}
    safe = gid.replace('.', '_')
    for kind in ['day', 'night']:
        for path in (Path(root)/kind.title()).rglob(f'{safe}_{kind}_2024??.tif'):
            match = D.IMAGE_RE.match(path.name)
            if not match:
                continue
            key = kind, gid, int(match[3])
            images[key] = ('failed', 'Trùng ảnh xã/tháng') if key in images else D.validate_image(path, kind)
    table = D.progress(admin, frames, images, sources)
    for field in D.FIELDS:
        bad = table.loc[~table[field].isin(D.TERMINAL)]
        res.append((field, 'FAIL' if len(bad) else 'PASS',
                    str(table[field].value_counts().to_dict()) + ('; ' + '; '.join(bad[field+'_error']) if len(bad) else '')))
    return res


def _cmp_csv(a, b, tol):
    a, b = D.aliases(a), D.aliases(b)
    metrics = [c for c in D.DAY_METRICS+D.NIGHT_METRICS if c in a and c in b]
    if metrics:
        a = a.loc[~a[metrics].isna().all(axis=1)]
    a, b = a.sort_values("month").reset_index(drop=True), b.sort_values("month").reset_index(drop=True)
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
        mine = df[df["gid_3"] == gid] if df is not None else pd.DataFrame()
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
                tol = (s.scales[0] / 2 + 1e-9) if day and s.dtypes[0] == "int16" else 0.0
        except Exception:
            pass
        out.append((f"Đối chiếu {name}", *_cmp_tif(mine[0], p_nb, tol)))
    return out or [("Đối chiếu notebook", "WARN", f"không tìm thấy file notebook của {gid}")]


def _short(note, limit=3):
    parts = note.replace("|", "/").split("; ")
    if len(parts) > limit + 2:
        parts = parts[:limit + 1] + [f"... và {len(parts) - limit - 1} mục khác"]
    return "; ".join(parts)


def summarize_progress(root, pipeline_code=None):
    """Read-only diagnostics, including when inventory stopped on a legacy CSV.

    This report never validates artifacts or changes the pipeline exit code.
    Legacy commune status must not be presented as verified monthly completion.
    """
    root = Path(root)
    lines = [f"### Tiến độ ({root.name})", ""]
    if pipeline_code is not None and str(pipeline_code) != '':
        lines += [f"Mã thoát pipeline: `{pipeline_code}`.", ""]
    path = root/'_control/progress.csv'
    if not path.is_file():
        return '\n'.join(lines + ["Cảnh báo: chưa có progress.csv; kiểm kê có thể chưa hoàn tất.", ""])
    try:
        frame = D.aliases(pd.read_csv(path))
    except (OSError, ValueError, pd.errors.ParserError, UnicodeError) as exc:
        return '\n'.join(lines + [f"Cảnh báo: không đọc được progress.csv ({type(exc).__name__}). "
                                   "Xem log Chạy pipeline; bảng chưa được xác minh.", ""])
    if 'gid_3' not in frame:
        return '\n'.join(lines + ["Cảnh báo: progress.csv thiếu gid_3/GID_3; bảng chưa được xác minh.", ""])
    lines += [f"Tổng {frame['gid_3'].nunique():,} xã, {len(frame):,} dòng tiến độ.", ""]
    if not set(D.KEY + D.FIELDS).issubset(frame):
        lines += ["Cảnh báo: bảng tiến độ cũ hoặc thiếu cột xã–tháng. "
                  "Chưa thể thống kê bốn phần ngày/đêm; không coi trạng thái cũ done là hoàn tất.", ""]
        return '\n'.join(lines)
    invalid = (frame['gid_3'].isna() | frame['gid_3'].astype(str).str.strip().eq('') |
               ~frame['year'].eq(D.YEAR) | ~frame['month'].isin(D.MONTHS))
    if invalid.any() or frame.duplicated(D.KEY).any():
        lines += ["Cảnh báo: khóa xã–tháng bị thiếu, sai hoặc trùng; bảng chưa được xác minh.", ""]
        return '\n'.join(lines)
    if frame.empty or not frame.groupby('gid_3').size().eq(12).all():
        lines += ["Cảnh báo: chưa đủ 12 dòng tháng năm 2024 cho mỗi xã.", ""]
    if not frame[D.FIELDS].isin(D.STATES).all().all():
        lines += ["Cảnh báo: bảng có trạng thái trống hoặc không hợp lệ.", ""]
    lines += [f"{len(frame):,} xã–tháng. Đây là trạng thái ghi trong bảng; "
              "lượt chạy tiếp vẫn kiểm tra file thực tế.", ""]
    for field in D.FIELDS:
        lines.append(f"- {field}: {frame[field].value_counts(dropna=False).to_dict()}")
    return '\n'.join(lines) + '\n'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root")
    ap.add_argument("--remote")
    ap.add_argument("--gids", default="")
    ap.add_argument("--notebook-dir", default="")
    ap.add_argument("--notebook-gid", default="")
    ap.add_argument("--report", default="")
    ap.add_argument("--self-test", action="store_true", help="Chạy kiểm thử offline v6, không truy cập Google")
    ap.add_argument("--progress-summary", action="store_true", help="Tóm tắt chỉ đọc, hỗ trợ bảng tiến độ cũ")
    ap.add_argument("--pipeline-code", default=None, help="Mã thoát pipeline để hiển thị trong tóm tắt")
    a = ap.parse_args()
    if a.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        raise SystemExit(0 if result.wasSuccessful() else 1)
    if a.progress_summary:
        print(summarize_progress(a.root or V.LOCAL_ROOT, a.pipeline_code))
        return
    root = a.root
    if a.remote:
        root = tempfile.mkdtemp(prefix="vngis_verify_")
        result = rclone_run(["copy", a.remote, root, "--exclude", "_control/logs/**",
                             "--exclude", "_control/parts/**", "--exclude", "_control/backups/**",
                             "--exclude", "_control/staging/**", "--exclude", "_control/outbox/**"])
        if result.returncode:
            raise SystemExit("Không tải được dữ liệu thí điểm từ Drive")
    if not root or not os.path.isdir(root):
        sys.exit("Cần --root hoặc --remote hợp lệ.")

    def _csv(name):
        p = os.path.join(root, "CSV", name)
        return pd.read_csv(p, dtype={"GID_1": str, "GID_3": str}) if os.path.isfile(p) else None
    day_csv, night_csv = _csv("day_indices.csv"), _csv("night_indices.csv")
    st = load_status(root)
    gids = [g.strip() for g in a.gids.split(",") if g.strip()] or sorted(st)
    lines = ["## Kết quả kiểm tra thí điểm VNGISDash 2024", ""]
    if not gids and day_csv is not None and 'gid_3' in day_csv:
        gids = sorted(day_csv.gid_3.unique(), key=D.natural_key)
    any_fail = not bool(gids)
    if any_fail:
        lines.append("FAIL: không có xã nào để xác minh.")
    for kind, frame in [('day', day_csv), ('night', night_csv)]:
        if frame is not None and list(frame) == D.COLUMNS[kind]:
            order = frame.apply(lambda r: (D.natural_key(r.gid_3), r.name_3, r.year, r.month), axis=1).tolist()
            if order != sorted(order):
                any_fail = True
                lines.append(f"FAIL: thứ tự tự nhiên trong CSV {kind} không đúng.")
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
        if rclone_run(["copyto", report, f"{a.remote}/_control/verify_report.md"]).returncode:
            any_fail = True
    sys.exit(1 if any_fail else 0)



# Kiểm thử offline gộp trong file xác minh v6, không cần thư mục tests.
N = R = V
Q = sys.modules[__name__]
V6_SCIENTIFIC_HASHES = 'mask_s2_sr 3de229fb0b7c280c8230d0708c0163deb6c0783b184b29329f698c65b17c88da\nadd_indices a01bd59402c5d468a6842261af48038e4558b90172ae5de033a3dfe9b87e3f9a\nmask_s2_clean c69947807eeed5e06f92304c6c5fbd9500e27baefda8dfb30815bb4e97996ade\n_month_dates cdd246ce2ce6e8460987c3774f4384a494d1e55adee58102ff07113139e84130\n_s2_windows 57807c1d792e21bb20cbd815509c4631699172bd8b80da405161128e205a243c\nday_image ca972a270d5b20ea8eac0d4e26997ad1294eb3350b6ce918a0123a2f8aa9dde9\nnight_image 8355dcb36775022ad5b29e2ae76d3b397807fff9b67ab3523054e0c7ae53b353\ntask3_all_months 44bc3bcb78bdca583c5d9ee548378886fe4a257d3fa987415503d55da2366c65\nmosaic_tiles c4a639414c849650dcb4b934ee5de3dc2dd66366825a66e21298057fdde987aa\ncompare_on_grid 437b167dba34e885cfd21293af9d583eed6ab1c0d31b9bc0e1e02aece88eb568\nwrite_tif 74e83ad0843b2480ae13213a6b268a09c6307e40ab0dc2e8f4d5d390add30005\nread_dbf 2c6a8ce557e376022b8046633346786bbdf369702b2f8da3b076f0fbf4caceba\ntask1_all_months 1cf4a2c27bbc7035b77dca10bded29df2c3c90b1f3cb3cbf4d64665da0932103\n'

import json
from pathlib import Path
import tempfile
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch, Mock

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import Affine

import vngis_2024 as V


def admin(gids=('VNM.1.2_1', 'VNM.1.10_1')):
    return pd.DataFrame([dict(GID_3=g, NAME_3='Xã ' + g, TYPE_3='Xa', GID_2='VNM.1_1',
                              NAME_2='Huyện', GID_1='VNM.1_1', NAME_1='Tỉnh') for g in gids])


def records(a, kind='day'):
    rows = []
    for gid in a['GID_3']:
        for m in range(1, 13):
            row = dict(GID_3=gid, YEAR=2024, MONTH=m)
            row.update({c: float(m) for c in D.DAY_METRICS if kind == 'day'})
            if kind == 'night':
                row.update({c: float(m) for c in D.NIGHT_METRICS})
                row['TIME'] = f'2024-{m:02d}'
                if m == 1:
                    row['TNL_MOM_GROWTH_PCT'] = np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def complete_progress(a):
    images = {(k, g, m): ('done', '') for k in ['day', 'night']
              for g in a['GID_3'] for m in range(1, 13)}
    return D.progress(a, {k: records(a, k) for k in ['day', 'night']}, images)


class ProgressSummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)/'VNGISDash_2024'
        self.path = self.root/'_control/progress.csv'
        self.path.parent.mkdir(parents=True)

    def tearDown(self):
        self.temp.cleanup()

    def test_legacy_uppercase_commune_progress_does_not_claim_monthly_completion(self):
        pd.DataFrame({'GID_3': ['VNM.1.2_1', 'VNM.1.10_1'], 'status': ['done', 'done']}).to_csv(self.path, index=False)
        original = self.path.read_bytes()
        report = Q.summarize_progress(self.root, '1')
        self.assertIn('Tổng 2 xã, 2 dòng', report)
        self.assertIn('bảng tiến độ cũ', report)
        self.assertIn('Mã thoát pipeline: `1`', report)
        self.assertNotIn('- day_image:', report)
        self.assertEqual(self.path.read_bytes(), original)

    def test_new_monthly_progress_reports_all_four_fields_and_no_source(self):
        frame = complete_progress(admin())
        frame.loc[0, 'day_image'] = 'no_source'
        frame.to_csv(self.path, index=False)
        report = Q.summarize_progress(self.root, '0')
        self.assertIn('24 xã–tháng', report)
        for field in D.FIELDS:
            self.assertIn(f'- {field}:', report)
        self.assertIn("'no_source': 1", report)
        self.assertNotIn('Cảnh báo', report)

    def test_uppercase_monthly_identifiers_are_supported_read_only(self):
        frame = complete_progress(admin()).rename(columns={c: c.upper() for c in D.PREFIX})
        frame.to_csv(self.path, index=False)
        self.assertNotIn('Cảnh báo', Q.summarize_progress(self.root))

    def test_missing_empty_malformed_and_conflicting_progress_show_warnings(self):
        self.assertIn('chưa có progress.csv', Q.summarize_progress(self.root))
        for text in ['', 'status\ndone\n', 'gid_3,status\n"unfinished',
                     'gid_3,GID_3\nVNM.1_1,VNM.2_1\n']:
            with self.subTest(text=text):
                self.path.write_text(text)
                original = self.path.read_bytes()
                self.assertIn('Cảnh báo', Q.summarize_progress(self.root))
                self.assertEqual(self.path.read_bytes(), original)

    def test_duplicate_or_invalid_month_keys_are_not_reported_as_verified(self):
        frame = complete_progress(admin())
        for bad in [pd.concat([frame, frame.iloc[[0]]]), frame.assign(year=2023),
                    frame.assign(month=13), frame.assign(gid_3=np.nan)]:
            with self.subTest(rows=len(bad)):
                bad.to_csv(self.path, index=False)
                report = Q.summarize_progress(self.root)
                self.assertIn('bảng chưa được xác minh', report)
                self.assertNotIn('- day_image:', report)

    def test_missing_month_or_unknown_status_is_visible(self):
        frame = complete_progress(admin()).iloc[:-1].copy()
        frame.loc[0, 'day_image'] = 'unexpected'
        frame.to_csv(self.path, index=False)
        report = Q.summarize_progress(self.root)
        self.assertIn('chưa đủ 12 dòng', report)
        self.assertIn('trạng thái trống hoặc không hợp lệ', report)

    def test_actual_workflow_summary_command_handles_legacy_progress_after_failure(self):
        # Execute the shell body used by Actions, preventing drift between CLI and YAML.
        repository = Path(V.__file__).resolve().parent
        workflow = (repository/'.github/workflows/vngis-2024.yml').read_text()
        section = workflow.split('      - name: Tóm tắt tiến độ\n', 1)[1].split('\n      - name:', 1)[0]
        command = '\n'.join(line[10:] for line in section.split('        run: |\n', 1)[1].splitlines())
        summary = self.root/'summary.md'
        pd.DataFrame({'GID_3': ['VNM.1.2_1'], 'status': ['done']}).to_csv(self.path, index=False)
        env = dict(os.environ, VNGIS_LOCAL_ROOT=str(self.root), GITHUB_STEP_SUMMARY=str(summary),
                   VNGIS_PIPELINE_CODE='1', PATH=str(Path(sys.executable).parent)+os.pathsep+os.environ['PATH'])
        result = subprocess.run(['bash', '-e', '-c', command], cwd=repository, env=env,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Mã thoát pipeline: `1`', summary.read_text())
        self.assertIn('bảng tiến độ cũ', summary.read_text())
        self.assertIn("steps.pipeline.outputs.code != '0'", workflow)
        self.assertIn('Báo lỗi nếu script thoát bất thường', workflow)


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.a = admin()

    def test_schema_legacy_migration_natural_order_and_names_by_gid(self):
        old = records(self.a).iloc[::-1].copy()
        old['NAME_3'] = 'wrong legacy name'
        frame = D.normalize(old, self.a, 'day')
        self.assertEqual(list(frame.columns), D.PREFIX + D.DAY_METRICS)
        self.assertEqual(frame['gid_3'].drop_duplicates().tolist(), self.a['GID_3'].tolist())
        self.assertEqual(frame.iloc[0]['name_3'], self.a.iloc[0]['NAME_3'])
        self.assertEqual(frame.groupby('gid_3')['month'].apply(list).iloc[0], list(range(1, 13)))
        self.assertTrue(frame['BLUE_mean'].eq(frame['month']).all())

    def test_missing_month_becomes_blank_and_blocks_night(self):
        old = records(self.a)
        old = old[old['MONTH'] != 5]
        frame = D.normalize(old, self.a, 'day')
        self.assertEqual(len(frame), 24)
        self.assertTrue(frame.loc[frame['month'].eq(5), D.DAY_METRICS].isna().all().all())
        table = complete_progress(self.a)
        table.loc[table['month'].eq(5), 'day_indices'] = 'pending'
        with self.assertRaisesRegex(RuntimeError, 'Chặn phần đêm'):
            D.require_day_complete(table)

    def test_no_source_requires_evidence_and_keeps_nan(self):
        blank = D.normalize(pd.DataFrame(), self.a, 'day').iloc[0]
        self.assertEqual(D.metric_state(blank, 'day')[0], 'pending')
        self.assertEqual(D.metric_state(blank, 'day', 0)[0], 'no_source')
        images = {(k, g, m): ('done', '') for k in ['day', 'night'] for g in self.a.GID_3 for m in D.MONTHS}
        sources = {(g, m, 'day_indices'): 0 for g in self.a.GID_3 for m in D.MONTHS}
        table = D.progress(self.a, {'day': pd.DataFrame(), 'night': records(self.a, 'night')}, images, sources)
        D.require_day_complete(table)
        self.assertTrue(table.day_indices.eq('no_source').all())

    def test_duplicates_wrong_year_and_unknown_gid(self):
        rows = records(self.a)
        self.assertEqual(len(D.normalize(pd.concat([rows, rows]), self.a, 'day')), 24)
        conflict = rows.iloc[[0]].copy()
        conflict['BLUE_mean'] = 100
        with self.assertRaisesRegex(ValueError, 'trùng khóa'):
            D.normalize(pd.concat([rows, conflict]), self.a, 'day')
        for c, v in [('YEAR', 2023), ('MONTH', 13), ('GID_3', 'unknown')]:
            wrong = rows.copy()
            wrong.loc[0, c] = v
            with self.assertRaises(ValueError):
                D.normalize(wrong, self.a, 'day')

    def test_preserve_valid_old_values_and_only_repair_missing(self):
        old = records(self.a).iloc[1:].copy()
        new = records(self.a)
        new[D.DAY_METRICS] *= 100
        result = D.merge_valid(old, new, self.a, 'day')
        self.assertEqual(result.iloc[0].BLUE_mean, 100)
        self.assertEqual(result.iloc[1].BLUE_mean, 2)

    def test_merge_night_into_initially_empty_table(self):
        frame = D.merge_valid(pd.DataFrame(), records(self.a, 'night'), self.a, 'night')
        self.assertEqual(frame.iloc[0].TIME, '2024-01')
        self.assertEqual(len(frame), 24)

    def test_boundary_count_and_duplicate_gid_fail_explicitly(self):
        with self.assertRaisesRegex(ValueError, '11,163'):
            D.administrative_table(self.a)
        with self.assertRaisesRegex(ValueError, 'mã trùng'):
            D.administrative_table(pd.concat([self.a, self.a]), expected=None)

    def test_full_gadm_scope_is_accepted_and_old_count_is_rejected(self):
        self.assertEqual(D.EXPECTED_COMMUNES, 11163)
        full = admin(tuple(f'VNM.1.{i}_1' for i in range(1, 11164)))
        self.assertEqual(len(D.administrative_table(full)), 11163)
        with self.assertRaisesRegex(ValueError, '11,136.*11,163'):
            D.administrative_table(full.iloc[:11136])

    def test_full_gadm_scope_still_rejects_duplicates_and_missing_names(self):
        full = admin(tuple(f'VNM.1.{i}_1' for i in range(1, 11164)))
        duplicate = full.copy()
        duplicate.loc[1, 'GID_3'] = duplicate.loc[0, 'GID_3']
        with self.assertRaisesRegex(ValueError, 'mã trùng'):
            D.administrative_table(duplicate)
        blank = full.copy()
        blank.loc[1, 'NAME_3'] = ''
        with self.assertRaisesRegex(ValueError, 'thông tin hành chính trống'):
            D.administrative_table(blank)

    def test_night_columns_and_first_month_growth_nan_is_valid(self):
        frame = D.normalize(records(self.a, 'night'), self.a, 'night')
        self.assertEqual(list(frame), D.PREFIX + D.NIGHT_METRICS)
        self.assertEqual(D.metric_state(frame.iloc[0], 'night')[0], 'done')

    def test_gate_rejects_missing_month_duplicate_or_partial_state(self):
        table = complete_progress(self.a)
        D.require_day_complete(table)
        for wrong in [table.iloc[1:], pd.concat([table, table.iloc[[0]]])]:
            with self.assertRaises(RuntimeError):
                D.require_day_complete(wrong)
        for state in ['pending', 'running', 'failed']:
            wrong = table.copy()
            wrong.loc[0, 'day_image'] = state
            with self.assertRaises(RuntimeError):
                D.require_day_complete(wrong)

    def test_resume_interrupted_work_but_recheck_done(self):
        prev = complete_progress(self.a)
        prev.loc[0, 'day_image'] = 'running'
        frames = {k: records(self.a, k) for k in ['day', 'night']}
        resumed = D.progress(self.a, frames, {}, previous=prev)
        self.assertEqual(resumed.iloc[0].day_image, 'failed')
        self.assertEqual(resumed.iloc[1].day_image, 'pending')
        recovered = D.progress(self.a, frames, {('day', self.a.iloc[0].GID_3, 1): ('done', '')}, previous=prev)
        self.assertEqual(recovered.iloc[0].day_image, 'done')

import ast
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import requests
from rasterio.transform import Affine

import vngis_2024 as V


class ScientificTests(unittest.TestCase):
    def test_fetch_plan_queries_only_active_phase_sources(self):
        collection = Mock()
        collection.filterBounds.return_value = collection
        collection.filterDate.return_value = collection
        collection.size.return_value = 1
        fc = Mock();fc.size.return_value=1
        with patch.object(V.ee,'ImageCollection',return_value=collection) as source, patch.object(V.ee,'List',side_effect=lambda x:x), patch.object(V.ee,'Dictionary',side_effect=lambda x:x), patch.object(V,'ee_getinfo',side_effect=lambda x:x):
            _, day = V.fetch_plan(fc, 'geom', 'day')
            self.assertEqual({c.args[0] for c in source.call_args_list},{V.S2_COLLECTION})
            self.assertEqual(day[1]['indices_count'],1)
            source.reset_mock()
            _, night = V.fetch_plan(fc, 'geom', 'night')
            self.assertEqual({c.args[0] for c in source.call_args_list},{V.VIIRS_A,V.VIIRS_B})
            self.assertEqual(night[1]['viirs'],V.VIIRS_A)

    def test_original_v6_scientific_functions_are_unchanged(self):
        root = Path(V.__file__).resolve().parent
        functions = {n.name: n for n in ast.parse((root/'vngis_2024.py').read_text()).body
                     if isinstance(n, ast.FunctionDef)}
        for line in V6_SCIENTIFIC_HASHES.splitlines():
            name, expected = line.split()
            if name == 'task1_all_months':
                current = functions[name]
                self.assertEqual(current.args.args[-1].arg,'months')
                current.args.args.pop()
                current.args.defaults.clear()
                loop = next(n for n in current.body if isinstance(n,ast.For))
                self.assertEqual(ast.dump(loop.iter),ast.dump(ast.parse('MONTHS if months is None else months',mode='eval').body))
                loop.iter = ast.Name(id='MONTHS',ctx=ast.Load())
            actual = hashlib.sha256(ast.dump(functions[name], include_attributes=False).encode()).hexdigest()
            self.assertEqual(actual, expected, name)

    def test_pilot_does_not_increase_workers_and_rejects_quality_reduction(self):
        code = 'import vngis_2024 as v; print(v.N_WORKERS)'
        env = {**os.environ, 'VNGIS_MODE': 'pilot', 'VNGIS_PILOT_N': '8', 'VNGIS_WORKERS': '1'}
        result = subprocess.run([os.sys.executable, '-c', code], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), '1')
        for name, value in [('VNGIS_DAY_BANDS', '6'), ('VNGIS_DAY_FORMAT', 'int16')]:
            result = subprocess.run([os.sys.executable, '-c', code], env={**env, name: value}, capture_output=True)
            self.assertNotEqual(result.returncode, 0)


class RateTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(R.log, 'disabled', True)
        logger.start(); self.addCleanup(logger.stop)
        self.now = 0.
        self.event = Mock()
        self.event.wait.side_effect = self.advance
        self.gate = N.RequestGate(1, 2, lambda: None, self.event, clock=lambda: self.now)
        flags = ['--transfers','2','--checkers','4','--tpslimit','2.0','--tpslimit-burst','2','--retries','1','--low-level-retries','1']
        controlled = patch.object(V,'RCLONE_COMMON',flags)
        controlled.start(); self.addCleanup(controlled.stop)

    def advance(self, seconds):
        self.now += seconds
        return False

    def error(self, code=429, header='7'):
        response = requests.Response()
        response.status_code = code
        response.headers['Retry-After'] = header
        return requests.HTTPError(response=response)

    def test_retry_after_seconds_and_http_date(self):
        self.assertEqual(N.retry_after('7'), 7)
        self.assertEqual(N.retry_after('Thu, 08 Oct 2026 04:00:10 GMT', datetime(2026,10,8,4,0,0,tzinfo=timezone.utc)), 10)
        self.assertIsNone(N.retry_after('invalid'))
        self.assertLessEqual(N.retry_delay(10), 120)

    def test_429_cooldown_is_shared_and_semaphore_released_during_backoff(self):
        def defer(service, delay, throttled):
            self.assertTrue(self.gate.semaphore.acquire(blocking=False))
            self.gate.semaphore.release()
            original(service, delay, throttled)
        original = self.gate.defer
        operation = Mock(side_effect=[self.error(), 'success'])
        with patch.object(self.gate, 'defer', side_effect=defer):
            self.assertEqual(self.gate.call('Earth Engine', operation, 2), 'success')
        self.assertGreaterEqual(self.now, 7)
        self.assertEqual(self.gate.throttles['Earth Engine'], 1)
        self.gate.defer('Image download', 11, True)
        before = self.now
        with self.gate.slot('Earth Engine'):
            self.assertGreaterEqual(self.now-before, 11)

    def test_ee_and_image_download_share_start_rate(self):
        with self.gate.slot('Earth Engine'):
            pass
        with self.gate.slot('Image download'):
            self.assertGreaterEqual(self.now, .5)

    def test_exhausted_429_is_bounded_and_stop_interrupts_wait(self):
        operation = Mock(side_effect=self.error(header='1'))
        with self.assertRaisesRegex(RuntimeError, '6/6'):
            self.gate.call('Earth Engine', operation, 6)
        self.assertEqual(operation.call_count, 6)
        self.assertEqual(self.gate.throttles['Earth Engine'], 6)
        stopped = N.RequestGate(1, 2, Mock(side_effect=V.StopRequested()), threading.Event())
        with self.assertRaises(V.StopRequested):
            stopped.wait(100)

    def test_drive_commands_use_throttle_flags_and_do_not_retry_permission_errors(self):
        with patch.object(V, 'REQUEST_GATE', self.gate), patch.object(R.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', '403 forbidden')) as run:
            R.rclone_run(['lsjson', 'gdrive:sample'])
            self.assertEqual(run.call_count, 1)
            argv = run.call_args.args[0]
            self.assertEqual(argv[argv.index('--tpslimit')+1], '2.0')
            self.assertEqual(argv[argv.index('--transfers')+1], '2')
            self.assertEqual(argv[argv.index('--checkers')+1], '4')

    def test_drive_retry_after_and_successful_command_are_not_replayed(self):
        results = [subprocess.CompletedProcess([],1,'','HTTP 429\nRetry-After: 8'),
                   subprocess.CompletedProcess([],0,'success','Recovered from 429')]
        with patch.object(V,'REQUEST_GATE',self.gate),patch.object(R.subprocess,'run',side_effect=results) as run:
            self.assertEqual(R.rclone_run(['moveto','source','destination']).returncode,0)
            self.assertEqual(run.call_count,2)
        self.assertGreaterEqual(self.now,8)

    def test_download_429_honors_header_without_logging_signed_url(self):
        image = Mock()
        image.getDownloadURL.return_value = 'https://example.invalid/?token=secret'
        first = requests.Response()
        first.status_code = 429
        first.headers['Retry-After'] = '9'
        first._content = b'throttled'
        second = requests.Response()
        second.status_code = 200
        second._content = b'x'*256
        with patch.object(V, 'REQUEST_GATE', self.gate), patch.object(V, '_http_get', side_effect=[first, second]):
            self.assertEqual(V.fetch_geotiff_bytes(image, 'region', 20), b'x'*256)
        self.assertGreaterEqual(self.now, 9)
        self.assertEqual(image.getDownloadURL.call_args.args[0]['scale'], 20)


class FakeDrive:
    def __init__(self, root, local):
        self.root, self.local = Path(root), Path(local)
        self.base = 'fake:VNGISDash_2024_PILOT'
        self.downloads = []
        self.uploads = []
        self.root.mkdir(parents=True)

    def fetch(self, rel, dest, optional=False):
        source, dest = self.root/rel, Path(dest)
        if not source.exists() and optional:
            return None
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
        self.downloads.append(rel)
        return dest

    def metadata(self, path, relative):
        return dict(Path=str(relative), Size=path.stat().st_size, ModTime=str(path.stat().st_mtime_ns),
                    Hashes={'md5': hashlib.md5(path.read_bytes()).hexdigest()})

    def listing(self, rel):
        root = self.root/rel
        return [self.metadata(p, p.relative_to(root)) for p in sorted(root.rglob('*')) if p.is_file()]

    def stat(self, rel, **kwargs):
        path = self.root/rel
        return self.metadata(path, rel) if path.is_file() else None

    def put(self, path, rel, backup=True):
        destination = self.root/rel
        if destination.exists() and backup:
            saved = self.root/f'_control/backups/{len(self.uploads)}/{rel}'
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(destination, saved)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, str(destination)+'.part')
        Path(str(destination)+'.part').replace(destination)
        self.uploads.append(rel)

    def pull_history(self):
        for rel in ['_control/status', '_control/parts']:
            for entry in self.listing(rel):
                self.fetch(f"{rel}/{entry['Path']}", self.local/rel/entry['Path'])

    def flush_outbox(self):
        pass


class EngineTests(unittest.TestCase):
    def setUp(self):
        logger = patch.object(R.log, 'disabled', True)
        logger.start(); self.addCleanup(logger.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.a = admin(('VNM.1.2_1',))
        self.gid = self.a.iloc[0].GID_3
        self.drive = FakeDrive(root/'remote', root/'local')
        self.engine = R.Engine(self.a, self.drive)
        for obj, name, value in [(V,'MIN_FREE_GB',0), (V,'PREFLIGHT',False), (V,'MAX_ATTEMPTS',1),
                                 (V,'MONTH_THREADS',2), (V,'N_WORKERS',2), (V,'UPLOAD_EVERY_SEC',300)]:
            if hasattr(obj, name):
                patcher = patch.object(obj,name,value)
                patcher.start(); self.addCleanup(patcher.stop)
        for name in ['communes_fc']:
            patcher = patch.object(V,name,Mock());patcher.start();self.addCleanup(patcher.stop)
        patcher = patch.object(V.ee.Filter,'eq',return_value=Mock());patcher.start();self.addCleanup(patcher.stop)
        patcher = patch.dict(os.environ,{'VNGIS_MIN_FREE_GB':'0'});patcher.start();self.addCleanup(patcher.stop)
        V.STOP_EVENT.clear();V.STOP_REASON[0]=None

    def write_image(self, month, kind='day'):
        ctx = self.engine.contexts[self.gid]
        path = self.drive.root/ctx['rel_'+kind+'_dir']/(V.day_name(ctx,month) if kind == 'day' else V.night_name(ctx,month))
        self.image_bytes(path, 20 if kind == 'day' else 500)
        return path

    @staticmethod
    def image_bytes(path, scale):
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        bands = D.DAY_BANDS if scale == 20 else ['avg_rad','cf_cvg']
        V.write_tif(str(path),np.ones((len(bands),4,4),dtype='float64'),
                    Affine(scale/111319.49079327357,0,105,0,-scale/111319.49079327357,21),'EPSG:4326',-9999,bands)

    def fill_day(self):
        for m in D.MONTHS:
            self.write_image(m)
        path = self.drive.root/'CSV/day_indices.csv'
        path.parent.mkdir(exist_ok=True)
        records(self.a).to_csv(path,index=False)

    def plan(self, fc, geom, kind):
        return 1,{m:dict(image_count=1,indices_count=1,s2_window=0,viirs=V.VIIRS_A) for m in D.MONTHS}

    def fake_download(self,image,region,scale,path,label):
        self.image_bytes(path,scale)

    def test_inventory_migrates_with_backup_and_does_not_trust_old_done(self):
        self.fill_day()
        original = (self.drive.root/'CSV/day_indices.csv').read_bytes()
        self.engine.inventory()
        backups = list((self.drive.root/'_control/backups').rglob('day_indices.csv'))
        self.assertEqual(backups[0].read_bytes(),original)
        self.assertEqual(list(pd.read_csv(self.drive.root/'CSV/day_indices.csv')),D.COLUMNS['day'])
        self.write_image(5).unlink()
        table = self.engine.inventory()
        self.assertEqual(table.loc[table.month.eq(5),'day_image'].iloc[0],'pending')
        with self.assertRaises(RuntimeError):
            D.require_day_complete(table)
        self.assertEqual(len(table),12)

    def test_corrupt_file_cache_is_reused_until_file_changes(self):
        self.fill_day()
        self.write_image(4).write_bytes(b'corrupt')
        self.engine.inventory()
        self.assertEqual(self.engine.table.loc[3,'day_image'],'failed')
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            decode.assert_not_called()
        self.write_image(4)
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            self.assertEqual(decode.call_count,1)
        D.require_day_complete(self.engine.table)

    def test_interrupted_inventory_checkpoints_and_only_decodes_remaining_files(self):
        self.fill_day()
        with patch.object(V,'check_stop',side_effect=[None,None,V.StopRequested()]):
            with self.assertRaises(V.StopRequested):
                self.engine.inventory()
        cache = self.drive.root/'_control/validated_images.json'
        self.assertEqual(len(json.loads(cache.read_text())),2)
        with patch.object(D,'validate_image',wraps=D.validate_image) as decode:
            self.engine.inventory()
            self.assertEqual(decode.call_count,10)
        D.require_day_complete(self.engine.table)

    def test_missing_indices_only_queries_missing_month_and_keeps_valid_values(self):
        self.fill_day()
        rows = records(self.a).iloc[1:]
        rows.to_csv(self.drive.root/'CSV/day_indices.csv',index=False)
        self.engine.inventory()
        def task(fc,months):
            self.assertEqual(months,[1])
            incoming = records(self.a).iloc[[0]].copy()
            incoming[D.DAY_METRICS] *= 100
            return incoming.rename(columns={'GID_3':'GID_3','MONTH':'MONTH'}).to_dict('records')
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'task1_all_months',side_effect=task),patch.object(V,'download_tif') as download:
            self.assertTrue(self.engine.run_phase('day'))
            download.assert_not_called()
        self.assertEqual(self.engine.frames['day'].iloc[0].BLUE_mean,100)
        self.assertEqual(self.engine.frames['day'].iloc[1].BLUE_mean,2)

    def test_missing_image_only_downloads_that_month(self):
        self.fill_day();self.write_image(7).unlink()
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months') as indices,patch.object(V,'download_tif',side_effect=self.fake_download) as download:
            self.assertTrue(self.engine.run_phase('day'))
            indices.assert_not_called()
            self.assertEqual(download.call_count,1)
        self.engine.inventory()
        D.require_day_complete(self.engine.table)

    def test_confirmed_no_source_keeps_twelve_blank_rows(self):
        self.engine.inventory()
        plan = {m:dict(image_count=0,indices_count=0,s2_window=None) for m in D.MONTHS}
        with patch.object(V,'fetch_plan',return_value=(1,plan)),patch.object(V,'task1_all_months') as task,patch.object(V,'download_tif') as download:
            self.assertTrue(self.engine.run_phase('day'))
            task.assert_not_called();download.assert_not_called()
        self.engine.inventory()
        D.require_day_complete(self.engine.table)
        self.assertTrue(self.engine.table.day_image.eq('no_source').all())
        self.assertTrue(self.engine.frames['day'][D.DAY_METRICS].isna().all().all())

    def test_no_source_is_not_inferred_from_failed_requests_and_night_is_blocked(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=RuntimeError('HTTP 429 quota exhausted')):
            self.assertFalse(self.engine.run_phase('day'))
        self.assertTrue(self.engine.table.day_image.eq('failed').all())
        self.assertFalse(self.engine.sources)
        with patch.object(V,'fetch_plan') as query,patch.object(V,'task3_all_months') as task:
            with self.assertRaises(RuntimeError):
                self.engine.run_phase('night')
            query.assert_not_called();task.assert_not_called()

    def test_actual_day_then_night_pipeline_and_verifier(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'night_image'),patch.object(V,'task1_all_months',return_value=records(self.a).to_dict('records')),patch.object(V,'task3_all_months',return_value=records(self.a,'night')),patch.object(V,'download_tif',side_effect=self.fake_download):
            self.assertTrue(self.engine.run_phase('day'))
            self.assertTrue(self.engine.table.night_image.eq('pending').all())
            self.engine.inventory()
            self.assertTrue(self.engine.run_phase('night'))
        self.engine.inventory()
        checks = Q.verify_commune(self.drive.root,self.gid,{},self.engine.frames['day'],self.engine.frames['night'])
        self.assertTrue(all(level == 'PASS' for _,level,_ in checks),checks)

    def test_asset_missing_gid_remains_failed_not_skipped(self):
        self.engine.inventory()
        full = admin(tuple([self.gid]+[f'VNM.9.{i}_1' for i in range(11162)]))
        with patch.object(V,'ee_getinfo',return_value=list(full.GID_3)[1:]):
            with self.assertRaisesRegex(ValueError,'thiếu 1'):
                self.engine.check_asset(full)
        self.assertTrue(self.engine.table.day_image.eq('failed').all())

    def test_failed_attempt_limit_survives_restart(self):
        self.engine.inventory()
        with patch.object(V,'fetch_plan',side_effect=RuntimeError('HTTP 429 exhausted')):
            self.assertFalse(self.engine.run_phase('day'))
        resumed = R.Engine(self.a,self.drive)
        resumed.inventory()
        with patch.object(V,'fetch_plan') as query:
            self.assertFalse(resumed.run_phase('day'))
            query.assert_not_called()

    def test_recover_valid_legacy_parts_if_csv_missing(self):
        part = self.drive.root/'_control/parts/day_legacy.jsonl'
        part.parent.mkdir(parents=True)
        part.write_text(''.join(json.dumps(r)+'\n' for r in records(self.a).to_dict('records')))
        self.engine.inventory()
        self.assertTrue(self.engine.table.day_indices.eq('done').all())
        self.assertEqual(len(self.engine.frames['day']),12)

    def test_main_stops_after_day_failure_and_reports_final_sync_failure(self):
        fake = Mock()
        fake.drive.stat.return_value=None
        fake.root = self.drive.local
        fake.table = complete_progress(self.a)
        fake.run_phase.return_value=False
        with patch.object(R,'Engine',return_value=fake),patch.object(V,'build_admin_table',return_value=self.a),patch.object(V,'load_targets',return_value=self.a),patch.object(V,'init_earth_engine'),patch.object(V,'setup_logging'),patch.object(V,'install_signal_handlers'):
            self.assertEqual(R.pipeline_main(V),1)
            fake.run_phase.assert_called_once_with('day')
            fake.checkpoint.side_effect=RuntimeError('Drive quota')
            self.assertEqual(R.pipeline_main(V,step='inventory'),1)

    def test_fatal_download_stop_is_a_failure_not_a_manual_stop(self):
        fake=Mock();fake.drive.stat.return_value=None;fake.root=self.drive.local
        fake.inventory.side_effect=V.StopRequested()
        with patch.object(R,'Engine',return_value=fake),patch.object(V,'build_admin_table',return_value=self.a),patch.object(V,'load_targets',return_value=self.a),patch.object(V,'setup_logging'),patch.object(V,'install_signal_handlers'),patch.object(V,'STOP_REASON',['fatal']):
            self.assertEqual(R.pipeline_main(V),1)

    def test_atomic_upload_failure_preserves_outbox_snapshot(self):
        drive = R.Drive('gdrive:pilot',self.drive.local)
        path = self.drive.local/'snapshot.csv'
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('old valid data')
        with patch.object(drive,'_put',side_effect=RuntimeError('quota')):
            with self.assertRaises(RuntimeError):
                drive.put(path,'CSV/day_indices.csv')
        temporary = path.with_suffix('.part');temporary.write_text('new data');temporary.replace(path)
        jobs = list((drive.local/'_control/outbox').glob('*/manifest.json'))
        self.assertEqual(len(jobs),1)
        self.assertEqual((jobs[0].parent/'payload').read_text(),'old valid data')
        with patch.object(drive,'_put') as upload:
            drive.flush_outbox()
            self.assertEqual(upload.call_args.args[1],'CSV/day_indices.csv')
            self.assertTrue(upload.call_args.args[2])
        self.assertFalse(list((drive.local/'_control/outbox').glob('*/manifest.json')))

    def test_repeated_replacements_keep_distinct_backups(self):
        drive = R.Drive('gdrive:pilot',self.drive.local)
        with patch.object(drive,'stat',return_value={'Size':1}),patch.object(drive,'command',return_value='') as command:
            drive._put('local.csv','CSV/day_indices.csv')
            drive._put('local.csv','CSV/day_indices.csv')
        targets = [call.args[0][2] for call in command.call_args_list if '/backups/' in call.args[0][2]]
        self.assertEqual(len(targets),2)
        self.assertNotEqual(targets[0],targets[1])

    def test_processing_interruption_resumes_without_recomputing_valid_metrics(self):
        self.engine.inventory()
        def stop_on_third(image,region,scale,path,label):
            if label.endswith('/3'):
                V.request_stop('deadline')
                raise V.StopRequested()
            self.fake_download(image,region,scale,path,label)
        with patch.object(V,'MAX_ATTEMPTS',3),patch.object(V,'MONTH_THREADS',1),patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months',return_value=records(self.a).to_dict('records')),patch.object(V,'download_tif',side_effect=stop_on_third):
            with self.assertRaises(V.StopRequested):
                self.engine.run_phase('day')
        self.engine.checkpoint(force=True)
        V.STOP_EVENT.clear();V.STOP_REASON[0]=None
        resumed = R.Engine(self.a,self.drive)
        resumed.inventory()
        with patch.object(V,'MAX_ATTEMPTS',3),patch.object(V,'fetch_plan',side_effect=self.plan),patch.object(V,'day_image'),patch.object(V,'task1_all_months') as indices,patch.object(V,'download_tif',side_effect=self.fake_download) as download:
            self.assertTrue(resumed.run_phase('day'))
            indices.assert_not_called()
            self.assertEqual(download.call_count,10)

    def test_verifier_rejects_empty_run(self):
        with patch('sys.argv',['verify_pilot.py','--root',str(self.drive.root)]),patch('builtins.print'):
            with self.assertRaises(SystemExit) as result:
                Q.main()
        self.assertEqual(result.exception.code,1)




if __name__ == "__main__":
    main()
