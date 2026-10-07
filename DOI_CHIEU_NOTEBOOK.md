# Đối chiếu notebook và pipeline

Nguồn: `VNGISDash_Task123_Merged_final.ipynb`. Pipeline chỉ giữ 3 chức năng: trích chỉ số ảnh ngày (Task 1) và đêm (Task 3.2), lấy tif ngày (Task 2), lấy tif đêm (Task 3.1). Bảng chọn Tỉnh → Xã (cell 13) và các cell phân tích đường, nhà xưởng, mặt nước (cell 21 đến 32) đã bỏ. Cột "Thay đổi" chỉ ghi phần vỏ (chọn xã, tải, ghi file, xử lý lỗi). Không có tham số khoa học nào bị đổi.

## Phần khoa học (giữ nguyên)

| Notebook | Pipeline (`vngis_2024.py`) | Thay đổi |
|---|---|---|
| Cell 11: `PROJECT_ID`, `ASSET_ID`, `communes_fc` | `PROJECT_ID`, `ASSET_ID`, `init_earth_engine()` | Xác thực bằng service account thay cho `ee.Authenticate()` |
| Cell 13 (đã bỏ bảng chọn): tên xã = `TYPE_3 NAME_3` | `commune_full_name()`, `build_ctx()` | Giữ đúng cách ghép tên để đặt tên file; danh sách xã lấy tự động |
| Cell 3: GADM 4.1, cột `GID_1..TYPE_3` (`gdf_cleaned`) | `build_admin_table()` → `ADMIN_DF` | Chỉ đọc bảng thuộc tính; Task 1 chỉ dùng bảng này để `merge` tên hành chính |
| Cell 15: `normalize_str` bản Task 1 | `normalize_str_t1()` | Không |
| Cell 17: `normalize_str` bản Task 2/3 | `normalize_str()` | Không |
| Cell 15: `mask_s2_sr`, `add_indices` | cùng tên | Không |
| Cell 15: `export_province_s2_local` (lọc theo xã, `CLOUDY_PIXEL_PERCENTAGE < 85`, median, `reduceRegions` scale 50, tileScale 4, EPSG:4326, mean + stdDev) | cùng tên | `print` thành log; không ghi CSV từng tháng (chỉ ghi file gộp 12 tháng như cuối cell 15) |
| Cell 15: vòng 12 tháng, `assign(YEAR, MONTH)`, `concat` | `process_commune()` phần Task 1 | Không |
| Cell 18: `mask_s2_clean`, `get_adaptive_monthly_composite` (nới ±15, ±30 ngày) | cùng tên | `print` bỏ |
| Cell 19: `add_indices(composite).clip(commune_geom)`, scale 20, EPSG:4326, region `commune_geom` | `process_commune()` phần Task 2 | `Export.image.toDrive` thay bằng `getDownloadURL` (cùng tham số notebook cell 21 dùng) |
| Cell 34: `get_viirs_monthly_composite` (VCMSLCFG, dự phòng VCMCFG, mean, clip) | cùng tên | Không |
| Cell 36: `night_img.toDouble()`, scale 500, EPSG:4326 | `process_commune()` phần Task 3.1 | `toDrive` thay bằng `getDownloadURL` |
| Cell 38: 6 reducer, ngưỡng 1.5 nW, 25 ha/pixel, CV, `TNL_MA3`, `TNL_MOM_GROWTH_PCT` | `compute_ntl_indices()` | Bọc thành hàm; báo lỗi rõ nếu không tháng nào có ảnh (notebook sẽ gặp KeyError) |


## Phần vỏ (viết mới) và lý do không ảnh hưởng giá trị pixel

| Phần | Cách làm | Vì sao giá trị không đổi |
|---|---|---|
| Tải ảnh | `getDownloadURL` với `region=commune_geom`, `scale`, `crs="EPSG:4326"`, `GEO_TIFF`, `filePerBand=False`, xử lý file zip | Đúng tham số cell 21 của notebook; Earth Engine tính cùng ảnh trên cùng lưới |
| Nén GeoTIFF | DEFLATE, predictor 3 | Nén không mất dữ liệu; đã thử ghi rồi đọc lại, mảng trùng từng bit |
| Xã quá lớn | Chia khung bao thành N×N ô, mỗi ô tải với cùng `scale` và `crs`, ghép bằng vị trí pixel nguyên | Không resample; code từ chối ghép nếu các ô lệch lưới hoặc phần chồng lấn khác giá trị. Preflight tải một vùng nhỏ theo hai cách (nguyên và 2×2 ô) và so từng pixel |
| Kiểm tra sau tải | Mở lại file: số kênh, CRS, đọc được | Chỉ đọc |
| Trạng thái, chạy tiếp | Ghi theo từng phần (T1, từng tháng T2, T3, T3.2, T4); lần sau chỉ làm phần thiếu | Phần đã xong không bị tính lại |
| Danh sách xã | `load_targets()`: `full` lấy mọi xã GADM; `pilot` tự lấy phường đầu tiên và xã đầu tiên | Không có bước chọn bằng tay |
| Đồng bộ Drive | `rclone move` cho ảnh (chỉ xóa bản trên máy sau khi Drive đã nhận đủ), `rclone copy` cho CSV | Không đụng tới nội dung file |
| Gộp CSV | `concat`, ép `YEAR`, `MONTH`, `ANALYSIS_MONTH`, `LIT_PIXELS` về số nguyên | Chỉ đổi kiểu hiển thị, tránh `2024.0` |
