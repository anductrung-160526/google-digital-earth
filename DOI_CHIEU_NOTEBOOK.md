# Đối chiếu notebook và pipeline

Nguồn: `VNGISDash_Task123_Merged_final.ipynb`. Pipeline giữ 3 chức năng: trích chỉ số ảnh ngày (Task 1) và đêm (Task 3.2), lấy tif ngày (Task 2), lấy tif đêm (Task 3.1). Bảng chọn Tỉnh → Xã (cell 13) và các cell phân tích đường, nhà xưởng, mặt nước (cell 21 đến 32) đã bỏ.

## Phần khoa học

| Notebook | Pipeline (`vngis_2024.py`) | Khác gì |
|---|---|---|
| Cell 11: `PROJECT_ID`, `ASSET_ID` | cùng tên | Xác thực bằng service account |
| Cell 3: GADM 4.1, cột `GID_1..TYPE_3` | `build_admin_table()` | Đọc thẳng file `.dbf` (không cần geopandas), cùng dữ liệu |
| Cell 13: tên xã = `TYPE_3 NAME_3` | `commune_full_name()` | Giữ để đặt tên thư mục; danh sách xã lấy tự động |
| Cell 15, 17: `normalize_str` | `normalize_str_t1()`, `normalize_str()` | Không |
| Cell 15: `mask_s2_sr`, `add_indices` | cùng tên | Không |
| Cell 15: `export_province_s2_local` + vòng 12 tháng | `task1_all_months()` | Cùng bộ lọc `CLOUDY_PIXEL_PERCENTAGE < 85`, cùng nhánh "rỗng thì dùng toàn bộ cảnh", cùng median, `reduceRegions` (mean + stdDev, scale 50, tileScale 4, EPSG:4326), tháng không có cảnh thì bỏ. Nhánh `if` chuyển thành `ee.Algorithms.If` phía máy chủ, 12 tháng gom thành **1 lần gọi** |
| Cell 18: `mask_s2_clean`, `get_adaptive_monthly_composite` (±15, ±30 ngày) | `mask_s2_clean`, `fetch_plan()` + `day_image()` | Số cảnh của 3 cửa sổ × 12 tháng lấy trong **1 lần gọi**; Python chọn cửa sổ đúng thứ tự `if` của notebook rồi dựng ảnh `col.map(mask_s2_clean).median()` |
| Cell 19: `add_indices(...).clip(commune_geom)`, scale 20, EPSG:4326 | `day_image()` + tải song song | `getDownloadURL` thay `toDrive`; mặc định lưu đủ 10 kênh, giá trị giữ nguyên |
| Cell 34: `get_viirs_monthly_composite` (VCMSLCFG, dự phòng VCMCFG) | `fetch_plan()` + `night_image()` | Kiểm tra 2 bộ VIIRS trong cùng lần gọi kế hoạch |
| Cell 36: `.toDouble()`, scale 500 | `night_image()` | Không đổi giá trị; nén không mất dữ liệu |
| Cell 38: 6 reducer, ngưỡng 1.5 nW, 25 ha/pixel, CV, `TNL_MA3`, `TNL_MOM_GROWTH_PCT` | `task3_all_months()` | 4 phép `reduceRegion` × 12 tháng gom thành **1 lần gọi**; phần tính chỉ số trên kết quả chép nguyên văn |

## Tùy chọn giảm dung lượng ảnh ngày (mặc định TẮT, ảnh giữ như notebook)

| Thay đổi | Ảnh hưởng |
|---|---|
| `VNGIS_DAY_FORMAT=int16`: lưu int16 = `round(giá trị × 10000)`, scale 0,0001 ghi trong file, NoData -32768 | Sai số tối đa 0,00005 (nửa bước 1e-4, bằng độ chính xác gốc của Sentinel-2 SR). Đổi `VNGIS_DAY_SCALE=1000` thì sai số 0,0005, file nhỏ thêm khoảng 40% |
| `VNGIS_DAY_BANDS=6`: chỉ lưu BLUE..SWIR2 | NDVI, NDBI, MNDWI, BSI tính lại được từ 6 kênh |

CSV chỉ số (Task 1, Task 3.2) tính trên máy chủ Earth Engine từ ảnh gốc, **không** bị ảnh hưởng bởi hai thay đổi này. Ảnh đêm giữ Float64 như notebook.

## Phần vỏ

| Phần | Cách làm |
|---|---|
| Song song | Nhiều xã cùng lúc; trong 1 xã, 3 lệnh gọi (kế hoạch, Task 1, Task 3.2) chạy song song và 24 ảnh tải song song; tổng số lệnh gọi EE cùng lúc giới hạn bởi `VNGIS_EE_CONCURRENCY` (24) |
| Xã quá lớn | Chia ô cùng `scale` và `crs`, ghép theo vị trí pixel nguyên; từ chối ghép nếu lệch lưới |
| CSV toàn quốc | Mỗi xã ghi vào `_control/parts/*.jsonl`; cuối mỗi lượt dựng lại `CSV/day_indices.csv`, `CSV/night_indices.csv` (bỏ trùng theo `GID_3`, `MONTH`) |
| Trạng thái, chạy tiếp | Ghi theo từng phần; lần sau chỉ làm phần thiếu |
| Đồng bộ Drive | `rclone move` cho ảnh, `rclone copy` cho CSV và `_control` |
