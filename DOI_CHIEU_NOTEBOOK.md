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
| Cell 15: `export_province_s2_local` + vòng 12 tháng | `task1_all_months()` | Cùng bộ lọc `CLOUDY_PIXEL_PERCENTAGE < 85`, cùng nhánh "rỗng thì dùng toàn bộ cảnh", cùng median, `reduceRegions` (mean + stdDev, scale 50, tileScale 4, EPSG:4326), tháng không có cảnh thì bỏ. Nhánh `if` chuyển thành `ee.Algorithms.If` phía máy chủ, các tháng cần bổ sung gom thành **1 lần gọi** |
| Cell 18: `mask_s2_clean`, `get_adaptive_monthly_composite` (±15, ±30 ngày) | `mask_s2_clean`, `fetch_plan()` + `day_image()` | Số cảnh của 3 cửa sổ × 12 tháng lấy trong **1 lần gọi**; Python chọn cửa sổ đúng thứ tự `if` của notebook rồi dựng ảnh `col.map(mask_s2_clean).median()` |
| Cell 19: `add_indices(...).clip(commune_geom)`, scale 20, EPSG:4326 | `day_image()` + tải song song | `getDownloadURL` thay `toDrive`; mặc định lưu đủ 10 kênh, giá trị giữ nguyên |
| Cell 34: `get_viirs_monthly_composite` (VCMSLCFG, dự phòng VCMCFG) | `fetch_plan()` + `night_image()` | Kiểm tra 2 bộ VIIRS trong kế hoạch riêng của giai đoạn đêm |
| Cell 36: `.toDouble()`, scale 500 | `night_image()` | Không đổi giá trị; nén không mất dữ liệu |
| Cell 38: 6 reducer, ngưỡng 1.5 nW, 25 ha/pixel, CV, `TNL_MA3`, `TNL_MOM_GROWTH_PCT` | `task3_all_months()` | 4 phép `reduceRegion` × các tháng cần bổ sung gom thành **1 lần gọi**; phần tính chỉ số trên kết quả chép nguyên văn |

## Điều phối thay đổi sau v6

- `Engine` trong `vngis_2024.py` giữ lịch ngày → kiểm kê hợp lệ → đêm trên toàn phạm vi. `fetch_plan(..., phase)` chỉ truy vấn nguồn của giai đoạn hiện tại.
- `task1_all_months(..., months)` giữ nguyên phép tính v6, chỉ dựng graph cho tháng cần bổ sung. Task 3.2 vẫn tính chuỗi nguồn v6 rồi chỉ ghi sửa các tháng thiếu/lỗi.
- Hai CSV thêm hành chính chuẩn bằng GID, cột tiền tố viết thường, 12 dòng/GID; tháng no_source để trống. Không thay đổi công thức chỉ số hoặc cách xử lý chuỗi có tháng thiếu của v6.
- Ảnh ngày luôn đủ 10 kênh float; không dùng các tùy chọn int16/6 kênh của ZIP. Ảnh đêm vẫn Float64.
- Request EE và tải ảnh có giới hạn dùng chung, pacing/cooldown/retry và checkpoint. Việc thay đổi này không thay cửa sổ, scale, reducer hoặc giá trị pixel.
- `V6_SCIENTIFIC_HASHES` trong `verify_pilot.py` lưu hash AST từ ZIP v6 (Python 3.11) cho các hàm khoa học. Kiểm thử so sánh mã và Drive giả không thay thế đối chiếu số liệu thật với notebook gốc.
