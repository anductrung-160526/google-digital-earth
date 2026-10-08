# Lấy dữ liệu theo lô (batch export)

Earth Engine tự tính trên máy chủ và ghi vào thư mục tạm `VNGIS_EXPORT_2024` trên Drive. GitHub Actions cắt ảnh theo xã, gộp CSV, ghi vào `VNGISDash_2024` đúng cấu trúc cũ rồi xóa file tạm. Cách này không bị lỗi 429 và tốn rất ít quota tương tác.

## Bước 0. Chuẩn bị

1. Tắt pipeline cũ: tab **Actions**, chọn **VNGISDash 2024 (chạy nối lượt)**, bấm **…** > **Disable workflow**. Nếu còn lượt đang chạy thì **Cancel**.
2. Nếu trên Drive có file `VNGISDash_2024/_control/STOP`, xóa nó đi (file này cũng dừng workflow mới).
3. Đưa các file mới lên repo: `batch_config.py`, `colab_export.py`, `process_exports.py`, `.github/workflows/vngis-batch.yml`, `VNGIS_batch_colab.ipynb`, `HUONG_DAN_BATCH.md`.
4. Secret `EE_SERVICE_ACCOUNT_JSON` nên là khóa của service account thuộc `vngis-ee-2`. Workflow dùng nó để xem tác vụ export đã xong chưa.

## Bước 1. Kiểm kê ban đầu

**Actions** > **VNGISDash 2024 batch** > **Run workflow**, chọn `step = inventory`. Kết quả nằm trong `VNGISDash_2024/_control/progress.csv` và tab Summary của lượt chạy.

## Bước 2. Phần ngày (ưu tiên)

**Trên Colab** (mở `VNGIS_batch_colab.ipynb`, chạy lần lượt):
1. Tải lên 3 file: `vngis_2024.py`, `batch_config.py`, `colab_export.py`.
2. `cx.init()`: đăng nhập bằng email chủ project `vngis-ee-2`, cho phép mount Drive.
3. `cx.submit_communes()` và `cx.quick_check()`: `quick_check` in ra số liệu mẫu, nếu báo lỗi thì dừng lại và gửi lỗi.
4. `cx.compute_day_plan()`: chọn cửa sổ thời gian cho từng xã và từng tháng, ghi `plan_day.json` vào Drive.
5. `cx.submit_day_csv()` và `cx.submit_day_images()`.
6. `cx.status()` để xem tiến độ. Có thể đóng Colab, tác vụ vẫn chạy trên Earth Engine.

**Trên GitHub:** Run workflow với `step = day`, `compare = 20`. Workflow tự chờ tác vụ export, cắt ảnh khi xong, và tự nối lượt.

**Kiểm tra lượt đầu:** mở Summary hoặc file `_control/compare_day_img_202401_w0.csv`. File này so 20 xã đã có ảnh cũ với ảnh cắt mới.
- `KHỚP`: tiếp tục.
- `KHÁC LƯỚI` hoặc `LỆCH GIÁ TRỊ`: tạo file `STOP_BATCH` ở gốc repo để dừng, rồi gửi file compare.

## Bước 3. Đối chiếu sang phần đêm

Khi phần ngày xong (Summary báo đủ nhóm, `progress.csv` cột `day_status` gần hết `done`):
1. Colab: `cx.submit_night_csv()` và `cx.submit_night_images()`.
2. GitHub: Run workflow với `step = night`. Xã nào đã có ảnh đêm sẽ được bỏ qua, chỉ lấy phần còn thiếu.

## Bước 4. Kiểm kê cuối

Run workflow với `step = inventory`. Xem các cột `day_tif_missing` và `night_tif_missing` trong `progress.csv`.

## Đầu ra

Giữ nguyên cấu trúc cũ:
- `Day/<GID_1>_<tỉnh>/<GID_3>_<xã>/<GID_3>_day_2024MM.tif`: float, 10 kênh, 20 m, EPSG:4326.
- `Night/.../<GID_3>_night_2024MM.tif`: 2 kênh `avg_rad`, `cf_cvg`, 500 m.
- `CSV/day_indices.csv`, `CSV/night_indices.csv`: gộp toàn quốc, cùng cột như trước.
- `_control/progress.csv`: tiến độ từng xã. `_control/batch_done.txt`: các nhóm ảnh đã cắt xong.

## Khác biệt so với tải từng xã

- **Ảnh ngày:** median từng pixel chỉ dùng các cảnh phủ pixel đó, nên giá trị bên trong xã giống hệt. Mỗi xã vẫn dùng đúng cửa sổ thời gian của nó (tháng, ±15 hoặc ±30 ngày). Pixel ngay trên ranh giới xã có thể khác vài điểm do cách xác định "pixel thuộc xã". Bước `compare` dùng để đo đúng chênh lệch này.
- **CSV ngày:** tính theo tỉnh, đúng phạm vi `export_province_s2_local` của notebook (lọc mây và nhánh "không có cảnh đạt thì dùng toàn bộ" xét trên cả tỉnh). Pipeline tải từng xã trước đây xét trên từng xã, nên một số tháng mây nhiều có thể khác nhẹ. Bản batch khớp notebook hơn.
- **CSV đêm:** mỗi xã vẫn `clip` và `reduceRegion` như notebook. Phần tính CV, MA3, tăng trưởng chép nguyên văn.

## Dừng và xử lý lỗi

- Dừng workflow: tạo file `STOP_BATCH` ở gốc repo.
- Tác vụ export lỗi: trong Colab chạy `cx.status()` để xem lỗi, sau đó gửi lại, ví dụ `cx.submit_day_images(months=[3], force=True)`.
- Lỗi "vị trí thật khác tên file": thêm biến `VNGIS_TILE_ORDER: colrow` vào phần `env` của workflow rồi chạy lại.
