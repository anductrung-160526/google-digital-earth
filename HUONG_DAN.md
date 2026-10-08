# VNGISDash 2024 v6: ngày trước, đêm sau, chạy tiếp từ Drive

Bản này lấy `vngis-github-repo-v6.zip` làm nền, dùng `getDownloadURL` tải trực tiếp theo xã. Luồng batch export/Colab của các phiên bản sau đã được gỡ khỏi repo. Nhánh chỉ giữ đúng 8 file có trong ZIP; schema, điều phối, retry và kiểm thử được gộp vào hai file Python v6.

## Chuẩn bị

- Python 3.11, `python -m pip install -r requirements.txt`, rclone và hai secrets GitHub Actions: `RCLONE_CONF` (remote `gdrive`) và `EE_SERVICE_ACCOUNT_JSON`.
- Project mặc định `vngis-ee-2`, asset `projects/vngis-ee-2/assets/communes_l3`. Có thể cấu hình `VNGIS_EE_PROJECT`/`VNGIS_EE_ASSET` khi chạy CLI; workflow đặt rõ hai giá trị này để dùng cấu hình đang có. Cấp quyền Earth Engine và Drive phù hợp cho tài khoản. Không đưa khóa vào repo hoặc chat.
- Phạm vi đầy đủ: **11.163 đơn vị GADM 4.1 cấp 3**, gồm 8.972 xã, 1.586 phường, 601 thị trấn, 2 trung tâm huấn luyện và 2 đảo. Không tự bỏ đơn vị khác loại xã. Đây là phạm vi GADM đã chọn, không phải xác nhận địa giới chính thức tại mọi thời điểm năm 2024.
- Pipeline kiểm tra cả tập GID trong asset, mã trùng và mã ngoài phạm vi. GID thiếu gây lỗi và chặn phần đêm, không trở thành `not_in_asset` rồi bị coi hoàn tất.

## Kiểm kê trước khi chạy tiếp

Sau khi merge bản sửa, chọn **Actions → VNGISDash 2024 v6 (ngày trước, đêm sau) → Run workflow**. Dùng lượt chạy mới để lấy mã mới.

1. `mode=full`, `step=inventory`: kiểm kê `VNGISDash_2024` và chuyển schema CSV cũ khi cần; không khởi tạo EE hoặc gửi export. Bước này có ghi cache, tiến độ và CSV đã chuyển đổi có sao lưu.
2. Xem log `KIỂM KÊ` và `_control/progress.csv`: mỗi xã–tháng có bốn trường độc lập `day_image`, `day_indices`, `night_image`, `night_indices`, cột lỗi tương ứng và `updated_at`.
3. Dùng `mode=pilot`, `pilot_n=2`, `step=run` để thử tại **VNGISDash_2024_PILOT**. Xã được chọn ổn định theo GID, xen kẽ phường/xã; không tự tăng workers theo pilot_n. Báo cáo xác minh nằm ở Summary và `_control/verify_report.md`.
4. Khi pilot đạt và đã đối chiếu với notebook khoa học, tự chọn `mode=full`, `step=run` để chạy tiếp dữ liệu thật.

Có thể chạy CLI: `python vngis_2024.py inventory` hoặc `python vngis_2024.py run`. Đặt `VNGIS_MODE` và thư mục trước khi import/chạy; mặc định là pilot.

Kiểm kê không tin `done`, `ok`, `none` trong JSONL cũ. Nó đối chiếu CSV và file thực tế theo GID/tháng, đọc mọi block TIFF chưa có cache, kiểm tra kênh, CRS, lưới pixel và dtype. Lần đầu có thể lâu vì phải tải toàn bộ ảnh chưa được xác minh. Log cho biết đang liệt kê, tải, đọc hoặc lưu dữ liệu; không thể ước lượng thời gian chỉ từ số xã.

Cache `_control/validated_images.json` được tái sử dụng khi đường dẫn, kích thước, mtime, hash do Drive cung cấp và phiên bản kiểm tra không đổi. Cache được upload sau 250 kiểm tra mới hoặc khi qua 120 giây tại điểm kết thúc kiểm tra một ảnh; đổi ngưỡng bằng `VNGIS_INVENTORY_CHECKPOINT_EVERY`. Khi ngắt có xử lý, chương trình cố lưu phần đã kiểm tra; dừng cưỡng bức chỉ giữ checkpoint đã upload. File lỗi giữ `failed`, được kiểm tra lại khi fingerprint thay đổi.

Ảnh đủ nhưng chỉ số thiếu: chỉ chạy lại phép tính chỉ số. Chỉ số đủ nhưng ảnh thiếu: chỉ tải ảnh thiếu. Task 1 chỉ tính các tháng còn thiếu. Task 3.2 cần tính chuỗi các tháng có nguồn để giữ rolling/pct_change v6, nhưng chỉ ghi bổ sung tháng thiếu/lỗi; không thay số liệu đã hợp lệ.

## Thứ tự và chất lượng khoa học

- Toàn bộ ảnh/chỉ số ngày phải là `done` hoặc `no_source` hợp lệ trước khi preflight, truy vấn nguồn, tải ảnh hay tính chỉ số đêm bắt đầu. Hoàn tất ngày của một xã không cho phép chạy đêm xã đó sớm.
- Ngày: Sentinel-2, đủ 10 kênh số thực, tải ở 20 m. Task 1 dùng scale 50, tileScale 4, bộ lọc mây và cửa sổ nguyên bản v6; Task 2 giữ cửa sổ tháng/±15/±30 ngày. Task 1 và Task 2 không dùng cùng ảnh tổng hợp.
- Đêm: VIIRS, ưu tiên VCMSLCFG rồi VCMCFG, 2 kênh, Float64, 500 m. Các reducer, ngưỡng 1.5, diện tích 25 ha/pixel, MA3 và tăng trưởng giữ nguyên v6.
- Không bật 6 kênh/int16. Chia ô giữ cùng lưới/scale; chỉ được dùng sau khi preflight so sánh từng pixel thành công. Mặc định pilot không thử chia ô, nên ảnh vượt giới hạn sẽ báo lỗi thay vì ghép lưới chưa được xác minh; đặt `VNGIS_PREFLIGHT_TILE_TEST=true` khi cần thử pilot có chia ô.
- `no_source` chỉ xuất hiện khi truy vấn nguồn thành công trả count=0. 429, thiếu file và lỗi tính toán không trở thành `no_source`. Nguồn có cảnh nhưng không trả đủ chỉ số vẫn là lỗi cần kiểm tra, không thay NaN bằng 0.

## Schema và lưu trữ

```text
VNGISDash_2024/
  Day/<GID_1>_<tỉnh>/<GID_3>_<xã>/<GID đã đổi dấu chấm>_day_2024MM.tif
  Night/<GID_1>_<tỉnh>/<GID_3>_<xã>/<GID đã đổi dấu chấm>_night_2024MM.tif
  CSV/day_indices.csv
  CSV/night_indices.csv
  _control/progress.csv
  _control/source_counts.csv
  _control/validated_images.json
  _control/status/*.jsonl
  _control/parts/*.jsonl
  _control/backups/...
  _control/logs/...
```

Hai CSV bắt đầu bằng `gid_3,name_3,type_3,gid_2,name_2,gid_1,name_1,year,month`. Theo sau là 20 chỉ số ngày hoặc 15 chỉ số đêm. Mỗi GID có 12 dòng năm 2024; tháng không có nguồn để trống các chỉ số. Khóa `(gid_3,year,month)` duy nhất; thứ tự GID tự nhiên, rồi tên xã/năm/tháng. Hành chính ghép bằng GID từ GADM. CSV cũ hợp lệ được chuyển schema, không tính lại ảnh. Parts JSONL v6 được dùng để khôi phục chỉ số thiếu; mâu thuẫn chưa giải quyết ở cùng khóa sẽ báo lỗi.

CSV và ảnh phải thay thế được sao lưu dưới `_control/backups/<stamp>/...`; dữ liệu đi qua staging rồi mới đổi tên chính thức. Không có lệnh xóa thư mục dữ liệu Drive. Các ảnh batch dùng chung còn trên Drive được giữ nguyên nhưng luồng v6 không tiêu thụ chúng.

Checkpoint CSV/nguồn/trạng thái/cache theo `VNGIS_UPLOAD_EVERY_SEC=300` tại điểm hoàn tất phép tính/ảnh/xã, cuối vòng và cuối lượt. Ảnh mới chỉ `done` sau kiểm tra đầy đủ và upload thành công. Outbox bất biến trên runner giữ upload chưa gửi xong; bước `--sync-only` gửi nốt outbox, không copy tùy tiện CSV cũ. Outbox cục bộ không sống qua việc runner bị xóa; lượt sau vẫn kiểm kê Drive để sửa phần chưa upload.

## Tham số hạn chế 429

| Biến | Khởi đầu |
|---|---:|
| VNGIS_WORKERS | 2 |
| VNGIS_MONTH_THREADS | 2 |
| VNGIS_EE_CONCURRENCY | 4 |
| VNGIS_EE_QPS | 2 |
| VNGIS_RCLONE_TRANSFERS | 2 |
| VNGIS_RCLONE_CHECKERS | 4 |
| VNGIS_RCLONE_TPSLIMIT | 2 |
| VNGIS_RCLONE_TPSLIMIT_BURST | 2 |
| VNGIS_UPLOAD_EVERY_SEC | 300 |
| VNGIS_REQUEST_ATTEMPTS | 6 |
| VNGIS_MAX_ATTEMPTS | 3 |

Workers, month_threads, EE concurrency/QPS, Drive transfers/TPS được chọn trong workflow và chuyển sang lượt sau. Checker/burst và các cấu hình còn lại đặt trong env workflow; CLI có thể đặt tất cả. Giới hạn 4 dùng chung cho lệnh EE và HTTP tải ảnh; tốc độ 2 áp dụng chung cho hai nhóm này. Lệnh rclone được tuần tự hóa, mọi lệnh nhận giới hạn TPS/checker/transfer; không chỉ upload.

429 có `Retry-After` thì chờ theo header; nếu không, exponential backoff có jitter từ 5 đến tối đa 120 giây. Cooldown dùng chung khiến worker khác không tiếp tục dồn request. Semaphore được nhả trong lúc chờ; STOP/deadline ngắt được việc chờ EE/tải ảnh. Drive/rclone dùng backoff cùng bộ điều khiển; rclone tự giới hạn TPS. Log phân biệt Earth Engine, Image download và Google Drive, không in URL có chữ ký/khóa.

Không có tham số bảo đảm hết 429. Giữ mặc định qua pilot; nếu ổn định, tăng từng mức và theo dõi tỷ lệ lỗi/thời gian. Khi vẫn có 429, giảm workers và month_threads về 1, EE concurrency về 2, EE QPS/Drive TPS về 1. Giữ chất lượng ảnh và endpoint standard; không tự đổi high-volume.

## Dừng, lỗi và nối lượt

- File `STOP` trong repo hoặc `_control/STOP` trên Drive dừng chuỗi. Poll Drive/repo mỗi 300 giây; tín hiệu TERM cũng dừng có xử lý. Để chạy tiếp, bỏ file STOP và Run workflow mới.
- Mỗi lượt có deadline 18.900 giây, timeout job 350 phút; thời gian đó dành cho cả kiểm kê và xử lý. Deadline trả mã 3, workflow tự nối và giữ tham số. `retry_failed` chuyển về false ở lượt nối để không reset bộ đếm vô hạn.
- Mã 0: bước yêu cầu hoàn tất; 1: lỗi cấu hình, quyền, kiểm tra hoặc hết số lần thử; 3: hết giờ, chạy tiếp; 130: người dùng/STOP dừng. Lỗi không tự nối vô hạn hoặc giả thành công. Mã 0 của `inventory` không có nghĩa dữ liệu toàn quốc đã đủ.
- Mỗi xã/giai đoạn tối đa 3 lần thử, lưu trong JSONL. Sau khi đã xử lý nguyên nhân, chọn `retry_failed=true` trong Run workflow để cho phép thử lại các phần còn thiếu. File đã hợp lệ vẫn được bỏ qua.
- Đừng dùng Cancel chỉ để tăng tốc; runner bị dừng cưỡng bức có thể chưa upload checkpoint cuối. Concurrency dùng chung `vngis-2024`, không tự hủy lượt đang chạy. Lượt workflow cũ đang chạy không tự nhận bản sửa này.

## Kiểm thử và giới hạn xác minh

`python verify_pilot.py --self-test` chạy schema, kiểm kê bằng Drive giả có file TIFF thật, chạy tiếp, thứ tự giai đoạn, 429/Retry-After, outbox và kiểm tra AST các hàm khoa học so với v6. `verify_pilot.py --remote gdrive:VNGISDash_2024_PILOT` kiểm tra dữ liệu thật sau pilot; báo lỗi nếu không có xã để xác minh.

Notebook khoa học gốc không nằm trong ZIP; repo không giữ thêm notebook điều khiển ngoài danh sách v6. Phải đối chiếu pilot với notebook khoa học khi có dữ liệu tham chiếu: `verify_pilot.py --root <thư mục đã mount> --notebook-dir <kết quả notebook> --notebook-gid <GID>`. Sai số CSV tối đa 1e-6; TIFF float yêu cầu giá trị pixel khớp trên cùng lưới.

Chưa xác minh Earth Engine/Drive thật trong môi trường phát triển nếu thiếu credentials khả dụng. Không tự chạy toàn quốc khi bàn giao.
