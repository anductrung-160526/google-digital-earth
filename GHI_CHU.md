# Ghi chú v6 đã chỉnh sửa

Nền khoa học và cách tải trực tiếp lấy từ `vngis-github-repo-v6.zip`. Schema, điều phối và pacing/retry được gộp trong `vngis_2024.py`; kiểm thử offline trong `verify_pilot.py --self-test`.

- JSONL v6 tiếp tục được đọc để khôi phục parts, bằng chứng nguồn mới và bộ đếm thử. Trạng thái cũ `done`, `ok`, `none`, `not_in_asset` không chứng minh file hợp lệ hoặc nguồn rỗng.
- Tiến độ mới theo xã–tháng với bốn trường ngày/đêm độc lập và lỗi/thời điểm. CSV luôn đủ 12 dòng/GID, thông tin hành chính viết thường lấy từ GADM.
- Chạy toàn bộ ngày, kiểm kê lại dữ liệu đã upload, rồi mới chạy đêm. Preflight/kế hoạch nguồn cũng tách giai đoạn.
- Giữ đủ 10 kênh float ngày. Các hàm lưu int16 của ZIP còn trong nguồn tham chiếu v6 nhưng cấu hình chạy từ chối int16/6 kênh.
- Task 1 lọc `CLOUDY_PIXEL_PERCENTAGE < 85`, fallback cảnh trong tháng, không nới biên. Task 2 không lọc cảnh, nới ±15/±30 ngày. CSV ngày không tính lại từ ảnh TIFF Task 2.
- MA3 và tăng trưởng đêm vẫn tính trên các tháng có nguồn liên tiếp như v6; tháng trống chỉ được bổ sung vào CSV sau tính toán. Vì thế tăng trưởng sau một tháng thiếu nguồn vẫn so với dòng có nguồn trước đó.
- Lưới chia ô chỉ được dùng khi preflight đạt. Không giảm độ phân giải để né quota.
- Không có cấu hình bảo đảm hết 429. Chạy pilot với cấu hình thận trọng, xem log dịch vụ và tỷ lệ lỗi trước khi tăng tải.
- Kiểm thử offline không xác nhận quota, quyền, nguồn ảnh, tính trùng lưới hoặc số liệu EE/Drive thật. Xem hướng dẫn pilot và đối chiếu notebook trong `HUONG_DAN.md`.

## Dọn nhánh theo danh sách ZIP v6

Nhánh chỉ giữ đúng 8 đường dẫn trong ZIP: vngis_2024.py, verify_pilot.py, requirements.txt, .gitignore, HUONG_DAN.md, DOI_CHIEU_NOTEBOOK.md, GHI_CHU.md và .github/workflows/vngis-2024.yml. Các file phụ, thư mục tests, workflow tests riêng, notebook và báo cáo riêng đã được gỡ sau khi gộp tính năng vào hai file Python. Workflow v6 vẫn chạy CI offline trên PR/main và kiểm thử trước xử lý dữ liệu. 39 kiểm thử được giữ, gồm hash AST khoa học từ ZIP. Không xóa dữ liệu Drive hoặc chạy toàn quốc khi dọn repo.
