# Bàn giao v6 đã chỉnh sửa

Cơ sở Git: `origin/main` tại `5b890ec` (merge phạm vi 11.163 GADM). ZIP người dùng: `vngis-github-repo-v6.zip`, SHA256 `8fb69cf8b541475c75a27e80b8b76700da517e2e19a380cbe810378473399749`.

Đã kiểm kê mọi file được Git theo dõi; checkout sạch trước sửa. Mọi file trong ZIP được khôi phục làm nền rồi mới sửa. Bản cuối là **v6 + các yêu cầu điều phối/schema/retry**, không phải bản sao byte-for-byte v6. Không reset/force-push, không tạo worktree, không chạy hoặc hủy workflow thật.

## File thuộc ZIP

| File | So với main trước sửa | Hành động cuối |
|---|---|---|
| vngis_2024.py | Khác | Khôi phục v6; giữ hàm khoa học, thêm chọn tháng Task 1, kế hoạch riêng từng giai đoạn, tải có pacing/retry; chuyển điều phối sang v6_runtime |
| verify_pilot.py | Khác | Khôi phục v6; kiểm tra schema tháng/ảnh/bằng chứng nguồn mới; bỏ tin done/none cũ và báo lỗi lượt xác minh rỗng |
| requirements.txt | Khác | Giữ nguyên yêu cầu thư viện v6, gồm pandas>=2.1,<3; không cần thư viện mới |
| .gitignore | Thiếu | Thêm đúng file v6 |
| HUONG_DAN.md | Khác | Khôi phục nền, cập nhật đầy đủ luồng v6 đã sửa; bỏ chỉ dẫn xóa Drive, hạ chất lượng và dự đoán pilot không có bằng chứng |
| DOI_CHIEU_NOTEBOOK.md | Giống | Giữ bảng đối chiếu khoa học; cập nhật lịch giai đoạn/schema và loại tùy chọn giảm chất lượng |
| GHI_CHU.md | Giống | Cập nhật trạng thái thực tế, giới hạn và khác biệt Task 1/2, chuỗi thời gian |
| .github/workflows/vngis-2024.yml | Khác | Khôi phục workflow trực tiếp v6, thêm inventory/run và tham số, kiểm thử trước chạy, báo lỗi đúng, nối chỉ mã 3, cài rclone qua APT có xác minh chữ ký |

Project mặc định cấu hình `vngis-ee-2` tiếp tục dùng project hiện tại, khác tên `digital-vietnam-earth` trong ZIP. Đây là cấu hình quyền/asset, không thay nguồn ảnh hoặc công thức. Asset phải có cùng tập GID GADM; CLI có thể đặt project/asset qua biến môi trường.

## File ngoài ZIP

| File | Hành động và lý do |
|---|---|
| data_contract.py | Giữ schema/kiểm tra độc lập đã có; sửa đọc count CSV rỗng và từ chối count phân số |
| tests/test_pipeline.py | Giữ kiểm thử hợp đồng; thay các bài kiểm thử worker batch không còn dùng bằng kiểm thử v6 |
| .github/workflows/tests.yml | Giữ CI offline trên PR/main |
| batch_config.py, colab_export.py, process_exports.py | Gỡ để không có entrypoint batch xung đột hoặc import thiếu sau đổi v6 |
| .github/workflows/vngis-batch.yml | Gỡ workflow batch; chỉ workflow v6 được dùng để chạy dữ liệu |
| HUONG_DAN_BATCH.md | Gỡ hướng dẫn luồng đã ngừng; thay bằng HUONG_DAN.md |
| VNGIS_batch_colab.ipynb | Gỡ notebook gửi batch; thêm VNGIS_v6.ipynb để điều khiển/check pilot trực tiếp |
| vngis-github-repo/vngis_2024.py | Gỡ bản runner trùng tên cũ có thể bị chạy nhầm |
| vngis-github-repo/requirements.txt | Gỡ manifest của runner cũ đã gỡ |
| vngis-github-repo/HUONG_DAN_GITHUB_ACTIONS.md | Gỡ hướng dẫn runner cũ |
| v6_runtime.py | Thêm inventory, cache/checkpoint, sao lưu/staging/outbox, lịch toàn bộ ngày→đêm và giới hạn lần thử |
| request_control.py | Thêm ngân sách concurrency chung, pacing, Retry-After, jitter/cooldown và khả năng ngắt chờ |
| tests/test_v6_runtime.py, tests/v6_scientific.sha256 | Thêm kiểm thử tích hợp với TIFF thật/Drive giả và hash AST khoa học từ ZIP |
| BAO_CAO_V6.md | Báo cáo này |

File ngoài ZIP được kiểm tra bằng danh sách Git, import và tham chiếu workflow/notebook. Các file bị gỡ đều là mã/tài liệu repo, không có thao tác gỡ dữ liệu Drive. Lịch sử Git và ZIP vẫn giữ được nền để đối chiếu.

## Xác minh và giới hạn

- **39 kiểm thử đã đạt** bằng Python 3.11/NumPy 2.2.6/pandas 2.3.3: schema đủ tháng, hành chính bằng GID, GID tự nhiên, trùng khóa, no_source, bảo toàn dữ liệu cũ, migration/backup, file hỏng/cache, ngắt kiểm kê/xử lý và chạy tiếp, phần ngày chặn đêm, không truy vấn nguồn ngoài giai đoạn, đủ chuỗi ngày→đêm, xác minh pilot, giới hạn thử qua restart, pacing và 429/Retry-After, outbox và backup không bị ghi đè.
- AST các hàm khoa học, tạo ảnh và ghép lưới được đối chiếu hash lấy từ ZIP v6; Task 1 chỉ thêm tham số giới hạn tháng. Kiểm tra này không xác nhận số liệu EE thật.
- Bảng GADM cache thực tế có 11.163 GID duy nhất và đủ thông tin hành chính; không âm thầm drop_duplicates.
- Script cài đặt môi trường được chạy lại thành công với dependency v6; rclone local backend được kiểm tra thật cho stat/staging/thay thế an toàn và giữ hai bản sao CSV.
- Workflow được kiểm tra bằng actionlint; yêu cầu mọi lệnh rclone đi qua bộ điều khiển chung. Retry SDK EE được tắt để ứng dụng quản lý số lần thử.
- Google credential variables có tên cấu hình nhưng file cục bộ chưa tồn tại. Bị chặn: đọc VNGISDash_2024 thật, đối chiếu GID asset thật, đo tỷ lệ 429 và chạy pilot/đối chiếu notebook trên EE thật. Không yêu cầu khóa trong chat, không chạy toàn quốc.
- ZIP không chứa notebook khoa học gốc. Notebook điều khiển mới không thay thế nó.
- Không thể bảo đảm hết 429 hoặc ước lượng thời gian toàn quốc khi chưa đo pilot. Đọc đầy đủ ảnh kiểm kê vẫn có chi phí I/O; quality giữ nguyên. Chia ô chỉ được dùng sau preflight thật thành công.
- Dừng cưỡng bức có thể mất checkpoint chưa upload; outbox cục bộ không tồn tại qua runner mới. Lượt mới kiểm kê Drive và chỉ làm phần thiếu/hỏng. Ảnh đã upload nhưng chưa có cache có thể phải đọc lại để xác minh.

## Chạy tiếp

Review/merge nhánh sửa, rồi Run workflow mới. Bắt đầu `mode=full, step=inventory` để kiểm kê dữ liệu thật (có backup trước migration), thử `mode=pilot, step=run` tại folder riêng, sau pilot đạt mới chọn `mode=full, step=run`. Giữ workers=2, month_threads=2, EE concurrency=4, EE QPS=2, Drive transfers/TPS=2, checkers=4, burst=2. Các xã hết giới hạn không tự được coi xong; sau sửa nguyên nhân có thể chọn retry_failed=true. Xem chi tiết trong HUONG_DAN.md.
