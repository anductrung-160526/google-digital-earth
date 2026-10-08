# Hướng dẫn VNGISDash 2024

Luồng hiện tại dùng batch export, hoàn tất ảnh và chỉ số ngày trước khi cho phép phần đêm. Xem [HUONG_DAN_BATCH.md](HUONG_DAN_BATCH.md) để kiểm kê dữ liệu đã có trong `VNGISDash_2024`, chuyển đổi CSV cũ và chạy tiếp phần thiếu.

Không xóa thư mục dữ liệu cũ khi nâng cấp. Workflow `vngis-2024.yml` cũng gọi luồng batch và không còn chạy ngày/đêm đồng thời. Các công thức khoa học vẫn nằm trong `vngis_2024.py`; bộ dữ liệu công khai dùng schema được định nghĩa tại `data_contract.py`.

Phạm vi yêu cầu là đúng 11.136 mã xã và 12 tháng năm 2024. Cần đối chiếu bộ địa giới chuẩn với asset Earth Engine trước khi xử lý; số lượng chưa được xác minh bằng dữ liệu thật trong môi trường cloud chưa có credentials.
