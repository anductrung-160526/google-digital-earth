# Hướng dẫn VNGISDash 2024

Luồng hiện tại dùng batch export, hoàn tất ảnh và chỉ số ngày trước khi cho phép phần đêm. Xem [HUONG_DAN_BATCH.md](HUONG_DAN_BATCH.md) để kiểm kê dữ liệu đã có trong `VNGISDash_2024`, chuyển đổi CSV cũ và chạy tiếp phần thiếu.

Không xóa thư mục dữ liệu cũ khi nâng cấp. Workflow `vngis-2024.yml` cũng gọi luồng batch và không còn chạy ngày/đêm đồng thời. Các công thức khoa học vẫn nằm trong `vngis_2024.py`; bộ dữ liệu công khai dùng schema được định nghĩa tại `data_contract.py`.

Phạm vi đã chọn là toàn bộ **11.163 đơn vị cấp 3 của GADM 4.1 Việt Nam**, lấy dữ liệu 12 tháng năm 2024. Bảng GADM thật đã được kiểm tra có 11.163 GID_3 duy nhất, 63 tỉnh và không thiếu thông tin hành chính. Asset Earth Engine vẫn phải có đúng tập mã này; việc đối chiếu asset cần xác thực Google.
