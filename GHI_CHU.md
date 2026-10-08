# Ghi chú và điểm cần xác nhận

## Chỗ pipeline làm khác so với bản prompt

1. **Trạng thái** lưu thành một file `.jsonl` mỗi lượt trong `_control/status/` (bản ghi sau cùng của mỗi xã có hiệu lực), thay cho 11.000 file `{GID_3}.json`. Kéo vài file nhỏ từ Drive nhanh hơn nhiều so với 11.000 file. Không cần lease vì workflow dùng `concurrency` nên mỗi lúc chỉ có một lượt chạy.
2. **Đã bỏ** bảng chọn Tỉnh → Xã và Task 4 (đường, nhà xưởng, mặt nước, OSM) theo yêu cầu. Thí điểm tự lấy `pilot_n` xã, không cần nhập mã.
3. **CSV** chỉ có 2 file gộp toàn quốc (`CSV/day_indices.csv`, `CSV/night_indices.csv`), không có CSV riêng từng xã. Thứ tự cột cố định theo danh sách cột trong notebook.
4. **Ảnh ngày** mặc định giữ như bản 4 (10 kênh, số thực, nén không mất dữ liệu). Chế độ int16 và 6 kênh vẫn có, bật bằng biến môi trường.

## Điểm trong notebook nên hỏi lại người hướng dẫn (pipeline giữ nguyên, chưa sửa)

1. **Task 1 và Task 2 dùng hai ảnh tổng hợp khác nhau** cho cùng một tháng: Task 1 lọc cảnh `CLOUDY_PIXEL_PERCENTAGE < 85` và không nới biên; Task 2 không lọc cảnh và nới biên ±15, ±30 ngày. Vì vậy chỉ số trong CSV Task 1 không tính từ đúng ảnh tif Task 2.
2. **Task 3.2 `TNL_MOM_GROWTH_PCT`** tính trên các dòng liên tiếp, nếu thiếu một tháng thì tăng trưởng sẽ so với tháng trước nữa.

## Giới hạn và giả định

- **Gom lệnh gọi phía máy chủ** dựa vào việc `ee.Algorithms.If` chỉ tính nhánh được chọn. Bước đối chiếu với notebook ở thí điểm (CSV lệch tối đa 1e-6) là bước xác nhận.

- Chưa chạy được với Earth Engine và Drive thật từ môi trường của tôi. Các phần đã kiểm tra offline: đặt tên file, luồng trạng thái và chạy tiếp, ghép ô ảnh, nén không mất dữ liệu, tự chọn xã thí điểm, tải song song, lưu int16, dựng CSV toàn quốc, `verify_pilot.py`. Bước thí điểm là bước xác nhận thật.
- Cách chia ô giả định Earth Engine đặt lưới pixel theo gốc tọa độ khi dùng `scale` + `crs`. Preflight kiểm tra giả định này trên dữ liệu thật; nếu không đạt, xã cần chia ô sẽ báo lỗi thay vì ghép sai. Ảnh ghép có thể dư vài hàng hoặc cột NoData ở viền so với ảnh tải nguyên, giá trị các pixel còn lại không đổi.
- Mã xã lấy từ GADM 4.1; xã nào không có trong asset `communes_l3` được đánh dấu `not_in_asset` và bỏ qua.
