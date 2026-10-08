# VNGIS 2024: ngày trước, đêm sau, chạy tiếp từ Drive

Pipeline dùng Earth Engine batch export, rồi cắt ảnh theo xã trên GitHub Actions. Thư mục đích là **VNGISDash_2024**; export dùng chung nằm trong **VNGIS_EXPORT_2024** và được giữ lại để sửa phần thiếu. Không xóa thư mục đích hay export khi nâng cấp.

## Chuẩn bị và phạm vi địa giới

- Dùng các file mới cùng phiên bản: `vngis_2024.py`, `batch_config.py`, `data_contract.py`, `colab_export.py`, `process_exports.py`, `verify_pilot.py` và notebook.
- GitHub Actions cần secrets `RCLONE_CONF` (remote `gdrive`) và `EE_SERVICE_ACCOUNT_JSON` có quyền Earth Engine trên project `vngis-ee-2`. Không đưa khóa vào repo hoặc chat.
- Phạm vi đã chọn là toàn bộ **11.163 đơn vị cấp 3 GADM 4.1 Việt Nam**, với GID_3 duy nhất, đủ GID/NAME cấp 1, 2, 3 và TYPE_3. Asset `projects/vngis-ee-2/assets/communes_l3` phải chứa đúng tập mã đó; Colab đối chiếu cả tập mã, không chỉ số lượng.
- Bảng GADM thật đã được kiểm tra có 11.163 mã duy nhất, 63 tỉnh, không thiếu thông tin hành chính. TYPE_3 gồm 8.972 xã, 1.586 phường, 601 thị trấn, 2 trung tâm huấn luyện và 2 đảo; giữ toàn bộ theo lựa chọn phạm vi, không tự loại các loại đơn vị khác xã.
- Lỗi cũ `Phạm vi địa giới có 11,163 xã; yêu cầu 11,136` là do số kỳ vọng không khớp GADM. Số kỳ vọng đã đổi sang 11.163 theo lựa chọn rõ ràng của người dùng. Nếu còn lỗi `Asset không khớp địa giới`, cần đối chiếu GID_3 của asset với GADM, không chỉ sửa tổng số hoặc tự bỏ 27 đơn vị.
- Có thể cung cấp CSV địa giới cùng phạm vi bằng `VNGIS_ADMIN_FILE` (đường dẫn file; tên cột hành chính viết hoa hoặc viết thường). Dữ liệu ảnh/chỉ số vẫn thuộc 12 tháng năm 2024; lựa chọn GADM 4.1 không tự xác nhận đây là bộ địa giới chính thức tại mọi thời điểm trong năm 2024.
- Workflow cũ `vngis-2024.yml` hiện gọi cùng pipeline batch. CLI `python vngis_2024.py day` cũng chuyển sang batch; luồng cũ chạy ngày/đêm cùng lúc đã ngừng sử dụng.

## 1. Kiểm kê dữ liệu cũ

Chạy workflow **VNGISDash 2024 batch**, chọn `step=inventory`. Nó:

1. Đọc dữ liệu thật trong `Day`, `Night`, `CSV` và tiến độ trước đó.
2. Đọc mọi block của ảnh chưa được xác minh để phát hiện file hỏng; kiểm tra số kênh, CRS, kích thước pixel và kiểu dữ liệu. Lần đầu có thể tốn nhiều I/O. Các lượt sau tái sử dụng kết quả đã kiểm tra chỉ khi đường dẫn, kích thước, thời gian sửa, hash do Drive cung cấp và phiên bản kiểm tra không đổi.
3. Chuyển CSV cũ sang schema mới, ghép thông tin hành chính bằng GID. Sao lưu bản cũ vào `_control/backups/<thời điểm>/CSV/...` trước khi thay thế. Upload qua file tạm rồi chuyển thành tên chính thức; lỗi sao lưu/upload làm bước thất bại.
4. Ghi `_control/progress.csv`: một dòng mỗi xã–tháng. `done` cũ hoặc `batch_done.txt` không thể thay thế kiểm tra file thật.

Dữ liệu hợp lệ được giữ nguyên. CSV có chỉ số mâu thuẫn ở cùng khóa, GID ngoài phạm vi hoặc năm/tháng sai sẽ được báo lỗi để xử lý rõ ràng, không âm thầm chọn/xóa bản ghi.

## 2. Giai đoạn ngày

Mở `VNGIS_batch_colab.ipynb`, tải lên **4 file** `vngis_2024.py`, `batch_config.py`, `colab_export.py`, `data_contract.py`. Chạy:

```python
import colab_export as cx
cx.init()                 # xác thực, mount Drive, kiểm tra tập mã xã
cx.inventory()            # kiểm kê dữ liệu đích trước khi gửi export
cx.submit_communes()      # ranh giới để cắt ảnh; tái sử dụng export đã có
cx.quick_check()          # kiểm tra công thức NGÀY trên vài xã
cx.compute_day_plan()     # giữ cửa sổ tháng / ±15 / ±30 ngày
cx.submit_day_csv()
cx.submit_day_images()
cx.status()
```

Chỉ gửi tác vụ cho tỉnh/nhóm có phần thiếu. Một export có thể dùng chung cho nhiều xã: vẫn dùng phạm vi tỉnh/nhóm gốc để giữ nguyên công thức và tái sử dụng kết quả. Tác vụ đang READY/RUNNING không được gửi trùng kể cả `force=True`. Tác vụ COMPLETED còn file export được tái sử dụng; nếu file đã mất thì cho phép gửi lại.

Trên GitHub chạy `step=day`, có thể chọn `compare=20` để đối chiếu ảnh mới với ảnh cũ. Workflow gộp chỉ số ngày, cắt các ảnh ngày còn thiếu/hỏng, rồi kiểm kê lại. Chỉ khi **mọi xã–tháng** có cả `day_image` và `day_indices` bằng `done` hoặc `no_source` mới hoàn tất giai đoạn ngày. Không tự gửi hoặc chạy phần đêm.

## 3. Giai đoạn đêm

Sau khi phần ngày đạt, chạy trên Colab:

```python
cx.require_day_complete() # kiểm tra file thật; không chỉ đọc cờ done
cx.submit_night_csv()
cx.submit_night_images()
```

Chạy GitHub `step=night`. Cả Colab, CLI và GitHub đều chặn phần đêm nếu bất kỳ xã–tháng ngày còn pending/running/failed. Có thể chạy riêng `day-csv`, `day-img`, `night-csv`, `night-img`; các bước đêm vẫn phải qua cổng kiểm tra ngày.

Nếu đủ ảnh nhưng thiếu chỉ số, chỉ export/bổ sung chỉ số bằng các phép tính hiện có trên Earth Engine. Nếu đủ chỉ số nhưng thiếu ảnh, chỉ cắt/bổ sung ảnh. Không suy chỉ số CSV từ ảnh GeoTIFF ở độ phân giải khác với phép giảm mẫu khoa học.

## Schema và tháng không có nguồn

Hai CSV bắt đầu bằng:

```text
gid_3,name_3,type_3,gid_2,name_2,gid_1,name_1,year,month
```

- Ngày: tiếp theo là đủ 20 cột `mean`, `stdDev` của `BLUE,GREEN,RED,NIR,SWIR1,SWIR2,NDVI,NDBI,MNDWI,BSI`, theo thứ tự từng kênh rồi mean/stdDev.
- Đêm: tiếp theo là `TIME,COMMUNE_AREA_HA,TNL,MEAN_RAD,STD_RAD,MIN_RAD,MAX_RAD,SPATIAL_CV,LIT_PIXELS,LIT_AREA_HA,ELECTRIFICATION_RATIO_PCT,LIT_POP_PROXY,CLOUD_FREE_OBS,TNL_MA3,TNL_MOM_GROWTH_PCT`.
- Mỗi xã có đúng 12 dòng của 2024. Khóa duy nhất `(gid_3,year,month)`. Sắp xếp GID tự nhiên, tên xã, năm, tháng; tên xã lấy nguyên từ địa giới chuẩn.
- Dòng chưa xử lý được tạo với chỉ số trống và trạng thái pending/failed. Chỉ ghi `no_source` khi có số cảnh bằng 0 do Earth Engine trả về, không suy từ file thiếu hoặc dòng rỗng. Tháng không có nguồn có toàn bộ chỉ số trống, không thay bằng 0.
- `_control/source_counts.csv` lưu bằng chứng số cảnh riêng cho từng thành phần. Ảnh ngày có cửa sổ thích ứng, CSV ngày dùng phạm vi/tháng của công thức gốc, nên bằng chứng nguồn của chúng được lưu riêng.
- `TNL_MOM_GROWTH_PCT` tháng đầu có thể NaN, hoặc vô hạn khi TNL tháng trước bằng 0; đó là kết quả công thức cũ, không bị đổi thành 0.
- Export CSV ngày cũ không có `SOURCE_COUNT` vẫn giữ được các chỉ số hợp lệ. Nếu còn dòng rỗng không có bằng chứng nguồn, cần tạo lại export tỉnh đó bằng `cx.submit_day_csv(only=["GID_tỉnh"], force=True)` sau khi kiểm tra không có tác vụ đang chạy.

## Tiến độ, chạy tiếp và kiểm tra

`progress.csv` có cột hành chính + year/month, bốn trạng thái `day_image`, `day_indices`, `night_image`, `night_indices`, mỗi trạng thái có cột `_error`. Giá trị: `pending`, `running`, `done`, `no_source`, `failed`. Lượt bị ngắt còn running sẽ chuyển thành failed để chạy lại; file hợp lệ đã hoàn tất vẫn được bỏ qua.

Các thư mục đầu ra giữ nguyên:

```text
VNGISDash_2024/
  Day/<tỉnh>/<xã>/...
  Night/<tỉnh>/<xã>/...
  CSV/day_indices.csv
  CSV/night_indices.csv
  _control/progress.csv
  _control/source_counts.csv
  _control/validated_images.json
  _control/backups/...
  _control/logs/...
```

Dừng bằng file `STOP_BATCH` trong repo hoặc `_control/STOP` trên Drive. Chạy lại cùng step để tiếp tục. Mã thoát 0: phần được yêu cầu hoàn tất; 1: lỗi cần xử lý; 3: hết giờ/chờ export, workflow nối lượt. Upload hoặc quyền đọc lỗi không bị coi là file chưa tồn tại.

Mặc định cắt ảnh với 2 worker để hạn chế RAM (`VNGIS_CUT_WORKERS`). `VNGIS_BATCH_WORK` đổi thư mục làm việc cục bộ. Môi trường cloud có thể đặt các đường dẫn dưới `/workspace` thay cho home. Không hứa thời gian hoàn tất toàn quốc khi chưa đo quota Earth Engine và I/O thực tế.

Kiểm thử độc lập:

```bash
python -m unittest discover -s tests -v
python verify_pilot.py --root /duong/dan/VNGISDash_2024 --gids VNM.x.y.z_1
```

Bộ kiểm tra đầu ra yêu cầu schema mới, đúng 12 tháng và ảnh hợp lệ hoặc bằng chứng no_source; chạy không có xã nào sẽ thất bại. Kiểm thử mẫu không xác nhận quyền Google, số xã của asset thật hay dữ liệu thực trên Drive.
