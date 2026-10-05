# Chạy VNGISDash 2024 trên GitHub Actions

Pipeline lấy dữ liệu năm 2024 của toàn bộ xã trong bảng GADM 4.1 và lưu vào một thư mục duy nhất trên Google Drive.

GitHub chỉ cho một job chạy tối đa 6 giờ. Workflow này chạy theo **lượt** khoảng 5 giờ 15 phút. Hết lượt, nó đồng bộ kết quả lên Drive rồi tự gọi lượt kế tiếp. Trạng thái từng xã nằm trên Drive, nên lượt sau luôn tiếp tục đúng chỗ dừng và không làm lại xã đã xong.

## 0. Trước khi bắt đầu

Bạn cần:

- Tài khoản GitHub.
- Quyền chỉnh sửa project Google Cloud `digital-vietnam-earth` (để tạo service account).
- Google Drive còn chỗ trống. Mức tối đa khoảng 870 GB nếu mỗi ảnh ngày 6–7 MB và đủ 12 tháng, thực tế ít hơn vì tháng nhiều mây không có ảnh. Drive chỉ cho tải lên tối đa khoảng 750 GB mỗi ngày.
- Một máy tính có trình duyệt để cài rclone và cấp quyền Drive (làm một lần).

Hai điều cần cân nhắc trước khi chạy:

1. **Repo nên để Public.** Runner chuẩn của GitHub miễn phí cho repo public. Repo private ở gói miễn phí chỉ có hạn mức phút mỗi tháng (theo tôi nhớ là 2.000 phút, bạn kiểm tra lại), không đủ để chạy liên tục. Repo public nghĩa là mã nguồn và log chạy ai cũng xem được. Khóa truy cập nằm trong Secrets nên không lộ.
2. **Điều khoản sử dụng.** GitHub yêu cầu dùng Actions đúng Terms of Service. Theo tôi nhớ, điều khoản giới hạn Actions cho CI/CD của chính dự án, nên một job xử lý dữ liệu chạy liên tục có thể bị coi là sai mục đích. Bạn nên đọc lại điều khoản Actions trước khi chạy dài ngày. Tài khoản bị khóa giữa chừng sẽ dừng cả đợt.

## 1. Tạo repo và đưa file lên

Repo cần có đúng các file sau:

```
vngis_2024.py
requirements.txt
.gitignore
.github/workflows/vngis-2024.yml
```

Cách dễ nhất: tạo repo mới (Public) trên github.com, giải nén gói file, rồi dùng **Add file ▸ Upload files** kéo cả thư mục vào. Thư mục `.github` là thư mục ẩn, trên Windows hoặc Mac cần bật hiển thị file ẩn mới kéo được. Hoặc dùng git:

```bash
git init
git add .
git commit -m "VNGISDash 2024"
git branch -M main
git remote add origin https://github.com/<tên-bạn>/<tên-repo>.git
git push -u origin main
```

Kiểm tra: tab **Actions** của repo hiện workflow **VNGISDash 2024 (chạy nối lượt)**. Không thấy nghĩa là file workflow chưa nằm đúng ở `.github/workflows/` trên nhánh `main`.

## 2. Tạo service account cho Earth Engine

Chạy không người trực nên không thể bấm đăng nhập Earth Engine. Cần một service account có khóa JSON.

1. Mở [console.cloud.google.com](https://console.cloud.google.com), chọn project `digital-vietnam-earth`.
2. **IAM & Admin ▸ Service Accounts ▸ Create service account**. Đặt tên `vngis-runner`.
3. Cấp cho tài khoản này hai vai trò ở cấp project (theo tài liệu Earth Engine, đây là cặp quyền cần để `ee.Initialize(project=...)` chạy được):
   - **Service Usage Consumer**
   - **Earth Engine Resource Viewer**
4. Mở service account vừa tạo ▸ tab **Keys ▸ Add key ▸ Create new key ▸ JSON**. File JSON tải về là khóa. Giữ kín và không đưa lên repo.

Ghi chú:

- Asset `communes_l3` nằm trong cùng project nên vai trò cấp ở project là đủ để đọc.
- Nếu Console không cho tạo khóa, tổ chức của bạn đang bật chính sách chặn tạo khóa service account. Nhờ quản trị tắt chính sách đó.
- Nếu lần chạy thử báo service account chưa đăng ký dùng Earth Engine, đăng ký theo trang [Earth Engine service accounts](https://developers.google.com/earth-engine/service_account).

## 3. Cấp quyền Google Drive cho rclone (làm một lần trên máy của bạn)

Cài rclone từ [rclone.org/downloads](https://rclone.org/downloads/), rồi chạy `rclone config` và trả lời lần lượt:

| Câu hỏi | Trả lời |
| --- | --- |
| `n/s/q>` | `n` (New remote) |
| `name>` | `gdrive` |
| `Storage>` | gõ `drive` (Google Drive) |
| `client_id>` và `client_secret>` | Enter, để trống |
| `scope>` | `1` (Full access) |
| `service_account_file>` | Enter |
| `Edit advanced config?` | `n` |
| `Use web browser to automatically authenticate?` | `y`, đăng nhập Google và cho phép |
| `Configure this as a Shared Drive?` | `n` |
| xác nhận | `y`, rồi `q` để thoát |

Kiểm tra: `rclone lsd gdrive:` phải liệt kê các thư mục trên Drive của bạn.

Lấy nội dung cấu hình: chạy `rclone config show gdrive` và copy **toàn bộ** kết quả, từ dòng `[gdrive]` đến hết dòng `token = {...}`. Nội dung này cho phép truy cập Drive của bạn nên chỉ dán vào GitHub Secrets.

Để trống `client_id` là cố ý: rclone dùng ứng dụng Google của chính rclone. Nếu bạn tạo ứng dụng Google riêng ở trạng thái "Testing", Google sẽ làm token hết hạn sau 7 ngày. Nếu sau này gặp lỗi giới hạn tốc độ, hãy tạo client riêng và chuyển ứng dụng sang "In production".

## 4. Thêm hai Secrets vào GitHub

Repo ▸ **Settings ▸ Secrets and variables ▸ Actions ▸ New repository secret**:

| Tên secret | Nội dung |
| --- | --- |
| `EE_SERVICE_ACCOUNT_JSON` | Toàn bộ nội dung file JSON ở bước 2 |
| `RCLONE_CONF` | Kết quả của `rclone config show gdrive` ở bước 3 |

Sau đó cất file JSON ở nơi an toàn hoặc xóa khỏi máy. File `.gitignore` đi kèm đã chặn các tên file khóa phổ biến, nhưng vẫn nên tự kiểm tra trước khi `git add`.

## 5. Chạy thử 10 xã

Tab **Actions ▸ VNGISDash 2024 (chạy nối lượt) ▸ Run workflow**, đặt `test_limit` = `10`, `workers` = `8`, bấm **Run workflow**. Mất vài phút đến vài chục phút.

Kiểm tra ba thứ:

1. Job **Chạy một lượt** xanh. Trong log của bước *Chạy pipeline* có các dòng `Earth Engine sẵn sàng bằng service account ...`, tốc độ `xã/giờ` và `HOÀN TẤT TOÀN BỘ`.
2. Trên Drive xuất hiện thư mục `VNGISDash_Communes_2024`. Mở `04_Status/progress.csv`: 10 dòng `status = done`, cột `osm_ok` = `True`. Nếu `osm_ok` là `False` hàng loạt, Overpass đang chặn IP của GitHub, xem bảng sự cố ở mục 9.
3. Mở một thư mục xã trong `03_Provinces/`: có `Day/`, `Night/VIIRS/` và 4 file trong `CSV/`.

Chạy thử không làm hỏng gì. Khi chạy thật, 10 xã đã xong được giữ nguyên và không bị làm lại. Muốn bắt đầu lại từ đầu, xóa thư mục `VNGISDash_Communes_2024` trên Drive.

## 6. Chạy thật

**Run workflow** với `test_limit` để trống. Lượt đầu chạy khoảng 5 giờ 15 phút rồi tự gọi lượt kế tiếp. Bạn có thể tắt máy.

Theo dõi tiến độ:

- Tab **Actions**: mỗi lượt là một dòng. Trang tóm tắt của mỗi lượt có số xã `done` / `partial` / `failed` / `pending`.
- Log bước *Chạy pipeline*: dòng `... xã/giờ | còn khoảng N giờ`.
- Drive: `04_Status/progress.csv` và thư mục `05_Logs/`.
- GitHub gửi email khi một lượt thất bại.

Khi hoàn tất, lượt cuối ghi `HOÀN TẤT TOÀN BỘ` và 4 CSV gộp toàn quốc nằm ở `02_Combined/`.

## 7. Dừng và tiếp tục

| Muốn | Làm |
| --- | --- |
| Dừng trật tự | Trên nhánh `main`: **Add file ▸ Create new file**, đặt tên `STOP`, commit. Trong tối đa 5 phút lượt đang chạy phát hiện, đồng bộ kết quả rồi thoát, và chuỗi không nối thêm lượt. |
| Dừng ngay | Tab **Actions ▸ lượt đang chạy ▸ Cancel workflow**. Phần làm dở (vài phút) sẽ được làm lại ở lượt sau. |
| Tiếp tục | Xóa file `STOP`, rồi **Run workflow**. Tiến độ cũ được giữ nhờ trạng thái trên Drive. |

File `STOP` phải nằm ở nhánh mặc định của repo.

## 8. Điều chỉnh

Trong file workflow, phần `env` của job `run`:

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `VNGIS_MAX_RUNTIME_SEC` | `18900` (5 giờ 15 phút) | Ngân sách thời gian mỗi lượt. Không đặt quá khoảng 19.500 vì GitHub hủy job ở 6 giờ và cần chừa thời gian đồng bộ. |
| `VNGIS_UPLOAD_EVERY_SEC` | `300` | Chu kỳ đẩy ảnh lên Drive. Ngắn thì ổ runner nhẹ hơn. |
| `VNGIS_FETCH_OSM` | `true` | Đổi thành `false` nếu Overpass chặn IP của GitHub. Các cột `osm_*` sẽ để trống. |
| `VNGIS_DRIVE_FOLDER` | `VNGISDash_Communes_2024` | Tên thư mục gốc trên Drive. |

Số xã song song chọn ở ô `workers` khi bấm Run workflow. Nếu log báo hết bộ nhớ hoặc hết ổ, giảm xuống 4 đến 6. Nếu Earth Engine báo quá nhiều yêu cầu đồng thời, cũng giảm.

## 9. Xử lý sự cố

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
| --- | --- | --- |
| `Thiếu secret RCLONE_CONF` hoặc `EE_SERVICE_ACCOUNT_JSON` | Chưa thêm hoặc sai tên secret | Thêm lại ở bước 4, tên phải khớp từng ký tự |
| `Caller does not have required permission` | Service account thiếu vai trò | Cấp đủ hai vai trò ở bước 2 |
| Báo service account chưa đăng ký Earth Engine | Chưa đăng ký | Làm theo ghi chú cuối bước 2 |
| `invalid_grant` hoặc `token expired` của rclone | Token Drive hết hạn hoặc bị thu hồi | Làm lại bước 3 rồi cập nhật secret `RCLONE_CONF` |
| `rateLimitExceeded` hoặc 403 của rclone | Vượt hạn mức API của ứng dụng rclone dùng chung | Giảm `workers`; nếu vẫn bị, tạo client Google riêng |
| `osm_ok` là `False` hàng loạt | Overpass chặn IP của GitHub | Đặt `VNGIS_FETCH_OSM` thành `false` hoặc chấp nhận cột OSM trống |
| Lượt kết thúc mã 2 và liên tục nối lượt | Earth Engine hoặc mạng gặp sự cố kéo dài | Chờ; xem log. Các xã lỗi trong sự cố không bị trừ lượt thử |
| `Resource not accessible by integration` ở bước nối lượt | Quyền workflow bị khóa ở cấp repo hoặc tổ chức | Settings ▸ Actions ▸ General ▸ Workflow permissions: cho phép workflow ghi |
| Workflow không hiện trong tab Actions | File sai đường dẫn hoặc chưa ở nhánh `main` | Phải là `.github/workflows/vngis-2024.yml` |
| Hết ổ đĩa trên runner | Quá nhiều xã chạy song song | Giảm `workers` hoặc `VNGIS_UPLOAD_EVERY_SEC` |

Mã thoát của script, hiện trong log bước *Chạy pipeline* (`Script thoát với mã N`):

| Mã | Ý nghĩa | Workflow làm gì |
| --- | --- | --- |
| 0 | Mọi xã đã xong | Dừng hẳn |
| 3 | Hết thời gian của lượt | Gọi lượt kế tiếp |
| 2 | Sự cố mạng hoặc Earth Engine kéo dài | Gọi lượt kế tiếp |
| 130 | Bị dừng tay (file `STOP`, hủy workflow) | Dừng |
| khác | Lỗi không lường trước | Báo lỗi và dừng. Xem log |

## 10. Giới hạn cần nhớ

- Mỗi job tối đa 6 giờ, mỗi workflow run tối đa 35 ngày (theo tài liệu GitHub). Chuỗi lượt ngắn tránh cả hai giới hạn.
- Mỗi lượt có chi phí khởi động vài phút: cài thư viện, tải ranh giới GADM, kéo trạng thái từ Drive.
- Phần xã đang làm dở khi hết giờ hoặc bị dừng được làm lại ở lượt sau. Dữ liệu đã đồng bộ không mất.
- Tôi chưa chạy được workflow này với Earth Engine và Drive thật, vì không có quyền truy cập. Bước chạy thử 10 xã ở mục 5 là bước xác nhận cuối cùng.
