# Hướng dẫn chạy VNGISDash 2024 trên GitHub Actions

Pipeline chạy notebook `VNGISDash_Task123_Merged_final.ipynb` (chỉ 3 chức năng: trích chỉ số ảnh ngày và đêm, lấy tif ngày, lấy tif đêm) cho toàn bộ xã/phường (GADM 4.1 cấp 3), 12 tháng năm 2024, và ghi kết quả lên Google Drive. Mỗi lượt chạy khoảng 5 giờ 15 phút, hết lượt thì tự gọi lượt kế tiếp. Trạng thái nằm trên Drive nên lượt sau làm tiếp đúng chỗ dừng, bạn có thể tắt máy.

Mỗi xã cho ra:

| Task | Kết quả |
|---|---|
| 1 | 1 CSV 12 tháng: trung bình và độ lệch chuẩn của 10 kênh (20 cột) |
| 2 | 12 ảnh GeoTIFF Sentinel-2, 10 kênh, 20 m |
| 3.1 | 12 ảnh GeoTIFF VIIRS, 2 kênh `avg_rad`, `cf_cvg`, 500 m |
| 3.2 | 1 CSV chỉ số ánh sáng đêm theo tháng |

## 0. Cần chuẩn bị

- **Dung lượng Drive.** Ảnh Sentinel-2 20 m, 10 kênh, cho cả nước 12 tháng là rất lớn. Ước tính thô của tôi là khoảng 0,5 đến 1,5 TB sau khi nén. Con số thật lấy từ báo cáo thí điểm (mục "Dung lượng và thời gian"): lấy số MB trung bình mỗi xã nhân khoảng 11.000. Drive giới hạn tải lên khoảng 750 GB mỗi ngày.
- **Repo nên để Public** để runner GitHub miễn phí không giới hạn phút. Khóa truy cập nằm trong Secrets nên không lộ. Bạn nên đọc lại điều khoản GitHub Actions trước khi chạy dài ngày.
- Quyền quản trị project Google Cloud `digital-vietnam-earth`.

## 1. Dọn bản cũ (bắt buộc)

1. Tab **Actions**: nếu còn lượt nào đang chạy, mở lượt đó và bấm **Cancel workflow**.
2. Trên Google Drive: **xóa thư mục `VNGISDash_Communes_2024`** của bản cũ (chứa ảnh rỗng và bản ghi trạng thái cũ).
3. Trong repo: xóa các file cũ (`vngis_2024.py`, `HUONG_DAN_GITHUB_ACTIONS.md`, `requirements.txt`, `.github/workflows/vngis-2024.yml`) rồi đưa bộ file mới lên ở bước 2.

## 2. Đưa file lên repo

Repo phải có đúng cấu trúc này ở **thư mục gốc** (không đặt trong thư mục con):

```
vngis_2024.py
verify_pilot.py
requirements.txt
.gitignore
HUONG_DAN.md
DOI_CHIEU_NOTEBOOK.md
GHI_CHU.md
.github/workflows/vngis-2024.yml
```

Cách nhanh: giải nén file zip, trên github.com chọn **Add file ▸ Upload files**, kéo toàn bộ nội dung vào (thư mục `.github` là thư mục ẩn, cần bật hiển thị file ẩn). Kiểm tra: tab **Actions** hiện workflow **VNGISDash 2024 (chạy nối lượt)**.

## 3. Service account cho Earth Engine

Nếu đã có service account và secret `EE_SERVICE_ACCOUNT_JSON` từ bản cũ, chỉ cần làm mục 3.2.

3.1. Tạo mới: [console.cloud.google.com](https://console.cloud.google.com) ▸ project `digital-vietnam-earth` ▸ **IAM & Admin ▸ Service Accounts ▸ Create service account** (tên ví dụ `vngis-runner`) ▸ tab **Keys ▸ Add key ▸ JSON**. Giữ kín file JSON.

3.2. Cấp role: **IAM & Admin ▸ IAM ▸ Grant access**, nhập email service account, thêm đủ hai role:

- **Service Usage Consumer** (`roles/serviceusage.serviceUsageConsumer`)
- **Earth Engine Resource Writer** (`roles/earthengine.writer`)

Role Viewer đơn lẻ có thể không đủ để tải ảnh, đây là nguyên nhân nghi ngờ của lỗi "output trống" lần trước. Nếu log báo service account chưa đăng ký Earth Engine, đăng ký theo trang [Earth Engine service accounts](https://developers.google.com/earth-engine/guides/service_account).

## 4. rclone cho Google Drive

Nếu đã có secret `RCLONE_CONF` và token chưa hết hạn, bỏ qua bước này.

Trên máy của bạn: cài rclone, chạy `rclone config`: `n` ▸ tên `gdrive` ▸ Storage `drive` ▸ client_id, client_secret để trống ▸ scope `1` ▸ Enter ▸ advanced `n` ▸ trình duyệt `y` (đăng nhập, cho phép) ▸ Shared Drive `n` ▸ `y` ▸ `q`. Sau đó chạy `rclone config show gdrive` và copy toàn bộ kết quả.

## 5. Secrets

Repo ▸ **Settings ▸ Secrets and variables ▸ Actions ▸ New repository secret**:

| Tên | Nội dung |
|---|---|
| `EE_SERVICE_ACCOUNT_JSON` | toàn bộ nội dung file JSON ở bước 3 |
| `RCLONE_CONF` | kết quả `rclone config show gdrive` |

Thêm: **Settings ▸ Actions ▸ General ▸ Workflow permissions ▸ Read and write permissions** (để lượt này gọi được lượt sau).

## 6. Chạy thí điểm 2 xã

Không cần chọn tỉnh, xã. Ở chế độ `pilot`, pipeline tự lấy số xã bạn nhập ở ô `pilot_n` (mặc định 2), theo thứ tự mã GADM, xen kẽ phường (đô thị) và xã (nông thôn). Ví dụ `pilot_n` = 5 cho 3 phường và 2 xã. Mã các xã được in ở dòng `Danh sách: ... (thí điểm: ...)` trong log.

**Chạy.** Tab **Actions ▸ VNGISDash 2024 (chạy nối lượt) ▸ Run workflow**: `mode` = `pilot`, `pilot_n` = số xã muốn thử (ví dụ `2`), `workers` = `2` (nếu thử nhiều xã có thể tăng lên `4` đến `8`).

Kết quả ghi vào thư mục riêng **`VNGISDash_PILOT_2024`**, không đụng tới dữ liệu thật.

**Đọc preflight (trong 2 đến 5 phút đầu).** Mở lượt chạy ▸ job **Chạy một lượt** ▸ bước **Chạy pipeline**, tìm các dòng `[preflight]`:

| Dòng log | Ý nghĩa |
|---|---|
| `[preflight] ĐẠT: ...` | Earth Engine, quyền tải ảnh, chia ô và Drive đều ổn. Pipeline chạy tiếp |
| `HTTP 403`, `permission`, `PermanentError` | Service account thiếu quyền: làm lại mục 3.2, rồi Run workflow lại |
| `rclone không ghi được` | Secret `RCLONE_CONF` sai hoặc token hết hạn: làm lại bước 4, 5 |
| `Kiểm tra chia ô KHÔNG ĐẠT` | Không chặn chạy. Gửi tôi dòng log này |

Nếu preflight lỗi, script dừng ngay với mã 1 (không chạy tiếp để tạo file rỗng như lần trước). Gửi tôi nguyên dòng lỗi.

**Đọc báo cáo kiểm tra.** Khi lượt thí điểm xong, bước **Kiểm tra kết quả thí điểm** tự chạy `verify_pilot.py`. Báo cáo nằm ở trang **Summary** của lượt chạy và trong file `VNGISDash_PILOT_2024/_control/verify_report.md` trên Drive. Mỗi xã có bảng PASS/WARN/FAIL:

- Trạng thái xã là `done`.
- Task 2: đủ số ảnh (12 trừ tháng không có ảnh kể cả khi nới biên ±30 ngày), mỗi ảnh 10 kênh, 20 m, EPSG:4326.
- Task 3.1: đủ ảnh đêm, 2 kênh, 500 m, Float64.
- Task 1, Task 3.2: số dòng và số cột đúng như notebook.
- Dung lượng và thời gian của từng xã, dùng để ước tính cho cả nước.

WARN "toàn NoData" ở một vài tháng là bình thường với tháng mây phủ kín. Có bất kỳ dòng FAIL nào thì chưa chạy toàn quốc.

**Đối chiếu với notebook (nên làm).** Trên Colab, chạy notebook cho 1 trong 2 xã thí điểm (chọn đúng xã có mã in trong log). Gom vào một thư mục (ví dụ `/content/nb_out`): file CSV gộp 12 tháng của Task 1, file CSV Task 3.2 và ảnh `S2_Day_..._202403.tif` do cell "Tải 1 ảnh để phân tích" tạo ra. Rồi chạy trong Colab (Drive đã mount):

```python
!pip -q install rasterio
!python verify_pilot.py --root /content/drive/MyDrive/VNGISDash_PILOT_2024 \
    --notebook-dir /content/nb_out --notebook-gid VNM.4.1.10_1 --gids VNM.4.1.10_1
```

Các dòng "Đối chiếu ..." phải PASS: CSV lệch tối đa 1e-6, ảnh trùng từng pixel.

## 7. Chạy toàn quốc

Sau khi thí điểm PASS: **Run workflow** với `mode` = `full`, `workers` = `8`. Kết quả ghi vào **`VNGISDash_Communes_2024`**.

Theo dõi:

- Tab **Actions**: mỗi lượt một dòng; trang Summary có số xã theo trạng thái và 15 xã lỗi đầu tiên.
- Drive: `_control/progress.csv` (một dòng mỗi xã) và `_control/logs/`.
- GitHub gửi email khi một lượt thất bại.

Khi xong toàn bộ, lượt cuối ghi `HOÀN TẤT` và tạo các file gộp trong `_merged/`.

## 8. Dừng và tiếp tục

| Muốn | Làm |
|---|---|
| Dừng trật tự | Tạo file tên `STOP` ở thư mục gốc repo (**Add file ▸ Create new file**), hoặc tạo file `STOP` trong `_control/` trên Drive. Trong tối đa 5 phút lượt đang chạy đồng bộ rồi thoát, không nối lượt |
| Dừng ngay | **Cancel workflow**. Phần làm dở vài phút sẽ được làm lại |
| Tiếp tục | Xóa file `STOP`, rồi **Run workflow** với cùng `mode` và `pilot_n` |

Luôn dùng **Run workflow**. Nút **Re-run all jobs** chạy lại với input của lần cũ.

## 9. Cấu trúc thư mục trên Drive

```
VNGISDash_Communes_2024/              (thí điểm: VNGISDash_PILOT_2024/, cùng cấu trúc)
├── _control/
│   ├── status/status_<thời điểm>_<run_id>.jsonl   trạng thái từng xã, mỗi lượt một file
│   ├── logs/run_<thời điểm>_<run_id>.log
│   ├── progress.csv                               tổng hợp trạng thái mọi xã
│   ├── verify_report.md                           (chỉ khi thí điểm)
│   └── STOP                                       (tự tạo khi muốn dừng)
├── 1_Task1_Spectral_Indices/<GID_1>_<tỉnh>/
│       s2_<tỉnh>_<GID_1>_<GID_3>_2024_Spectral_Indices.csv
├── 2_Task2_Day_S2/<GID_1>_<tỉnh>/S2_Day_<tỉnh>_<xã>_<GID_3>_202401-202412/
│       S2_Day_<tỉnh>_<xã>_<GID_3>_2024MM.tif       (12 file)
├── 3_Task3_Night_VIIRS/<GID_1>_<tỉnh>/VIIRS_Night_<tỉnh>_<xã>_<GID_3>_202401-202412/
│       VIIRS_Night_<tỉnh>_<xã>_<GID_3>_2024MM.tif  (12 file)
├── 4_Task3_Economic_Indices/<GID_1>_<tỉnh>/
│       VIIRS_Night_<tỉnh>_<xã>_<GID_3>_202401-202412_Economic_Indices.csv
└── _merged/                                        (tạo khi xong toàn bộ)
    ├── Task1_Spectral_Indices_2024_ALL.csv
    ├── Task3_Economic_Indices_2024_ALL.csv
    └── by_province/<tên>_<GID_1>.csv
```

Tên thư mục và tên file ảnh, CSV giữ đúng quy tắc đặt tên của notebook. Muốn gộp lại CSV bất kỳ lúc nào: chạy `python vngis_2024.py --merge-only` ở máy có rclone.

## 10. Xử lý sự cố

| Hiện tượng | Cách xử lý |
|---|---|
| `Thiếu secret ...` | Thêm đúng tên secret ở bước 5 |
| Preflight báo 401/403/permission | Cấp đủ 2 role ở mục 3.2; chờ vài phút cho IAM cập nhật rồi chạy lại |
| `Asset không có xã ...` | GID_3 trong asset `communes_l3` không khớp GADM; gửi tôi mã xã |
| `invalid_grant`, `token expired` (rclone) | Làm lại bước 4 và cập nhật `RCLONE_CONF` |
| `24 lượt tải liên tiếp thất bại` | Script tự dừng với mã 1 để không tạo file rỗng. Gửi tôi dòng "Lỗi gần nhất" |
| Log báo `vượt hạn mức tải, chia NxN ô` | Bình thường với xã lớn; ảnh được ghép lại đúng lưới pixel |
| Log cảnh báo còn thư mục `03_Provinces`, `04_Status` | Thư mục Drive còn dữ liệu bản cũ: xóa theo mục 1 |
| `Resource not accessible by integration` ở bước nối lượt | Bật Read and write permissions (cuối bước 5) |

Mã thoát (dòng `Script thoát với mã N` trong log):

| Mã | Ý nghĩa | Workflow làm gì |
|---|---|---|
| 0 | Xong toàn bộ | Dừng |
| 1 | Lỗi cấu hình, preflight không đạt, hoặc tải ảnh hỏng hàng loạt | Báo đỏ, dừng. Đọc dòng `[preflight]` / `ERROR` |
| 2 | Earth Engine gặp sự cố kéo dài | Nối lượt |
| 3 | Hết giờ của lượt | Nối lượt |
| 130 | Dừng bằng file STOP hoặc hủy tay | Dừng |
