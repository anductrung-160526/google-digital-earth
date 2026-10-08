# -*- coding: utf-8 -*-
"""Cấu hình chung cho cách lấy dữ liệu theo lô (batch export).

Luồng: Colab (tài khoản của bạn) gửi tác vụ Export lên Earth Engine -> kết quả ghi vào thư mục
EXPORT_FOLDER trên Drive -> GitHub Actions (process_exports.py) cắt ảnh theo xã, gộp CSV, ghi vào
VNGISDash_2024. Hoàn tất phần ngày trước phần đêm; giữ export dùng chung để chạy tiếp.
"""

PROJECT_ID = "vngis-ee-2"
ASSET_ID = f"projects/{PROJECT_ID}/assets/communes_l3"

EXPORT_FOLDER = "VNGIS_EXPORT_2024"          # thư mục tạm ở gốc My Drive, Earth Engine ghi vào đây

# Lưới pixel cố định cho EPSG:4326. Earth Engine đổi scale (mét) sang độ theo 1 độ = 111319.49079327357 m.
# Dùng crsTransform tường minh để mọi file (tỉnh, toàn quốc, từng xã) nằm trên cùng một lưới.
M_PER_DEG = 111319.49079327357
D20 = 20 / M_PER_DEG
D500 = 500 / M_PER_DEG
T20 = [D20, 0, 0, 0, -D20, 0]
T500 = [D500, 0, 0, 0, -D500, 0]

FILE_DIMENSIONS = 4096                        # mỗi ô ảnh export tối đa 4096 x 4096 pixel (~0,7 GB), vừa ổ máy GitHub

# Khung bao Việt Nam (rộng hơn một chút), chỉ dùng làm vùng export cho ảnh toàn quốc
VN_BBOX = [102.0, 8.0, 110.0, 23.6]

# Tên file export
COMMUNES_GEOJSON = "communes_l3"              # -> communes_l3.geojson
PLAN_DAY = "plan_day.json"


def day_csv_prefix(gid1):
    return f"day_csv_{gid1.replace('.', '-')}"


def night_csv_prefix(gid1):
    return f"night_csv_{gid1.replace('.', '-')}"


def day_img_prefix(month, window):
    return f"day_img_2024{month:02d}_w{window}"


def night_img_prefix(month):
    return f"night_img_2024{month:02d}"


def window_class(counts):
    """counts = [c0, c1, c2] của một tháng: số cảnh Sentinel-2 trong tháng, ±15 ngày, ±30 ngày.
    Đúng thứ tự if của get_adaptive_monthly_composite (notebook cell 18). None = bỏ tháng."""
    c0, c1, c2 = counts
    return 0 if c0 > 0 else (1 if c1 > 0 else (2 if c2 > 0 else None))
