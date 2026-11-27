# Hệ thống SAGSINS Client-Server Giám sát Rừng Sơn Trà (PBL4)

Hệ thống được tổ chức theo kiến trúc **Client-Server 3 tầng SAGSINS thuần (Mặt đất - Không gian - Không trung)**, tách biệt rõ hai thư mục độc lập để triển khai linh hoạt trên **1 máy (multi-process)** hoặc **2 máy qua mạng LAN**:

```text
PBL4/
├── server/                     # MÁY 1 (Server: Trạm Kiểm Lâm, 3 Vệ tinh LEO, Heatmap, TCP/UDP Socket Server, Web UI :8000)
│   ├── main.py
│   ├── requirements.txt
│   ├── api/
│   ├── agents/
│   ├── core/
│   ├── data/
│   ├── environment/
│   ├── static/
│   └── templates/
├── client/                     # MÁY 2 (100% Python Standard Library - không cần pip install, có thể copy riêng sang Máy 2)
│   ├── protocol.py             # Đóng gói khung nhị phân 26B Header + Checksum CRC32
│   ├── uav_client.py           # Tiến trình Client UAV độc lập (1 tiến trình = 1 OS PID riêng)
│   ├── client_app.py           # Giao diện Web quản lý & chạy 35 PID UAV Client tại :8080
│   └── client_ui.html
└── docs/                       # Tài liệu báo cáo PBL4 & mô tả giao diện
```

## Cơ chế truyền thông 2 kênh SAGSINS (A2G <-> LEO)
- **Tầng Mặt đất & Không gian (Server):** `1 Trạm Kiểm Lâm Sơn Trà` + `3 Vệ tinh LEO (quỹ đạo SGP4)` + `7 phân khu GeoJSON` + `210 điểm nhiệt`. Server khởi động với **`0/35` UAV** và chờ Client kết nối qua **TCP `:9000`** / **UDP `:9001`**.
- **Tầng Không trung (Client):** Mỗi UAV chạy như một tiến trình Hệ điều hành độc lập (`uav_client.py`). Server đánh giá liên tục 2 kênh truyền về Trạm:
  1. **`GROUND_LINK` (A2G trực tiếp):** Khoảng cách tới Trạm Kiểm Lâm `<= 3.5 km`.
  2. **`LEO_SATELLITE` (Vệ tinh LEO):** Khi vượt `3.5 km`, tự động chuyển mạch qua vệ tinh LEO có SNR cao nhất trong tầm `2,500 km` (`UAV -> Vệ tinh LEO -> Trạm`).
  3. **`OUT_OF_RANGE`:** Ngoài vùng phủ sóng cả A2G lẫn LEO.

---

## Hướng dẫn chạy trên 1 máy (Loopback `127.0.0.1`)

### Bước 1: Cài thư viện cho Server và khởi chạy Máy 1
```powershell
pip install -r server/requirements.txt
python server/main.py
```
- Bản đồ giám sát Server: `http://127.0.0.1:8000/`
- Dashboard Mạng & HĐH: `http://127.0.0.1:8000/dashboard`

### Bước 2: Mở giao diện Client Fleet Control (`client/`) để chạy 35 UAV thật
```powershell
python client/client_app.py --server-host 127.0.0.1 --port 8080
```
Mở trình duyệt `http://127.0.0.1:8080/` và bấm **`Khởi chạy 35 UAV (35 PID thật)`**.  
Hoặc mở thêm terminal thủ công để chạy một UAV tương tác trực tiếp bằng bàn phím:
```powershell
python client/uav_client.py --host 127.0.0.1 --id UAV_EXT_01 --zone ZONE_07
```

---

## Hướng dẫn chạy trên 2 máy qua mạng LAN
1. **Trên Máy 1 (Server):** Chạy `python server/main.py` và xem dòng log `[CLIENT-SERVER] ... --host <SERVER_LAN_IP>`. Mở Firewall cho TCP `8000`, `9000` và UDP `9001`.
   - Giao diện Server trên Máy 1: `http://127.0.0.1:8000/` và `http://127.0.0.1:8000/dashboard`
2. **Trên Máy 2 (Client):** Chỉ cần copy thư mục `client/` sang Máy 2 (chỉ cần Python 3 chuẩn, không cần cài thư viện ngoài):
   ```powershell
   python client/client_app.py --server-host <SERVER_LAN_IP> --port 8080
   ```
   - Giao diện Client trên Máy 2: `http://127.0.0.1:8080/`
