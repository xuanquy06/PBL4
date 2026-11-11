# SAGSINS MAS — Hệ thống Đa tác nhân Giám sát & Điều phối Chữa cháy rừng Bán đảo Sơn Trà

Dự án mô phỏng **Hệ thống Đa tác nhân (Multi-Agent System - MAS)** hoạt động trên kiến trúc mạng tích hợp 3 tầng **Không gian – Trên không – Mặt đất (SAGSINS: Space-Air-Ground Satellite-Integrated Network System)** phục vụ bài toán tuần tra, phát hiện sớm và điều phối UAV ứng cứu cháy rừng tại **Bán đảo Sơn Trà (Đà Nẵng)**.

---

## 1. Tổng quan Kiến trúc Hệ thống (32 Tác nhân)

Hệ thống bao gồm **32 tác nhân tự trị (Autonomous Agents)** giao tiếp thời gian thực:

| Tầng mạng | Số lượng | Thành phần | Vai trò chính |
| :--- | :---: | :--- | :--- |
| **Tầng Mặt đất (Ground)** | `01` | `TRẠM_KIỂM_LÂM_SƠN_TRÀ` | Trung tâm chỉ huy, thu thập dữ liệu quan trắc, khởi tạo phiên đấu thầu sự cố và là trạm sạc/thay pin cho đội bay. |
| **Tầng Trên không (Air - FANET)** | `28` | `MÁY_BAY_UAV_01` ➔ `28` | Chia làm **21 UAV tuần tra** (phân bổ đều vào 7 phân khu `Z1`–`Z7`, mỗi phân khu 3 chiếc bay theo 3 vòng Bắc – Trung – Nam ở độ cao 1.000m – 1.300m) và **07 UAV dự bị trực chiến** (`UAV_22`–`28`) tại trạm. |
| **Tầng Không gian (Space - LEO)** | `03` | `VỆ_TINH_LEO_01` ➔ `03` | Vệ tinh quỹ đạo thấp (mô phỏng theo cơ học quỹ đạo SGP4 thực tế) làm kênh truyền dự phòng (Space Backhaul) khi mất đường truyền mặt đất. |

---

## 2. Các Thuật toán & Cơ chế Cốt lõi

### 2.1. Giao thức Đấu thầu Phân tán FIPA-CNP (Contract Net Protocol)
Khi người dùng kích hoạt một điểm cháy trên bản đồ, ứng dụng dùng vị trí đó làm **sự kiện phát hiện giả lập** (chưa tích hợp cảm biến camera/nhiệt thực):
1. **Báo cáo phát hiện (`FIRE_DETECTION_REPORT`):** UAV gần điểm cháy nhất có thể truyền sẽ gửi tọa độ về trạm qua broker, theo tuyến nhiều chặng nếu cần. Nếu UAV gần nhất không có tuyến khả dụng, hệ thống thử UAV kế tiếp.
2. **Phát lệnh gọi thầu (`CFP_FIRE_EVENT`):** Sau khi báo cáo được chuyển tới trạm, hệ thống mở phiên CNP và mời các UAV phù hợp tham gia đấu thầu.
3. **Đánh giá & Báo giá (`PROPOSE` / `REFUSE`):**
   - Mỗi UAV tự tính năng lượng bay khứ hồi $E_{\text{khứ hồi}}$ dựa trên khoảng cách 3D và công suất động cơ.
   - **Ràng buộc an toàn:** Chỉ các UAV có mức pin dự kiến sau khi bay tới điểm cháy và quay về $> 25\%$ mới được phép nộp hồ sơ thầu (`PROPOSE`); ngược lại tự động gửi bản tin từ chối (`REFUSE`).
   - **Hàm chi phí đa mục tiêu:** Ưu tiên UAV thuộc đúng phân khu đang cháy, khoảng cách gần nhất, mức pin cao nhất và cân bằng số nhiệm vụ đã nhận.
4. **Trao thầu (`ACCEPT_PROPOSAL`) & Cơ động hiện trường:** UAV có chi phí thấp nhất thắng thầu, lập tức tách khỏi vòng tuần tra để bay tới tâm cháy, gửi gói dữ liệu hiện trường về trạm và bay lượn 1 vòng nhỏ ($\sim 130\text{m}$) khoanh vùng đám cháy trước khi quay lại quỹ đạo tuần tra gốc.

### 2.2. Định tuyến phân tán theo thông tin láng giềng
* **Mô hình liên kết:** Các ngưỡng A2G (4,2 km) và A2A (7 km) được dùng như giả định cự ly truyền dẫn trong mô phỏng; hiện chưa có dữ liệu địa hình/DEM để xác định che khuất thực tế theo từng vị trí.
* **Chọn tuyến:** Mỗi UAV và vệ tinh quảng bá tuyến tới trạm cùng các chỉ số chi phí qua beacon. Nút nguồn chỉ xét bảng láng giềng 1-hop, kiểm tra chất lượng chặng đầu bằng mô hình kênh truyền (FSPL, SNR, dung lượng và PER), rồi chọn tuyến khả dụng có chi phí thấp nhất; không chạy tìm kiếm Dijkstra trên toàn đồ thị trong luồng định tuyến vận hành. Bộ định tuyến toàn cục còn lại trong mã chỉ phục vụ tham chiếu/kiểm thử.
* **Phục hồi:** Khi tuyến đang dùng không còn khả dụng, nút gửi có thể chọn tuyến thay thế từ các quảng bá láng giềng còn hạn. Khả năng hội tụ phụ thuộc vào beacon và trạng thái láng giềng mà node đã nhận được.

### 2.3. Cơ chế Thay ca Tại chỗ ("Người mới tới nơi – Người cũ mới về" / In-Zone Hot-Swap)
Để giải quyết bài toán khoảng cách từ Trạm tới các phân khu khác nhau (`Z1` gần, `Z7` xa gần $8\text{ km}$) mà vẫn đảm bảo **mỗi phân khu luôn duy trì đủ 3 UAV hoạt động 24/7**:
1. Khi một UAV tuần tra chạm ngưỡng cần thay ca (hoặc khi người dùng giả lập bấm nút **Pin yếu**), nó phát lệnh gọi **UAV dự bị** từ Trạm xuất kích bay ra trước, nhưng bản thân **vẫn ở lại bay tuần tra giữ vị trí**.
2. Chỉ khi UAV dự bị đã bay vào tới phân khu (khoảng cách $\le 450\text{m}$) và tiếp quản vòng tuần tra, UAV cũ mới rời phân khu bay về Trạm sạc.
3. Sau khi sạc đầy 100% tại Trạm, UAV vừa về sẽ chuyển sang trạng thái **Dự bị trực chiến (`STANDBY`)** tại Trạm cho lượt thay ca kế tiếp.

---

## 3. Giao diện Điều hành & Giám sát Thời gian thực

Hệ thống cung cấp 2 giao diện Web thời gian thực, cùng sử dụng endpoint WebSocket dashboard `/ws/mophong`. Telemetry của UAV đi qua broker mạng trước khi được phát thành sự kiện cho giao diện:

1. **Trang Bản đồ Chiến thuật (`/` — [`templates/index.html`](templates/index.html)):**
   - Bản đồ vệ tinh 7 phân khu (`Z1`–`Z7`) bán đảo Sơn Trà.
   - Bộ công cụ tương tác trực tiếp: **Báo cháy / Xoá cháy**, giả lập sự cố từng UAV (**Sập 0% / Pin yếu 18% / Sạc 100%**), điều chỉnh tốc độ mô phỏng (**Tạm dừng, 1x, 2x, 5x**).
   - Bộ lọc lớp hiển thị thông minh: **7 Vùng**, **Tia Gói tin RF tức thời**, **Tuyến định tuyến E2E**, **Nhãn tên**, cùng các lớp chuyên sâu (**Tia FANET, Tia A2G, Tia Vệ tinh, Quỹ đạo**).
2. **Trang Dashboard Điều phối (`/dashboard` — [`templates/dashboard.html`](templates/dashboard.html)):**
   - 6 thẻ KPI thời gian thực: Quân số UAV kết nối, Phân bổ liên kết 3 tầng (A2G / FANET / LEO), Tỷ lệ nhận gói thành công (PDR), Độ trễ trung bình, Tổng năng lượng tiêu thụ (Joule) và Số phiên đấu thầu CNP.
   - Bảng chi tiết phiên đấu thầu FIPA-CNP (danh sách UAV thắng thầu, thua thầu, từ chối thầu).
   - Ma trận trạng thái 7 phân khu & 28 UAV theo thời gian thực.
   - Nhật ký sự kiện giao thức MAS (Log Stream) và tính năng **Xuất báo cáo thực nghiệm ra file CSV** (`/api/xuat-csv`).

---

## 4. Cấu trúc Thư mục Dự án

```text
PBL4/
├── agents/                     # Mô hình các tác nhân tự trị
│   ├── uav_agent.py            # Tác nhân UAV (Bay tuần tra, đấu thầu CNP, FANET Relay, thay ca Hot-Swap)
│   ├── satellite_agent.py      # Tác nhân Vệ tinh LEO (Quỹ đạo SGP4, chuyển tiếp không gian)
│   └── ground_station.py       # Tác nhân Trạm Kiểm Lâm Mặt Đất
├── core/                       # Lõi giao tiếp và định tuyến mạng
│   ├── base_agent.py           # Lớp cơ sở của mọi tác nhân (Pin, hàng đợi thông điệp, FSM)
│   ├── message_broker.py       # Định tuyến theo beacon láng giềng, kênh vô tuyến & WebSocket dashboard
│   └── ground_station_server.py# Điều phối phiên đấu thầu CNP tại Trạm mặt đất
├── environment/                # Động cơ vật lý & quản lý kịch bản
│   ├── physics_engine.py       # Tính toán 3D ECEF, ngưỡng cự ly LoS giả định, FSPL, SNR, Shannon, PER
│   ├── scenario_manager.py     # Khởi tạo 7 vùng Geofence Sơn Trà, 28 UAV & ghi log CSV
│   └── simulation_loop.py      # Vòng lặp đồng hồ vật lý trung tâm (World Clock)
├── api/                        # Các REST API & WebSocket Endpoints (FastAPI)
│   ├── routes.py
│   └── schemas.py
├── templates/                  # Giao diện người dùng (UI/UX Pro Max Dark Console)
│   ├── index.html              # Trang Bản đồ Chiến thuật
│   └── dashboard.html          # Trang Dashboard Điều phối
├── static/                     # Tài nguyên tĩnh & thư viện Leaflet cục bộ
├── tests/                      # Bộ kiểm thử tự động (Unit & Integration Tests)
├── main.py                     # Điểm khởi chạy ứng dụng FastAPI
└── requirements.txt            # Danh sách thư viện Python
```

---

## 5. Hướng dẫn Cài đặt & Chạy Chương trình

### Yêu cầu hệ thống
* Python `3.10+`

### Bước 1: Cài đặt thư viện
```bash
pip install -r requirements.txt
```

### Bước 2: Khởi chạy máy chủ mô phỏng
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Bước 3: Mở giao diện trên trình duyệt
* **Bản đồ Chiến thuật:** http://localhost:8000/
* **Dashboard Phân tích:** http://localhost:8000/dashboard

---

## 6. Kịch bản Demo Nhanh (Dành cho Báo cáo / Bảo vệ Đồ án)

1. **Demo 1 – Phát hiện cháy & Đấu thầu CNP + Truyền tin trực tiếp (A2G):**
   - Bấm nút **Báo cháy** ở góc trên bên trái bản đồ, sau đó click vào một điểm ở **Vùng Z2 hoặc Z3** (gần Trạm Kiểm Lâm).
   - Quan sát các tia đỏ `CFP` phát ra, bảng **Đấu thầu CNP gần nhất** bên phải công bố UAV chiến thắng bay tới tâm cháy, lượn 1 vòng khoanh vùng và truyền thẳng gói tin về `SƠN_TRÀ`.
2. **Demo 2 – Cháy ở sườn khuất núi & Định tuyến Đa chặng (FANET E2E):**
   - Bấm **Báo cháy** và click vào **Vùng Z6 hoặc Z7** (phía Đông bán đảo, bị núi Sơn Trà chắn sóng về trạm).
   - Quan sát UAV thắng thầu tại Z6/Z7 tự động định tuyến đa chặng qua các UAV trung gian ở Z5, Z4 (`UAV_xx ➔ UAV_yy ➔ SƠN_TRÀ`) hiển thị bằng **Tuyến E2E màu xanh dương**.
3. **Demo 3 – Tự hồi phục & Thay ca Tại chỗ khi UAV yếu pin / sập nguồn:**
   - Chọn một UAV ở ô chọn góc trên trái bản đồ rồi bấm nút **Pin yếu (18%)** hoặc **Sập (0%)**.
   - Quan sát UAV dự bị (`[DỰ BỊ]`) từ Trạm Kiểm Lâm lập tức cất cánh bay ra phân khu để tiếp quản vòng tuần tra, đảm bảo phân khu không bị mất độ phủ sóng.
