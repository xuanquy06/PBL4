# SAGSINS MAS — Hệ thống Đa tác nhân Giám sát & Điều phối cháy rừng Bán đảo Sơn Trà

Dự án mô phỏng **Hệ thống Đa tác nhân (Multi-Agent System - MAS)** vận hành trên kiến trúc mạng tích hợp 3 tầng **Không gian – Trên không – Mặt đất (SAGSINS: Space-Air-Ground Satellite-Integrated Network System)** phục vụ bài toán tuần tra tự chủ, phát hiện sớm đám cháy, tự chữa lành mạng và thay ca khép kín cho đội bay UAV tại **Bán đảo Sơn Trà (Đà Nẵng)**.

---

## 1. Kiến trúc Hệ thống 3 Tầng (39 Tác nhân Tự trị)

Hệ thống khởi tạo **39 tác nhân tự trị (Autonomous Agents)** giao tiếp và phối hợp thời gian thực:

| Tầng mạng | Số lượng | Định danh | Vai trò chính |
| :--- | :---: | :--- | :--- |
| **Tầng Mặt đất (Ground)** | `01` | `TRẠM_KIỂM_LÂM_SƠN_TRÀ` `(16.105, 108.250)` | Trung tâm chỉ huy, thu thập gói tin telemetry/cảnh báo cháy, trạm gốc định tuyến và trạm sạc nhanh cho đội bay UAV. Kết hợp lưới điểm nhiệt Heatmap (`HeatNode`) trên 7 phân khu `ZONE_01`–`ZONE_07`. |
| **Tầng Trên không (Air - FANET)** | `35` | `MÁY_BAY_UAV_01` ➔ `35` | Gồm **28 UAV tuần tra chủ động** (`UAV_01`–`UAV_28`, chia đều **4 UAV/phân khu** `Z1`–`Z7`, pin khởi tạo lệch pha `45%–100%` theo 4 dải `90–100%`, `75–86%`, `60–72%`, `45–56%`) và **07 UAV dự bị trực chiến** (`UAV_29`–`UAV_35`, pin `100%`, trạng thái `STANDBY` tại Trạm Kiểm Lâm). |
| **Tầng Không gian (Space - LEO)** | `03` | `VỆ_TINH_LEO_01` ➔ `03` | Vệ tinh quỹ đạo thấp (mô phỏng quỹ đạo SGP4 thực tế đồng bộ từ CelesTrak/NORAD kèm bộ đệm CSV Offline) làm kênh truyền dự phòng không gian (Space Backhaul) khi đứt liên kết mặt đất. |

---

## 2. Các Thuật toán & Cơ chế Tự trị Cốt lõi

### 2.1. Bản đồ Độ bất định Giám sát (Staleness / Uncertainty Heatmap)
Bản đồ Heatmap biểu diễn **độ cũ dữ liệu giám sát** tại từng `HeatNode` trong 7 phân khu rừng Sơn Trà:
- **Làm mát tỏa mượt trong tầm quét (`Cooling Radius = 600m`):** Mọi `HeatNode` (không phải đám cháy thật) nằm trong bán kính $\le 600\text{m}$ quanh ít nhất một UAV đang bay (`PATROLLING`, `RELAYING`, `RESPONDING`, `CONTAINING`) được giảm dần `risk_score` theo tốc độ **`0.35/s`**, có trọng số tỏa từ tâm UAV (`1.25x` tại tâm $\rightarrow$ `0.75x` tại biên `600m`) về mức an toàn **`0.05`** (màu xanh lá), giúp UAV bay lướt xuyên qua tâm điểm nóng mượt mà thay vì quay đầu giật cục ở mép.
- **Tích lũy độ cũ dữ liệu (`Staleness Rate = 0.025/s`):** Các điểm nằm ngoài tầm quét ($> 600\text{m}$) tăng dần `risk_score` thêm `0.025/giây` và khóa trần tại **`0.68`** (chuyển vàng/cam để thu hút UAV tới tuần tra).
- **Bảo toàn đám cháy thật (`active_fires`):** Điểm cháy do người dùng kích hoạt giữ nguyên mức **`1.0`** (màu đỏ) và **không tự tắt** khi UAV bay tới; chỉ bị xóa khi bấm nút **Xoá cháy**.

### 2.2. Điều hướng Tự chủ bằng Trường Thế năng & Chống tụ cụm
Các UAV tuần tra **không bay theo đường ray/waypoint cứng**, mà tự điều hướng theo vector trường lực tổng hợp (`_di_chuyen_tu_chu_theo_truong_luc` trong [`agents/uav_agent.py`](agents/uav_agent.py)):
- **Lực hút nhiệt (Heatmap Gradient):** Hút UAV về các `HeatNode` có `risk_score` cao nhất trong phân khu `zone_id` phụ trách.
- **Chống tụ cụm đồng đội (`Virtual Pheromone / Target Claiming`):** Nếu một `HeatNode` đã có UAV đồng đội khác đứng gần hơn UAV hiện tại, lực hút của điểm đó đối với UAV hiện tại tự động **giảm `80%`**, giúp các UAV tự phân chia khu vực quét.
- **Lực đẩy giữa các UAV (`Inter-UAV Repulsion`):** Khi hai UAV cách nhau $< 1200\text{m}$ (gấp đôi bán kính quét `600m`), sinh ra vector lực đẩy mạnh gấp **`2.5x`** lực hút Heatmap để chống chồng lấn vùng quét.
- **Tuân thủ Geofence:** Giữ UAV bay trong đa giác phân khu `ZONE_01`–`ZONE_07` bằng thuật toán Ray-Casting kết hợp trượt biên mượt.
- **Ngắt lực hút khi trực chiến / quay về:** Các UAV ở trạng thái `STANDBY`, `CHARGING` và `RETURNING` tắt hoàn toàn cơ chế hút theo điểm nóng để đứng yên tại trạm hoặc bay thẳng về sạc.

### 2.3. Tự chủ Phát hiện, Khoanh vùng Đám cháy & Tự chữa lành sự cố
1. **Phát hiện & Khoanh vùng liên tục (`CONTAINING`):** Khi bay vào bán kính $\le 600\text{m}$ quanh điểm cháy chưa bị khóa, UAV tự kiểm tra pin khứ hồi $> 25\%$, khóa mục tiêu (`TARGET_LOCKED`), chuyển sang bay lượn vòng tròn bán kính `130m` quanh tâm cháy (`CONTAINING`) và duy trì trạng thái này cho đến khi người dùng bấm **Xoá cháy** hoặc UAV tụt pin $< 25\%$.
2. **Bàn giao P2P khi UAV đang giữ đám cháy bị Pin yếu hoặc Sập nguồn:**
   - Khi UAV đang `RESPONDING` hoặc `CONTAINING` bị **Pin yếu (`< 25%`)** hoặc bị bấm **Sập (`0%` - `OFFLINE`)**, nó lập tức hủy tác vụ cũ, **mở khóa mục tiêu cháy** (`mo_khoa_muc_tieu_chay`) và phát bản tin P2P `YEU_CAU_HO_TRO_CHAY_P2P` ủy quyền cho đồng đội đang bay trên không có mức pin cao nhất trong cùng phân khu tiếp quản ngay lập tức.
   - Đồng thời, 1 UAV dự bị `STANDBY` tại Trạm được điều động bay ra phân khu để giữ nguyên quân số `4/4`.

### 2.4. Định tuyến Đa chặng Dijkstra E2E & Chuyển mạch Vệ tinh LEO
- **Mạng kết hợp 3 tầng ([`core/message_broker.py`](core/message_broker.py)):**
  - **Tầng 1 (A2G):** Truyền thẳng về `TRẠM_KIỂM_LÂM_SƠN_TRÀ` khi nằm trong tầm phủ sóng và không bị địa hình che khuất.
  - **Tầng 2 (FANET Mesh):** Bắc cầu đa chặng qua các UAV lân cận trong tầm liên lạc `<= 2200m` bằng thuật toán **Dijkstra** (tối ưu theo chất lượng kênh FSPL, SNR, dung lượng Shannon và PER).
  - **Tầng 3 (LEO Satellite Fallback):** Khi đường truyền mặt đất và bắc cầu FANET bị đứt, gói tin tự động chuyển mạch theo tuyến `[UAV ➔ VỆ_TINH_LEO ➔ TRẠM_KIỂM_LÂM]`.
- **Duy trì tuyến E2E ưu tiên:** Chừng nào vẫn còn đám cháy đang được UAV khoanh vùng (`CONTAINING`), hệ thống liên tục tính toán lại và ưu tiên vẽ **tuyến E2E màu xanh dương** từ UAV báo cháy về Trạm ở mỗi tick mô phỏng.

### 2.5. Vòng đời Thay ca & Sạc nhanh Khép kín (Hot-Swap & Recharge Cycle)
```text
PATROLLING (28 UAV, pin lệch pha 45%–100%)
   │
   ▼  Pin < 25% ──► Phát P2P bàn giao + Đánh thức 1 UAV STANDBY (100%) bay ra thế chỗ (giữ đủ 4/4)
RETURNING (Tắt hút Heatmap, bay thẳng về Trạm Kiểm Lâm)
   │
   ▼  Cách Trạm <= 100m
CHARGING (Hạ độ cao 25m, sạc nhanh +15% pin/giây)
   │
   ▼  Pin đạt 100%
STANDBY (Tái gia nhập kho dự bị 7/7 tại Trạm cho lượt thay ca tiếp theo)
```

---

## 3. Giao diện Điều hành & Giám sát Thời gian thực

Hệ thống giao tiếp với trình duyệt qua WebSocket `/ws/mophong` với 2 màn hình chính:

1. **Bản đồ Chiến thuật (`/` — [`templates/index.html`](templates/index.html)):**
   - Bản đồ vệ tinh 7 phân khu (`Z1`–`Z7`) với thanh trạng thái `4/4` từng vùng và huy hiệu **`DỰ BỊ 7/7`**.
   - **Lớp Heatmap tối ưu hiệu năng:** Tự động tính lại `radius`/`blur` theo mức zoom (`zoomend`), tạm dừng render khi kéo/zoom bản đồ (`movestart`/`zoomstart`) và throttle cập nhật `400ms/lần`.
   - **Bộ lọc & Tia trực quan:** Bật/tắt **7 Vùng**, **Heatmap**, **Quét 600m**, **Tia Gói tin (RF)**, **Tuyến E2E**, **Nhãn tên**, **Tia FANET** (mạng lưới P2P đồng đều nét mảnh), **Tia A2G**, **Tia Vệ tinh** (tự động ẩn vòng quét và tia tại khu vực Trạm đối với UAV `STANDBY`/`CHARGING`).
   - **Điều khiển trực tiếp:** **Báo cháy / Xoá cháy**, **Sập (0%) / Pin yếu (18%) / Sạc (100%)**, chỉnh tốc độ mô phỏng (**Tạm dừng, 1x, 2x, 5x**) và lọc danh sách **Tuần tra (28)** / **Dự bị (7/7)**.
2. **Dashboard Phân tích (`/dashboard` — [`templates/dashboard.html`](templates/dashboard.html)):**
   - Theo dõi KPI thời gian thực (quân số `35/35`, tỷ lệ PDR %, độ trễ ms, năng lượng tiêu hao Joule, phân bổ A2G/FANET/LEO), ma trận 7 phân khu, luồng log giao thức MAS và xuất dữ liệu thực nghiệm CSV (`/api/xuat-csv`).

---

## 4. Cấu trúc Thư mục Dự án

```text
PBL4/
├── agents/                     # Mô hình các tác nhân tự trị
│   ├── uav_agent.py            # Tác nhân UAV (Trường thế năng, phát hiện cháy, khoanh vùng 130m, P2P handover, sạc +15%/s)
│   ├── satellite_agent.py      # Tác nhân Vệ tinh LEO (Quỹ đạo SGP4, chuyển tiếp không gian)
│   └── ground_station.py       # Tác nhân Trạm Kiểm Lâm Mặt Đất
├── core/                       # Lõi giao tiếp và định tuyến mạng
│   ├── base_agent.py           # Lớp cơ sở của tác nhân (Pin, hàng đợi thông điệp, FSM)
│   ├── message_broker.py       # Định tuyến Dijkstra, kênh truyền vô tuyến, thống kê vùng & WebSocket
│   └── ground_station_server.py# Điều phối trạng thái đám cháy & đồng bộ telemetry tới Web UI
├── environment/                # Động cơ vật lý & quản lý kịch bản
│   ├── physics_engine.py       # Tính 3D ECEF, FSPL, SNR, dung lượng Shannon, PER
│   ├── scenario_manager.py     # Khởi tạo 7 Geofence Sơn Trà, 35 UAV, Heatmap Staleness/Cooling & log CSV
│   └── simulation_loop.py      # Vòng lặp đồng hồ vật lý (World Clock) & điều phối dự bị vùng
├── api/                        # Các REST API & WebSocket Endpoints (FastAPI)
│   ├── routes.py
│   └── schemas.py
├── data/                       # Dữ liệu địa lý GeoJSON, TLE vệ tinh, cấu hình CSV & log thực nghiệm
├── templates/                  # Giao diện Web Console
│   ├── index.html              # Trang Bản đồ Chiến thuật
│   └── dashboard.html          # Trang Dashboard Phân tích
├── static/                     # Tài nguyên tĩnh & thư viện Leaflet cục bộ
├── tests/                      # Bộ kiểm thử tự động (Unit & Integration Tests)
├── AGENT.md                    # Quy chuẩn kiến trúc & thông số dự án
├── main.py                     # Điểm khởi chạy ứng dụng FastAPI
└── requirements.txt            # Danh sách thư viện Python
```

---

## 5. Hướng dẫn Cài đặt & Chạy Chương trình

### Yêu cầu hệ thống
- Python `3.10+`

### Bước 1: Cài đặt thư viện
```bash
pip install -r requirements.txt
```

### Bước 2: Khởi chạy máy chủ mô phỏng
```bash
python main.py
```
*(Hoặc chạy qua Uvicorn: `uvicorn main:app --reload --host 0.0.0.0 --port 8000`)*

### Bước 3: Mở giao diện trên trình duyệt
- **Bản đồ Chiến thuật:** `http://localhost:8000/`
- **Dashboard Phân tích:** `http://localhost:8000/dashboard`

---

## 6. Kịch bản Demo Nhanh (Dành cho Báo cáo / Bảo vệ Đồ án)

1. **Demo 1 – Tuần tra Tự chủ & Heatmap Độ bất định:**
   - Quan sát 28 UAV tự tản ra (Anti-Clustering) quét sạch các vùng vàng cam về màu xanh an toàn trong bán kính `600m`, trong khi các vùng chưa có UAV bay qua sẽ dần ngả vàng theo thời gian (`0.025/s`).
2. **Demo 2 – Báo cháy, Khoanh vùng Liên tục & Tuyến E2E Đa chặng:**
   - Bấm nút **Báo cháy** ở góc trên trái bản đồ rồi click vào vùng **Z6 hoặc Z7** (sườn Đông bị núi che khuất).
   - Quan sát UAV gần nhất trong tầm `600m` khóa mục tiêu, bay vòng tròn khoanh vùng `130m` liên tục quanh ngọn lửa và duy trì **Tuyến E2E xanh dương** truyền đa chặng qua FANET/LEO về `SƠN_TRÀ` cho đến khi bấm **Xoá cháy**.
3. **Demo 3 – Thay ca Khép kín & Bàn giao Cháy khi Sập nguồn / Pin yếu:**
   - Chọn UAV đang tuần tra hoặc đang khoanh vùng cháy rồi bấm **Pin yếu (18%)** hoặc **Sập (0%)**.
   - Quan sát UAV dự bị `STANDBY` từ Trạm Kiểm Lâm (`DỰ BỊ 7/7` $\rightarrow$ `6/7`) lập tức cất cánh bay ra vùng thiếu quân để giữ `4/4`, đồng đội cùng vùng tiếp quản đám cháy nếu đang có sự cố, còn UAV yếu pin bay thẳng về Trạm sạc nhanh `+15%/s` rồi quay lại hàng ngũ `STANDBY`.
