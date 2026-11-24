# SAGSINS MAS — Hệ thống Đa tác nhân, Hệ điều hành & Mạng máy tính (PBL4)

Dự án mô phỏng **Hệ thống Đa tác nhân (Multi-Agent System - MAS)** kết hợp **Lõi Hệ điều hành** và **Chồng giao thức Mạng máy tính TCP/IP chạy thật** trên kiến trúc mạng tích hợp 3 tầng **Không gian – Trên không – Mặt đất (SAGSINS)** phục vụ bài toán tuần tra tự chủ, phát hiện sớm đám cháy, tự chữa lành mạng và thay ca khép kín cho đội bay UAV tại **Bán đảo Sơn Trà (Đà Nẵng)**.

---

## 1. Kiến trúc Hệ thống 3 Tầng (39 Tác nhân Tự trị)

Hệ thống khởi tạo **39 tác nhân tự trị (Autonomous Agents)**, mỗi node được cấp phát địa chỉ **IPv4 theo Subnet riêng**:

| Tầng mạng | Số lượng | Định danh & Subnet IPv4 | Vai trò chính |
| :--- | :---: | :--- | :--- |
| **Tầng Mặt đất (Ground)** | `01` | `TRẠM_KIỂM_LÂM_SƠN_TRÀ`<br>`10.0.0.1` `(16.105, 108.250)` | Trung tâm chỉ huy, Socket Server TCP/UDP thật, trạm gốc định tuyến và **Trạm sạc giới hạn 3 cổng (`Semaphore(3)`)**. Kết hợp lưới điểm nhiệt Heatmap (`HeatNode`) trên 7 phân khu. |
| **Tầng Trên không (Air - FANET)** | `35` | `MÁY_BAY_UAV_01` ➔ `35`<br>Tuần tra: `10.0.<1..7>.<1..4>`<br>Dự bị: `10.0.8.<1..7>` | **28 UAV tuần tra chủ động** (`UAV_01`–`28`, chia đều **4 UAV/vùng** `Z1`–`Z7`, pin khởi tạo lệch pha `45%–100%`) và **07 UAV dự bị trực chiến** (`UAV_29`–`35`, pin `100%`, `STANDBY` tại Trạm). |
| **Tầng Không gian (Space - LEO)** | `03` | `VỆ_TINH_LEO_01` ➔ `03`<br>`10.0.99.<1..3>` | Vệ tinh quỹ đạo thấp (quỹ đạo SGP4 thực tế từ CelesTrak/NORAD kèm bộ đệm CSV Offline) làm kênh truyền dự phòng (Space Backhaul) khi đứt kết nối mặt đất. |

---

## 2. Thành phần Môn học PBL4: Hệ điều hành & Mạng máy tính

### 2.1. Lõi Hệ điều hành (`core/os_kernel.py`)
1. **Counting Semaphore Trạm sạc (`ChargingDockSemaphore(3)`):**
   - Trạm Kiểm Lâm giới hạn tối đa **3 đế sạc hoạt động đồng thời** thông qua `threading.Semaphore(3)` và `RLock` đảm bảo công bằng FIFO.
   - Nếu có từ 4 UAV pin yếu cùng bay về trạm, 3 chiếc đầu gọi `acquire()` thành công vào trạng thái `CHARGING` (`+15%/giây`), các chiếc còn lại bị chặn ở trạng thái **`WAITING_DOCK`** (hàng đợi chờ I/O). Khi 1 chiếc đầy `100%` gọi `release()`, UAV chờ đầu hàng lập tức được cấp cổng.
2. **Loại trừ Tương hỗ bằng Mutex (`threading.RLock`):**
   - Miền găng khóa mục tiêu đám cháy (`khoa_muc_tieu_chay`), thống kê mạng và bộ đệm Packet Sniffer (`net_state_lock`) được bảo vệ bằng `threading.RLock()`, ngăn chặn **Race Condition** khi nhiều UAV cùng phát hiện 1 đám cháy trong cùng tick mô phỏng.
3. **Lập lịch Hàng đợi Ưu tiên (`BoundedPriorityQueue` — Producer/Consumer):**
   - Mỗi tác nhân sử dụng hàng đợi ưu tiên 3 cấp giới hạn `100` phần tử:
     - **`P0` (Cao nhất):** `FIRE_ALERT`, `FIRE_DETECTION_REPORT`, `YEU_CAU_HO_TRO_CHAY_P2P`, `FIRE_CONTAINMENT_TELEMETRY`, `XAC_NHAN_ACK`.
     - **`P1` (Trung bình):** `TELEMETRY_REPORT`, `YEU_CAU_BAN_GIAO_VUNG`, `THONG_BAO_KHOI_PHUC_VUNG`.
     - **`P2` (Thấp nhất):** `HELLO_BEACON`.
   - Khi tràn bộ đệm, hệ thống ghi nhận `Buffer Overflow Drop` để bảo vệ bộ nhớ.
4. **Đa luồng (`Threading` & `ThreadPoolExecutor`) & OS Monitor:**
   - Tách luồng **TCP Server**, luồng **UDP Server**, **Worker ThreadPool (4 luồng)** xử lý cập nhật Heatmap song song và luồng chính điều phối.
   - Đọc trực tiếp tài nguyên tiến trình thật từ OS API (`PID`, danh sách `Thread TID`, `CPU %`, `RAM Working Set MB`).

### 2.2. Lõi Mạng máy tính (`core/network_stack.py` & `core/message_broker.py`)
1. **Đóng gói Khung nhị phân 26-byte Header + Checksum CRC32 + TTL:**
   - Gói tin chuẩn gồm `Version/Proto`, `Priority`, `Seq`, `Ack`, `TTL` (mặc định `16`, giảm `1` sau mỗi chặng — hủy gói `TTL_DROP` khi `TTL == 0`), `Flags`, `Src IP`, `Dst IP`, `Payload Length` và **`CRC32`** tính bằng `zlib.crc32()`.
2. **Giao vận Socket TCP & UDP Thật trên `127.0.0.1`:**
   - **TCP (ARQ Tin cậy có ACK & Truyền lại):** Áp dụng cho cảnh báo cháy, khoanh vùng và bàn giao khẩn P2P. Khi nhiễu kênh truyền `PER` làm đảo bit khung dây, bên nhận phát hiện lỗi qua `CRC32_MISMATCH` và tự động **truyền lại (`RETRANS` tối đa 3 lần)** cho tới khi nhận được gói nhị phân `ACK`.
   - **UDP (Không kết nối):** Áp dụng cho `TELEMETRY_REPORT` và `HELLO_BEACON` định kỳ; gửi datagram 1 lần và hủy ngay nếu hỏng `CRC32`.
3. **Định tuyến Đa chặng Dijkstra E2E & Bảng định tuyến L3:**
   - Mỗi node duy trì bảng định tuyến `L3 Routing Table` (`Dest`, `Next Hop`, `Interface`, `Metric`, `TTL`) phục vụ chọn tuyến A2G ➔ FANET Mesh (`<= 2200m`) ➔ Vệ tinh LEO Fallback, đồng thời ưu tiên hiển thị liên tục tuyến E2E xanh dương của UAV đang khoanh vùng cháy.

---

## 3. Tự chủ Bầy đàn UAV, Heatmap & Vòng đời Khép kín

- **Bản đồ Độ bất định Heatmap ([`environment/scenario_manager.py`](environment/scenario_manager.py)):** Làm mát tỏa mượt từ tâm UAV trong `Cooling Radius = 600m` (`Cooling Rate = 0.35/s`) về `0.05` (xanh lá); ngoài tầm quét tăng `Staleness Rate = 0.025/s` đến trần `0.68` (vàng cam). Đám cháy thật (`active_fires`, mức `1.0` đỏ) giữ nguyên tới khi bấm **Xoá cháy**.
- **Điều hướng Trường Thế năng & Chống tụ cụm ([`agents/uav_agent.py`](agents/uav_agent.py)):** Hút về `HeatNode` cao nhất trong vùng kết hợp **Virtual Pheromone** (giảm `80%` lực hút nếu có đồng đội gần hơn), lực đẩy **Inter-UAV Repulsion** (`< 1200m`, trọng số `2.5x`) và trượt biên Ray-Casting. Tắt lực hút điểm nóng khi `STANDBY`, `WAITING_DOCK`, `CHARGING`, `RETURNING`.
- **Xử lý Sự cố Khi Đang Khoanh Vùng Cháy (`130m`):** Nếu UAV đang giữ đám cháy tụt pin `< 25%` hoặc bị bấm **Sập (`0%` - `OFFLINE`)**, hệ thống lập tức mở khóa cháy và bàn giao P2P `YEU_CAU_HO_TRO_CHAY_P2P` cho đồng đội có pin cao nhất đang bay trong cùng phân khu, đồng thời đánh thức 1 UAV `STANDBY` ra bù đủ `4/4`.
- **Chu trình Thay ca & Sạc Semaphore Khép kín:**
  `PATROLLING` ➔ `Pin < 25%` (gọi dự bị `STANDBY` bay ra thế chỗ) ➔ `RETURNING` ➔ (về cách Trạm `<= 100m`, gọi `Semaphore.try_acquire_dock`) ➔ **`CHARGING` (`+15%/s`, tối đa 3 UAV)** hoặc **`WAITING_DOCK` (xếp hàng FIFO)** ➔ đầy `100%` (`release_dock`) ➔ `STANDBY`.

---

## 4. Giao diện Điều hành & Giám sát Thời gian thực

1. **Bản đồ Chiến thuật (`/` — [`templates/index.html`](templates/index.html)):**
   - Topbar hiển thị 5 chỉ số gọn (`UAV kết nối`, `7 Phân khu`, `PDR`, `Trễ TB`, **`Sạc Sem x/3`**).
   - Sidebar trên có 2 tab chuyển mượt: **`Tổng quan`** (Phát hiện cháy, Heatmap TB, Tuyến E2E kèm `TCP/UDP & TTL`) và **`Mạng & HĐH`** (Semaphore trạm sạc, Socket Ports, **Bảng định tuyến L3** theo UAV đang chọn).
   - Danh sách 39 node hiển thị địa chỉ IPv4, phân biệt `[DỰ BỊ]`, `[CHỜ SẠC]`, `[ĐANG SẠC]`, và ẩn vòng quét/tia tại khu vực Trạm.
2. **Dashboard Hệ điều hành & Mạng (`/dashboard` — [`templates/dashboard.html`](templates/dashboard.html)):**
   - 6 thẻ KPI bao gồm PDR/TCP ACK/Retrans, CRC32/TTL Drop, Semaphore Trạm sạc, OS Threads/RAM/CPU/PID.
   - **Cột 1 (Mạng máy tính):** **Packet Sniffer** thời gian thực (bắt từng gói `Seq`, `TCP/UDP`, `P0..P2`, `IP ➔ IP`, `TTL`, `CRC32`, `ACK/RETRANS/ERR`) + tab **Phản ứng Cháy** + chuỗi E2E.
   - **Cột 2 (Hệ điều hành):** Trực quan 3 **Dock Sạc Semaphore (`DOCK #1..#3`)** & hàng đợi `WAITING_DOCK`, thống kê **Priority Scheduler (`P0/P1/P2`)** & **Mutex `RLock`**, bảng **OS Threads thật** + tab **Ma trận 7 Vùng**.
   - **Cột 3 (Nhật ký Giao thức):** Lọc tức thì theo 4 nhóm **Tất cả / Mạng / HĐH / Bầy đàn**, hỗ trợ xuất CSV `/api/xuat-csv`.

---

## 5. Cấu trúc Thư mục Dự án

```text
PBL4/
├── agents/                     # Các tác nhân tự trị (UAV, Vệ tinh LEO, Trạm mặt đất)
├── core/
│   ├── os_kernel.py            # Semaphore(3), Mutex RLock, BoundedPriorityQueue, ThreadPool & OS Monitor
│   ├── network_stack.py        # IPv4 Subnet, 26B Packet + CRC32 + TTL, Real TCP/UDP Sockets & Sniffer
│   ├── base_agent.py           # Lớp cơ sở Node (IP, Priority Queue, L3 Routing Table, FSM)
│   ├── message_broker.py       # Định tuyến Dijkstra, kênh PHY, ARQ Retry qua Socket & WebSocket
│   └── ground_station_server.py# Giám sát trạng thái đám cháy & đồng bộ Web UI
├── environment/                # PhysicsEngine (3D ECEF, FSPL, SNR, PER), ScenarioManager, SimulationLoop
├── api/                        # FastAPI REST & WebSocket (/ws/mophong, /api/routing-table/{id}, /api/xuat-csv)
├── data/                       # GeoJSON 7 phân khu Sơn Trà, TLE vệ tinh, CSV cấu hình & log QoS
├── templates/                  # Giao diện Dark Console (index.html, dashboard.html)
├── static/                     # Thư viện Leaflet cục bộ
├── tests/                      # Unit & Integration Tests
├── AGENT.md                    # Quy chuẩn kiến trúc & thông số
├── main.py                     # Điểm khởi chạy Server chính (FastAPI + TCP :9000 + UDP :9001)
├── uav_client.py               # Tiến trình UAV Client độc lập kết nối TCP/UDP từ Terminal/Máy khác
└── requirements.txt            # Thư viện Python phụ thuộc
```

---

## 6. Hướng dẫn Cài đặt & Chạy Chương trình

### Chế độ 1: Chạy Server Trạm Kiểm Lâm & Bản đồ (`Terminal 1` / `Máy 1`)
```bash
pip install -r requirements.txt
python main.py
```
Mở trình duyệt tại:
- **Bản đồ Chiến thuật:** `http://127.0.0.1:8000/`
- **Dashboard HĐH & Mạng:** `http://127.0.0.1:8000/dashboard`

### Chế độ 2: Kết nối UAV Client Độc lập theo mô hình Client–Server (`Terminal 2` hoặc `Máy 2` chung Wi-Fi)
Khi `main.py` khởi chạy, Terminal sẽ in sẵn địa chỉ IP LAN và câu lệnh mẫu. Mở cửa sổ Terminal khác (hoặc laptop thứ 2 chung mạng LAN) và chạy:
```bash
python uav_client.py --host 127.0.0.1 --id UAV_EXT_01 --zone ZONE_03
```
*(Nếu chạy từ máy tính thứ 2, thay `127.0.0.1` bằng IP LAN của máy chạy Server, ví dụ `--host 192.168.1.15`).*

---

## 7. Kịch bản Demo Nhanh Bảo vệ PBL4

1. **Demo Mạng máy tính (TCP ARQ vs UDP, TTL, CRC32 & Routing Table):**
   - Mở `/dashboard` ➔ Cột 1 (**Packet Sniffer**) quan sát các gói `TELEM` chạy giao thức `UDP` (`10.0.x.y ➔ 10.0.0.1`) kèm `CRC32` và `TTL`.
   - Bấm **Báo cháy** ở vùng Z6/Z7 trên bản đồ ➔ Quan sát gói cảnh báo chạy `TCP` (`P0`), nếu dính nhiễu `PER` sẽ tự truyền lại (`RETRANS_OK`) trong log `[TCP_ARQ]` và nhận `ACK` thật qua Socket TCP. Bấm vào 1 UAV để xem **Bảng định tuyến L3** trong tab `Mạng & HĐH`.
2. **Demo Hệ điều hành (Counting Semaphore, Mutex & Priority Scheduling):**
   - Bấm **Pin yếu (18%)** cho 4–5 UAV liên tiếp ➔ Quan sát ở tab **OS Kernel & Sync** (và topbar `Sạc Sem`) đúng **3 UAV** chiếm `DOCK #1..#3` (`CHARGING`), các UAV về sau bị chặn ở hàng đợi **`WAITING_DOCK`**; ngay khi 1 chiếc sạc đầy `100%` gọi `release()`, chiếc chờ tiếp theo tự động vào sạc.
   - Bảng **Priority Queue** hiển thị gói cháy `P0` được xử lý ưu tiên trước `P1`/`P2`, cùng danh sách các **Thread OS thật** (`SAGSINS-TCP-Server`, `SAGSINS-UDP-Server`, `SAGSINS-Worker-*`).
3. **Demo Client–Server Đa tiến trình & Bắt gói Wireshark:**
   - Chạy `python uav_client.py --host <IP_SERVER> --id UAV_EXT_01 --zone ZONE_03` ở Terminal/máy khác ➔ quan sát trên bản đồ xuất hiện ngay chiếc `[CLIENT] UAV_EXT_01` màu xanh ngọc kèm **PID riêng** và Thread `SAGSINS-TCP-Client-*` trên Server.
   - Dùng phím `[f]` báo cháy qua TCP, `[b]` xin cổng sạc Semaphore, hoặc bấm `Ctrl+C` tắt đột ngột Client để thấy Server tự phát hiện ngắt kết nối TCP và giải phóng tài nguyên. Có thể mở **Wireshark** với filter `tcp.port == 9000 || udp.port == 9001` để bắt trực tiếp gói tin thật.
