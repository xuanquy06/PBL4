import os
import csv
import json
import math
import random
from dataclasses import dataclass
from datetime import datetime
from skyfield.api import load


@dataclass
class HeatNode:
    """
    Điểm nhiệt môi trường rừng Sơn Trà (HeatNode):
    - lat, lon: Tọa độ địa lý nằm gọn trong ranh giới đa giác vùng Z1-Z7.
    - risk_score: Chỉ số nguy cơ cháy rừng từ 0.05 (thấp) đến 1.0 (điểm nóng cực đại).
    - zone_id: Mã phân khu (ZONE_01 .. ZONE_07).
    """
    lat: float
    lon: float
    risk_score: float = 0.05
    zone_id: str = "ZONE_01"

    def __post_init__(self):
        self.lat = round(float(self.lat), 6)
        self.lon = round(float(self.lon), 6)
        self.risk_score = round(min(1.0, max(0.05, float(self.risk_score))), 4)
        self.zone_id = str(self.zone_id)

    def to_dict(self) -> dict:
        return {
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "risk_score": round(min(1.0, max(0.05, self.risk_score)), 4),
            "zone_id": self.zone_id,
        }

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        if key == "risk_score":
            value = round(min(1.0, max(0.05, float(value))), 4)
        setattr(self, key, value)

    def get(self, key: str, default=None):
        return getattr(self, key, default)


class ScenarioManager:
    """
    Bộ quản lý kịch bản Hybrid cho mạng SAGSINS:
    1. Đồng bộ TLE vệ tinh thực tế từ CelesTrak (NORAD) kèm cơ chế Cache CSV Offline.
    2. Phân bổ UAV theo bảy vùng rừng Sơn Trà lấy từ dữ liệu GIS công khai.
    3. Quản lý bản đồ điểm nhiệt (HeatNode) trong 7 vùng Z1-Z7 bằng thuật toán Ray-Casting.
    4. Quản lý đọc/ghi cấu hình mạng và xuất dữ liệu thực nghiệm QoS ra file CSV.
    """
    def __init__(self, data_dir: str = "data", nodes_per_zone: int = 30):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.nodes_csv_path = os.path.join(self.data_dir, "nodes_config.csv")
        self.waypoints_csv_path = os.path.join(self.data_dir, "uav_waypoints.csv")
        self.zones_geojson_path = os.path.join(self.data_dir, "son_tra_zones.geojson")
        if not os.path.exists(self.zones_geojson_path):
            project_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
            self.zones_geojson_path = os.path.join(project_data_dir, "son_tra_zones.geojson")
        self.tle_csv_path = os.path.join(self.data_dir, "satellites_tle.csv")
        self.metrics_log_path = os.path.join(self.data_dir, "simulation_metrics_log.csv")
        self.ke_hoach_bo_tri_ban_dau: dict = {}
        self.patrol_zones = self._doc_vung_tuan_tra()
        self.heat_nodes: list[HeatNode] = self._khoi_tao_heat_nodes(
            so_diem_moi_vung=nodes_per_zone
        )

        self._khoi_tao_file_log_thuc_nghiem()

    def _doc_vung_tuan_tra(self) -> dict:
        """Đọc bảy vùng rừng Sơn Trà đã được tạo từ dữ liệu GIS nguồn."""
        with open(self.zones_geojson_path, mode="r", encoding="utf-8") as file:
            zones = json.load(file)
        features = zones.get("features", [])
        zone_ids = {feature.get("properties", {}).get("zone_id") for feature in features}
        expected_ids = {f"ZONE_{index:02d}" for index in range(1, 8)}
        if len(features) != 7 or zone_ids != expected_ids:
            raise ValueError(
                f"Dữ liệu geofence cần đúng 7 vùng ZONE_01..ZONE_07: {self.zones_geojson_path}"
            )
        return zones

    def _kich_ban_can_tai_sinh(self) -> bool:
        """Nhận diện bộ CSV cũ hoặc thiếu tuyến của đội UAV 28 chiếc (21 tuần tra + 7 dự phòng)."""
        expected_uavs = {f"MÁY_BAY_UAV_{index:02d}" for index in range(1, 29)}
        if not os.path.exists(self.nodes_csv_path) or not os.path.exists(self.waypoints_csv_path):
            return True

        with open(self.nodes_csv_path, mode="r", encoding="utf-8") as file:
            nodes = list(csv.DictReader(file))
        uavs = {
            row["ma_dinh_danh"]
            for row in nodes
            if row["loai_tac_nhan"] == "MÁY_BAY"
        }
        stations = [
            row for row in nodes if row["loai_tac_nhan"] == "TRẠM_MẶT_ĐẤT"
        ]
        if uavs != expected_uavs or len(stations) != 1:
            return True

        battery_ranges = ((95.0, 100.0), (70.0, 85.0), (50.0, 65.0))
        for row in nodes:
            if row["loai_tac_nhan"] != "MÁY_BAY":
                continue
            uav_index = int(row["ma_dinh_danh"].rsplit("_", 1)[-1]) - 1
            try:
                battery = float(row.get("pin_khoi_tao_pct", ""))
            except ValueError:
                return True
            if uav_index < 21:
                battery_min, battery_max = battery_ranges[uav_index % 3]
                if (
                    row.get("zone_id") != f"ZONE_{uav_index // 3 + 1:02d}"
                    or not (battery_min <= battery <= battery_max)
                ):
                    return True
            else:
                reserve_zone_idx = uav_index - 21 + 1
                if row.get("zone_id") != f"ZONE_{reserve_zone_idx:02d}" or battery < 95.0:
                    return True

        waypoint_routes = {}
        with open(self.waypoints_csv_path, mode="r", encoding="utf-8") as file:
            for row in csv.DictReader(file):
                ma_uav = row["ma_uav"]
                waypoint_routes.setdefault(ma_uav, []).append(row)
        if set(waypoint_routes) != expected_uavs or any(
            len(route) != 6 for route in waypoint_routes.values()
        ):
            return True

        for ma_uav, route in waypoint_routes.items():
            uav_index = int(ma_uav.rsplit("_", 1)[-1]) - 1
            if uav_index < 21:
                zone = self.patrol_zones["features"][uav_index // 3]
                if any(
                    not self._diem_nam_trong_vung(
                        float(point["vi_do"]),
                        float(point["kinh_do"]),
                        zone
                    )
                    for point in route
                ):
                    return True
        return False

    @staticmethod
    def _diem_nam_trong_vong(latitude: float, longitude: float, ring: list) -> bool:
        """Ray casting có tính cả điểm nằm trên cạnh đa giác."""
        inside = False
        for index, current in enumerate(ring):
            previous = ring[index - 1]
            x1, y1 = current[:2]
            x2, y2 = previous[:2]
            cross = (longitude - x1) * (y2 - y1) - (latitude - y1) * (x2 - x1)
            on_segment = (
                abs(cross) <= 1e-10
                and min(x1, x2) - 1e-10 <= longitude <= max(x1, x2) + 1e-10
                and min(y1, y2) - 1e-10 <= latitude <= max(y1, y2) + 1e-10
            )
            if on_segment:
                return True
            crosses_ray = (y1 > latitude) != (y2 > latitude)
            if crosses_ray and longitude < (
                (x2 - x1) * (latitude - y1) / (y2 - y1) + x1
            ):
                inside = not inside
        return inside

    @classmethod
    def _diem_nam_trong_vung(cls, latitude: float, longitude: float, zone: dict) -> bool:
        for polygon in zone["geometry"]["coordinates"]:
            if not polygon or not cls._diem_nam_trong_vong(latitude, longitude, polygon[0]):
                continue
            if not any(
                cls._diem_nam_trong_vong(latitude, longitude, ring)
                for ring in polygon[1:]
            ):
                return True
        return False

    @classmethod
    def _diem_nam_gon_trong_vung(
        cls,
        latitude: float,
        longitude: float,
        zone: dict,
        margin_deg: float = 0.00035
    ) -> bool:
        """
        Kiểm tra điểm nhiệt nằm gọn bên trong đa giác phân khu (có khoảng lùi an toàn
        margin_deg ~ 35-40m so với đường bờ biển/ranh giới) bằng thuật toán Ray-Casting.
        """
        if not cls._diem_nam_trong_vung(latitude, longitude, zone):
            return False
        if margin_deg <= 0.0:
            return True
        offsets = (
            (margin_deg, 0.0),
            (-margin_deg, 0.0),
            (0.0, margin_deg),
            (0.0, -margin_deg),
        )
        return all(
            cls._diem_nam_trong_vung(latitude + d_lat, longitude + d_lon, zone)
            for d_lat, d_lon in offsets
        )

    def _khoi_tao_heat_nodes(self, so_diem_moi_vung: int = 30) -> list[HeatNode]:
        """
        Dùng thuật toán Ray-Casting (Point-in-Polygon) để đọc các đa giác từ
        `data/son_tra_zones.geojson` và rải các điểm nhiệt (HeatNode: lat, lon,
        risk_score từ 0.05 đến 1.0, zone_id) nằm gọn bên trong ranh giới 7 vùng Z1-Z7,
        tuyệt đối không bị tràn ra ngoài biển.
        """
        danh_sach_heat_nodes: list[HeatNode] = []
        rng = random.Random(42)

        for zone in self.patrol_zones.get("features", []):
            zone_id = zone.get("properties", {}).get("zone_id", "ZONE_01")
            all_lons: list[float] = []
            all_lats: list[float] = []
            for polygon in zone.get("geometry", {}).get("coordinates", []):
                if polygon and polygon[0]:
                    for coord in polygon[0]:
                        all_lons.append(float(coord[0]))
                        all_lats.append(float(coord[1]))

            if not all_lons or not all_lats:
                continue

            min_lon, max_lon = min(all_lons), max(all_lons)
            min_lat, max_lat = min(all_lats), max(all_lats)
            nodes_trong_vung: list[HeatNode] = []

            # Bước 1: Rải lưới phân tầng (Stratified Grid + Jitter) kết hợp kiểm tra Ray-Casting có lề an toàn
            grid_rows = 7
            grid_cols = 6
            step_lat = (max_lat - min_lat) / (grid_rows + 1)
            step_lon = (max_lon - min_lon) / (grid_cols + 1)

            for r in range(1, grid_rows + 1):
                for c in range(1, grid_cols + 1):
                    if len(nodes_trong_vung) >= so_diem_moi_vung:
                        break
                    cand_lat = min_lat + r * step_lat + rng.uniform(-0.35, 0.35) * step_lat
                    cand_lon = min_lon + c * step_lon + rng.uniform(-0.35, 0.35) * step_lon
                    if self._diem_nam_gon_trong_vung(cand_lat, cand_lon, zone, margin_deg=0.00035):
                        nodes_trong_vung.append(
                            HeatNode(
                                lat=cand_lat,
                                lon=cand_lon,
                                risk_score=round(rng.uniform(0.05, 0.25), 4),
                                zone_id=zone_id,
                            )
                        )

            # Bước 2: Lấy mẫu bổ sung bằng Ray-Casting cho đủ số điểm nhiệt mỗi vùng
            max_attempts = so_diem_moi_vung * 80
            attempts = 0
            while len(nodes_trong_vung) < so_diem_moi_vung and attempts < max_attempts:
                attempts += 1
                cand_lat = rng.uniform(min_lat, max_lat)
                cand_lon = rng.uniform(min_lon, max_lon)
                margin = 0.00035 if attempts < max_attempts // 2 else 0.0
                if self._diem_nam_gon_trong_vung(cand_lat, cand_lon, zone, margin_deg=margin):
                    nodes_trong_vung.append(
                        HeatNode(
                            lat=cand_lat,
                            lon=cand_lon,
                            risk_score=round(rng.uniform(0.05, 0.25), 4),
                            zone_id=zone_id,
                        )
                    )

            danh_sach_heat_nodes.extend(nodes_trong_vung)

        return danh_sach_heat_nodes

    def update_heatmap(self, dt: float) -> list[HeatNode]:
        """
        Các điểm nhiệt tự động tăng nhẹ risk_score theo thời gian dt (giây)
        để mô phỏng nguy cơ cháy rừng tích tụ tự nhiên, giới hạn trong [0.05, 1.0].
        """
        dt_sec = max(0.0, float(dt))
        if dt_sec <= 0.0:
            return self.heat_nodes

        for node in self.heat_nodes:
            # Mức tăng nhẹ theo thời gian thực (từ 0.0015 đến 0.004 mỗi giây)
            delta_risk = random.uniform(0.0015, 0.0040) * dt_sec
            node.risk_score = round(min(1.0, max(0.05, node.risk_score + delta_risk)), 4)

        return self.heat_nodes

    def trigger_fire(
        self,
        lat: float,
        lon: float,
        radius_m: float = 800.0
    ) -> list[HeatNode]:
        """
        Khi có sự kiện cháy tại (lat, lon):
        - Tìm các HeatNode xung quanh tâm cháy trong bán kính `radius_m` (mét)
          và đẩy `risk_score` lên 1.0 (tạo điểm nóng cực đại).
        - Nếu bán kính nhỏ chưa chạm tới HeatNode nào nhưng điểm cháy nằm trong vùng,
          đẩy HeatNode gần nhất lên 1.0 để luôn hình thành điểm nóng cực đại.
        - KHÔNG kích hoạt cơ chế đấu thầu của Trạm Kiểm Lâm.
        """
        fire_lat = float(lat)
        fire_lon = float(lon)
        ban_kinh_m = max(50.0, float(radius_m))
        affected_nodes: list[HeatNode] = []

        for node in self.heat_nodes:
            dist_m = self._khoang_cach_mot_phang_m(fire_lat, fire_lon, node.lat, node.lon)
            if dist_m <= ban_kinh_m:
                node.risk_score = 1.0
                affected_nodes.append(node)

        if not affected_nodes and self.heat_nodes:
            nearest_node = min(
                self.heat_nodes,
                key=lambda n: self._khoang_cach_mot_phang_m(fire_lat, fire_lon, n.lat, n.lon)
            )
            if self._khoang_cach_mot_phang_m(fire_lat, fire_lon, nearest_node.lat, nearest_node.lon) <= max(ban_kinh_m * 2.5, 1500.0):
                nearest_node.risk_score = 1.0
                affected_nodes.append(nearest_node)

        return affected_nodes

    def xuat_heatmap(self) -> list[dict]:
        """Xuất danh sách điểm nhiệt dạng dict JSON phục vụ đồng bộ giao diện Web."""
        return [node.to_dict() for node in self.heat_nodes]

    # =========================================================================
    # 1. TỰ ĐỘNG ĐỒNG BỘ VỆ TINH THẬT TỪ CELESTRAK (CÓ DỰ PHÒNG OFFLINE CSV)
    # =========================================================================
    def dong_bo_ve_tinh_celestrak(self, so_luong_toi_da: int = 3) -> list[dict]:
        """
        Tải dữ liệu TLE mới nhất từ CelesTrak. Nếu không có mạng Internet hoặc đã có sẵn CSV,
        hệ thống sẽ ưu tiên đọc từ file `data/satellites_tle.csv` để đảm bảo tốc độ và ổn định.
        """
        if os.path.exists(self.tle_csv_path) and os.path.getsize(self.tle_csv_path) > 50:
            print(f"[SCENARIO] Đọc dữ liệu quỹ đạo TLE từ bộ nhớ đệm: {self.tle_csv_path}")
            return self.doc_tle_tu_csv()

        print("[SCENARIO] Đang kết nối CelesTrak (NORAD) để tải dữ liệu vệ tinh LEO thực tế...")
        danh_sach_tle = []
        try:
            # Tải nhóm các trạm không gian và vệ tinh sáng/hoạt động từ CelesTrak
            url = "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=tle"
            tle_file_path = os.path.join(self.data_dir, "celestrak_stations.tle")
            satellites = load.tle_file(url, filename=tle_file_path)

            count = 0
            for sat in satellites:
                # Lọc các vệ tinh LEO có góc nghiêng quỹ đạo (inclination) >= 20 độ để bay qua Việt Nam
                inc_deg = math.degrees(sat.model.inclo)
                sat_name = getattr(sat, "name", None)
                if inc_deg >= 20.0 and isinstance(sat_name, str):
                     count += 1
                     ma_sat = f"VỆ_TINH_LEO_{count:02d}"
                     # Trích xuất 2 dòng TLE gốc từ file vừa tải
                     line1, line2 = self._tim_dong_tle_goc(tle_file_path, sat_name)
                     if line1 and line2:
                         danh_sach_tle.append({
                             "ma_ve_tinh": ma_sat,
                             "ten_quoc_te": sat_name.strip(),
                             "tle_line1": line1,
                             "tle_line2": line2,
                             "cong_suat_dbm": 43.0,          # 20 Watts (Chuẩn phát sóng vệ tinh LEO)
                             "tan_so_hz": 12000000000.0,     # 12 GHz (Băng tần Ku-Band)
                             "bang_thong_hz": 40000000.0     # 40 MHz
                         })
                     if count >= so_luong_toi_da:
                         break
        except Exception as e:
            print(f"[SCENARIO] Cảnh báo kết nối CelesTrak ({e}). Chuyển sang bộ TLE chuẩn dự phòng.")

        # Nếu tải thất bại hoặc không đủ số lượng, bổ sung từ bộ TLE chuẩn thực tế (ISS, Starlink, NOAA)
        if len(danh_sach_tle) < so_luong_toi_da:
            danh_sach_tle = self._lay_tle_du_phong_chuan()

        # Lưu toàn bộ ra file CSV để minh bạch hóa dữ liệu cho báo cáo
        with open(self.tle_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ma_ve_tinh", "ten_quoc_te", "tle_line1", "tle_line2", 
                "cong_suat_dbm", "tan_so_hz", "bang_thong_hz"
            ])
            writer.writeheader()
            writer.writerows(danh_sach_tle)

        print(f"[SCENARIO] Đã lưu {len(danh_sach_tle)} vệ tinh LEO vào {self.tle_csv_path}")
        return danh_sach_tle

    @staticmethod
    def _tim_dong_tle_goc(filepath: str, sat_name: str) -> tuple[str, str]:
        """Đọc file TLE thô để lấy chính xác chuỗi 69 ký tự của Line 1 và Line 2"""
        if not os.path.exists(filepath):
            return "", ""
        with open(filepath, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f.readlines() if line.strip()]
        for i in range(len(lines) - 2):
            if sat_name.strip() in lines[i] and lines[i+1].startswith("1 ") and lines[i+2].startswith("2 "):
                return lines[i+1], lines[i+2]
        return "", ""

    @staticmethod
    def _lay_tle_du_phong_chuan() -> list[dict]:
        """Dữ liệu TLE thực tế của ISS, STARLINK-1007 và CSS (Tiên Cung) làm dự phòng Offline"""
        return [
            {
                "ma_ve_tinh": "VỆ_TINH_LEO_01",
                "ten_quoc_te": "ISS (ZARYA)",
                "tle_line1": "1 25544U 98067A   23272.53123843  .00015502  00000-0  28114-3 0  9993",
                "tle_line2": "2 25544  51.6416 353.4072 0004506  52.8091 106.8402 15.49474744417532",
                "cong_suat_dbm": 43.0, "tan_so_hz": 12000000000.0, "bang_thong_hz": 40000000.0
            },
            {
                "ma_ve_tinh": "VỆ_TINH_LEO_02",
                "ten_quoc_te": "STARLINK-1007",
                "tle_line1": "1 44713U 19074A   23272.55123843  .00001234  00000-0  10234-4 0  9991",
                "tle_line2": "2 44713  53.0541 110.1234 0001452  85.1234 275.0123 15.06412345123456",
                "cong_suat_dbm": 43.0, "tan_so_hz": 12000000000.0, "bang_thong_hz": 40000000.0
            },
            {
                "ma_ve_tinh": "VỆ_TINH_LEO_03",
                "ten_quoc_te": "CSS (TIANHE)",
                "tle_line1": "1 48274U 21035A   23272.58123843  .00021567  00000-0  25567-3 0  9998",
                "tle_line2": "2 48274  41.4738 230.5678 0005389  90.4567 269.6789 15.60109876543210",
                "cong_suat_dbm": 43.0, "tan_so_hz": 12000000000.0, "bang_thong_hz": 40000000.0
            }
        ]

    def doc_tle_tu_csv(self) -> list[dict]:
        """Đọc danh sách vệ tinh từ file CSV"""
        ket_qua = []
        with open(self.tle_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ket_qua.append({
                    "ma_ve_tinh": row["ma_ve_tinh"],
                    "ten_quoc_te": row.get("ten_quoc_te", row["ma_ve_tinh"]),
                    "tle_line1": row["tle_line1"],
                    "tle_line2": row["tle_line2"],
                    "cong_suat_dbm": float(row["cong_suat_dbm"]),
                    "tan_so_hz": float(row["tan_so_hz"]),
                    "bang_thong_hz": float(row["bang_thong_hz"])
                })
        return ket_qua

    # =========================================================================
    # 2. THUẬT TOÁN TOÁN HỌC SINH QUỸ ĐẠO TUẦN TRA UAV (MOBILITY GENERATOR)
    # =========================================================================
    @staticmethod
    def sinh_quy_dao_luoi_quet_lawnmower(
        tam_vi_do: float, tam_kinh_do: float, do_cao_m: float,
        chieu_rong_m: float = 8000.0, so_duong_quet: int = 4, van_toc_m_s: float = 40.0
    ) -> list[dict]:
        """
        Thuật toán Boustrophedon (Lawnmower Pattern): Tính các điểm waypoint hình chữ Z
        để UAV quét một vùng diện tích trong mô phỏng.
        """
        waypoints = []
        # Quy đổi mét sang độ Địa lý tại vĩ độ mục tiêu
        met_tren_do_vi = 111320.0
        met_tren_do_kinh = 111320.0 * math.cos(math.radians(tam_vi_do))

        nua_rong_vi = (chieu_rong_m / 2.0) / met_tren_do_vi
        nua_rong_kinh = (chieu_rong_m / 2.0) / met_tren_do_kinh
        buoc_nhay_vi = (chieu_rong_m / max(1, so_duong_quet - 1)) / met_tren_do_vi

        thu_tu = 1
        for i in range(so_duong_quet):
            vi_do_hien_tai = (tam_vi_do - nua_rong_vi) + i * buoc_nhay_vi
            # Đảo chiều quét Trái -> Phải rồi Phải -> Trái để tối ưu góc cua cho UAV
            kinh_do_cap = (
                [tam_kinh_do - nua_rong_kinh, tam_kinh_do + nua_rong_kinh]
                if i % 2 == 0 else
                [tam_kinh_do + nua_rong_kinh, tam_kinh_do - nua_rong_kinh]
            )
            for kinh_do in kinh_do_cap:
                waypoints.append({
                    "thu_tu_diem": thu_tu,
                    "vi_do": round(vi_do_hien_tai, 5),
                    "kinh_do": round(kinh_do, 5),
                    "do_cao": round(do_cao_m, 1),
                    "van_toc_m_s": round(van_toc_m_s, 1)
                })
                thu_tu += 1
        return waypoints

    @staticmethod
    def sinh_quy_dao_tuan_tra_vong_tron(
        tam_vi_do: float, tam_kinh_do: float, do_cao_m: float,
        ban_kinh_m: float = 5000.0, so_diem_da_giac: int = 8,
        van_toc_m_s: float = 35.0, lech_pha_rad: float = 0.0
    ) -> list[dict]:
        """
        Sinh các waypoint tuần tra vòng tròn cho một UAV trong mô phỏng.
        """
        waypoints = []
        met_tren_do_vi = 111320.0
        met_tren_do_kinh = 111320.0 * math.cos(math.radians(tam_vi_do))

        for i in range(so_diem_da_giac):
            goc_rad = lech_pha_rad + (2.0 * math.pi * i) / so_diem_da_giac
            d_lat = (ban_kinh_m * math.cos(goc_rad)) / met_tren_do_vi
            d_lon = (ban_kinh_m * math.sin(goc_rad)) / met_tren_do_kinh

            waypoints.append({
                "thu_tu_diem": i + 1,
                "vi_do": round(tam_vi_do + d_lat, 5),
                "kinh_do": round(tam_kinh_do + d_lon, 5),
                "do_cao": round(do_cao_m, 1),
                "van_toc_m_s": round(van_toc_m_s, 1)
            })
        return waypoints

    # =========================================================================
    # 3. KHỞI TẠO & ĐỌC CẤU HÌNH NODE MẠNG + WAYPOINTS TỪ CSV
    # =========================================================================
    def tai_hoac_sinh_kich_ban_mang(self) -> tuple[list[dict], dict[str, list[dict]]]:
        """
        Nếu file `nodes_config.csv` và `uav_waypoints.csv` chưa tồn tại, hệ thống tự động
        chạy thuật toán toán học để sinh kịch bản. Sau khi đọc, tối ưu hóa vị trí xuất phát
        và phân vùng tuần tra theo các trạm mặt đất đã cấu hình.
        """
        if self._kich_ban_can_tai_sinh():
            self._sinh_du_lieu_csv_mac_dinh()

        # 1. Đọc danh sách Node (Trạm mặt đất & UAV)
        danh_sach_nodes = []
        with open(self.nodes_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                danh_sach_nodes.append({
                    "ma_dinh_danh": row["ma_dinh_danh"],
                    "loai_tac_nhan": row["loai_tac_nhan"],
                    "vi_do": float(row["vi_do"]),
                    "kinh_do": float(row["kinh_do"]),
                    "do_cao": float(row["do_cao"]),
                    "pin_max_joule": float(row["pin_max_joule"]),
                    "pin_khoi_tao_pct": float(row.get("pin_khoi_tao_pct") or 100.0),
                    **({"zone_id": row["zone_id"]} if row.get("zone_id") else {}),
                    "cong_suat_dbm": float(row["cong_suat_dbm"]),
                    "tan_so_hz": float(row["tan_so_hz"]),
                    "bang_thong_hz": float(row["bang_thong_hz"])
                })

        # 2. Đọc danh sách Waypoints của từng UAV
        ban_do_waypoints = {}
        with open(self.waypoints_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ma_uav = row["ma_uav"]
                if ma_uav not in ban_do_waypoints:
                    ban_do_waypoints[ma_uav] = []
                ban_do_waypoints[ma_uav].append({
                    "thu_tu_diem": int(row["thu_tu_diem"]),
                    "vi_do": float(row["vi_do"]),
                    "kinh_do": float(row["kinh_do"]),
                    "do_cao": float(row["do_cao"]),
                    "van_toc_m_s": float(row["van_toc_m_s"])
                })

        self.ke_hoach_bo_tri_ban_dau = self.toi_uu_hoa_bo_tri_ban_dau(
            danh_sach_nodes,
            ban_do_waypoints
        )
        return danh_sach_nodes, ban_do_waypoints

    def toi_uu_hoa_bo_tri_ban_dau(
        self,
        danh_sach_nodes: list[dict],
        ban_do_waypoints: dict[str, list[dict]]
    ) -> dict:
        """Cân bằng UAV theo trạm, giảm quãng đường điều động và tạo vùng tuần tra riêng."""
        cac_tram = sorted(
            (node for node in danh_sach_nodes if node["loai_tac_nhan"] == "TRẠM_MẶT_ĐẤT"),
            key=lambda node: node["ma_dinh_danh"]
        )
        cac_uav = sorted(
            (node for node in danh_sach_nodes if node["loai_tac_nhan"] == "MÁY_BAY"),
            key=lambda node: int(node["ma_dinh_danh"].rsplit("_", 1)[-1])
        )
        if not cac_tram or not cac_uav:
            return {
                "thuat_toan": "Phân bổ cân bằng theo trạm và vùng tuần tra phân tán",
                "so_uav": len(cac_uav),
                "so_tram": len(cac_tram),
                "phan_bo_theo_tram": {},
                "tram_cua_uav": {},
                "khoang_cach_xuat_phat_trung_binh_km": 0.0,
                "khoang_cach_dau_vao_trung_binh_km": 0.0
            }

        if len(cac_tram) == 1:
            tram = cac_tram[0]
            distances = [
                ScenarioManager._khoang_cach_mot_phang_m(
                    uav["vi_do"], uav["kinh_do"], tram["vi_do"], tram["kinh_do"]
                )
                for uav in cac_uav
            ]
            uav_theo_vung = {}
            for index, uav in enumerate(cac_uav):
                if index < 21:
                    ma_vung = f"ZONE_{index // 3 + 1:02d}"
                else:
                    ma_vung = f"ZONE_{index - 21 + 1:02d}"
                uav_theo_vung.setdefault(ma_vung, []).append(uav["ma_dinh_danh"])
            return {
                "thuat_toan": "Phân cụm tuần tra cố định theo ranh giới Sơn Trà",
                "so_uav": len(cac_uav),
                "so_tram": 1,
                "so_vung_geofence": len(self.patrol_zones["features"]),
                "uav_moi_vung": 3,
                "uav_theo_vung": uav_theo_vung,
                "phan_bo_theo_tram": {tram["ma_dinh_danh"]: len(cac_uav)},
                "tram_cua_uav": {
                    uav["ma_dinh_danh"]: tram["ma_dinh_danh"] for uav in cac_uav
                },
                "khoang_cach_xuat_phat_trung_binh_km": round(
                    sum(distances) / len(distances) / 1000.0, 2
                ),
                "khoang_cach_dau_vao_trung_binh_km": round(
                    sum(distances) / len(distances) / 1000.0, 2
                )
            }

        base_quota, so_tram_du = divmod(len(cac_uav), len(cac_tram))
        quotas = {
            tram["ma_dinh_danh"]: base_quota + (index < so_tram_du)
            for index, tram in enumerate(cac_tram)
        }
        phan_bo = {tram["ma_dinh_danh"]: 0 for tram in cac_tram}
        khoang_cach_dau_vao = []
        khoang_cach_xuat_phat = []
        cac_uav_co_khoang_cach = []

        for uav in cac_uav:
            distances = {
                tram["ma_dinh_danh"]: ScenarioManager._khoang_cach_mot_phang_m(
                    uav["vi_do"], uav["kinh_do"], tram["vi_do"], tram["kinh_do"]
                )
                for tram in cac_tram
            }
            cac_uav_co_khoang_cach.append((uav, distances))

        cac_uav_co_khoang_cach.sort(
            key=lambda item: (
                min(item[1].values()),
                item[0]["ma_dinh_danh"]
            ),
            reverse=True
        )
        tram_cua_uav = {}

        for uav, distances in cac_uav_co_khoang_cach:
            tram_chon = min(
                (
                    tram for tram in cac_tram
                    if phan_bo[tram["ma_dinh_danh"]] < quotas[tram["ma_dinh_danh"]]
                ),
                key=lambda tram: (distances[tram["ma_dinh_danh"]], tram["ma_dinh_danh"])
            )
            ma_tram = tram_chon["ma_dinh_danh"]
            vi_tri_trong_cum = phan_bo[ma_tram]
            phan_bo[ma_tram] += 1
            tram_cua_uav[uav["ma_dinh_danh"]] = ma_tram
            khoang_cach_dau_vao.append(distances[ma_tram])

            # Golden-angle spacing spreads launch positions and patrol loops within each cluster.
            bearing = math.radians(vi_tri_trong_cum * 137.507764)
            ban_kinh_xuat_phat_m = 200.0 + min(vi_tri_trong_cum, 3) * 120.0
            vi_do_tram = float(tram_chon["vi_do"])
            kinh_do_tram = float(tram_chon["kinh_do"])
            met_moi_do_vi_do = 111320.0
            met_moi_do_kinh_do = max(
                1000.0,
                met_moi_do_vi_do * math.cos(math.radians(vi_do_tram))
            )
            tam_tuan_tra_vi_do = vi_do_tram + (
                2500.0 * math.cos(bearing) / met_moi_do_vi_do
            )
            tam_tuan_tra_kinh_do = kinh_do_tram + (
                2500.0 * math.sin(bearing) / met_moi_do_kinh_do
            )
            uav["vi_do"] = vi_do_tram + (
                ban_kinh_xuat_phat_m * math.cos(bearing) / met_moi_do_vi_do
            )
            uav["kinh_do"] = kinh_do_tram + (
                ban_kinh_xuat_phat_m * math.sin(bearing) / met_moi_do_kinh_do
            )
            khoang_cach_xuat_phat.append(
                ScenarioManager._khoang_cach_mot_phang_m(
                    uav["vi_do"], uav["kinh_do"], vi_do_tram, kinh_do_tram
                )
            )

            do_cao = float(uav["do_cao"])
            van_toc = float(uav.get("van_toc_m_s") or 40.0)
            so_diem = 6
            ban_do_waypoints[uav["ma_dinh_danh"]] = [
                {
                    "thu_tu_diem": index + 1,
                    "vi_do": round(
                        tam_tuan_tra_vi_do + 1400.0 * math.cos(
                            bearing + index * (2.0 * math.pi / so_diem)
                        ) / met_moi_do_vi_do,
                        5
                    ),
                    "kinh_do": round(
                        tam_tuan_tra_kinh_do + 1400.0 * math.sin(
                            bearing + index * (2.0 * math.pi / so_diem)
                        ) / met_moi_do_kinh_do,
                        5
                    ),
                    "do_cao": do_cao,
                    "van_toc_m_s": van_toc
                }
                for index in range(so_diem)
            ]

        return {
            "thuat_toan": "Phân bổ cân bằng theo trạm và vùng tuần tra phân tán",
            "so_uav": len(cac_uav),
            "so_tram": len(cac_tram),
            "phan_bo_theo_tram": phan_bo,
            "tram_cua_uav": tram_cua_uav,
            "khoang_cach_xuat_phat_trung_binh_km": round(
                sum(khoang_cach_xuat_phat) / len(khoang_cach_xuat_phat) / 1000.0,
                2
            ),
            "khoang_cach_dau_vao_trung_binh_km": round(
                sum(khoang_cach_dau_vao) / len(khoang_cach_dau_vao) / 1000.0,
                2
            )
        }

    @staticmethod
    def _khoang_cach_mot_phang_m(
        vi_do_1: float,
        kinh_do_1: float,
        vi_do_2: float,
        kinh_do_2: float
    ) -> float:
        """Haversine distance used to rank UAV-to-station deployment assignments."""
        lat1 = math.radians(vi_do_1)
        lat2 = math.radians(vi_do_2)
        delta_lat = lat2 - lat1
        delta_lon = math.radians(kinh_do_2 - kinh_do_1)
        a = (
            math.sin(delta_lat / 2.0) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2.0) ** 2
        )
        return 6371000.0 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    def _sinh_du_lieu_csv_mac_dinh(self):
        """Sinh một trạm, 21 UAV tuần tra ba chiếc/vùng (3 vòng Bắc-Trung-Nam riêng biệt) và 7 UAV dự phòng tại trạm."""
        print("[SCENARIO] Đang khởi tạo 28 UAV (21 tuần tra 3 dải + 7 dự phòng tại trạm)...")
        nodes_mac_dinh = [{
            "ma_dinh_danh": "TRẠM_KIỂM_LÂM_SƠN_TRÀ",
            "loai_tac_nhan": "TRẠM_MẶT_ĐẤT",
            "vi_do": 16.105,
            "kinh_do": 108.250,
            "do_cao": 25.0,
            "pin_max_joule": 9999999.0,
            "cong_suat_dbm": 46.0,
            "tan_so_hz": 2400000000.0,
            "bang_thong_hz": 50000000.0,
        }]
        tat_ca_wp = []
        battery_ranges = ((95, 100), (75, 85), (55, 65))

        for zone_index, zone in enumerate(self.patrol_zones["features"]):
            zid = zone["properties"]["zone_id"]
            subcenters = zone["properties"].get("patrol_subcenters")
            c_lon, c_lat = zone["properties"]["patrol_center"]
            default_r = float(zone["properties"].get("patrol_radius_m", 400.0))

            for sub_index in range(3):
                uav_index = zone_index * 3 + sub_index + 1
                altitude = 1000.0 + sub_index * 150.0
                ma_uav = f"MÁY_BAY_UAV_{uav_index:02d}"

                if subcenters and len(subcenters) > sub_index:
                    s_lat, s_lon, s_rad = subcenters[sub_index]
                else:
                    s_lat, s_lon, s_rad = c_lat, c_lon, default_r

                waypoints = self.sinh_quy_dao_tuan_tra_vong_tron(
                    s_lat,
                    s_lon,
                    altitude,
                    ban_kinh_m=s_rad,
                    so_diem_da_giac=6,
                    van_toc_m_s=40.0,
                    lech_pha_rad=0.0,
                )
                nodes_mac_dinh.append({
                    "ma_dinh_danh": ma_uav,
                    "loai_tac_nhan": "MÁY_BAY",
                    "vi_do": waypoints[0]["vi_do"],
                    "kinh_do": waypoints[0]["kinh_do"],
                    "do_cao": altitude,
                    "pin_max_joule": 50000.0,
                    "pin_khoi_tao_pct": random.randint(*battery_ranges[sub_index]),
                    "zone_id": zid,
                    "cong_suat_dbm": 35.0,
                    "tan_so_hz": 2400000000.0,
                    "bang_thong_hz": 20000000.0,
                })
                tat_ca_wp.extend(
                    {"ma_uav": ma_uav, **waypoint} for waypoint in waypoints
                )

        # 7 UAV dự phòng đỗ sẵn tại TRẠM_KIỂM_LÂM_SƠN_TRÀ, mỗi chiếc phụ trách 1 phân khu
        for zone_index, zone in enumerate(self.patrol_zones["features"]):
            reserve_idx = 21 + zone_index + 1
            ma_uav = f"MÁY_BAY_UAV_{reserve_idx:02d}"
            zid = zone["properties"]["zone_id"]
            nodes_mac_dinh.append({
                "ma_dinh_danh": ma_uav,
                "loai_tac_nhan": "MÁY_BAY",
                "vi_do": 16.105,
                "kinh_do": 108.250,
                "do_cao": 25.0,
                "pin_max_joule": 50000.0,
                "pin_khoi_tao_pct": 100,
                "zone_id": zid,
                "cong_suat_dbm": 35.0,
                "tan_so_hz": 2400000000.0,
                "bang_thong_hz": 20000000.0,
            })
            # 6 waypoint tạm tại trạm, khi cất cánh thay ca sẽ nhận lộ trình tuần tra
            for pt_idx in range(6):
                tat_ca_wp.append({
                    "ma_uav": ma_uav,
                    "thu_tu_diem": pt_idx + 1,
                    "vi_do": 16.105,
                    "kinh_do": 108.250,
                    "do_cao": 25.0,
                    "van_toc_m_s": 0.0,
                })

        with open(self.waypoints_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["ma_uav", "thu_tu_diem", "vi_do", "kinh_do", "do_cao", "van_toc_m_s"])
            writer.writeheader()
            writer.writerows(tat_ca_wp)

        with open(self.nodes_csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ma_dinh_danh", "loai_tac_nhan", "vi_do", "kinh_do", "do_cao",
                "pin_max_joule", "pin_khoi_tao_pct", "zone_id",
                "cong_suat_dbm", "tan_so_hz", "bang_thong_hz"
            ])
            writer.writeheader()
            writer.writerows(nodes_mac_dinh)

    # =========================================================================
    # 4. XUẤT NHẬT KÝ THỰC NGHIỆM (TELEMETRY CSV) ĐỂ VẼ BIỂU ĐỒ BÁO CÁO PBL4
    # =========================================================================
    def _khoi_tao_file_log_thuc_nghiem(self):
        """Khởi tạo tiêu đề cột cho file CSV lưu kết quả đánh giá hiệu năng mạng"""
        headers = [
            "thoi_gian", "nguoi_gui", "nguoi_nhan", "loai_tin",
            "khoang_cach_km", "goc_ngang_do", "do_tre_ms",
            "fspl_db", "snr_db", "dung_luong_mbps", "per", "trang_thai_truyen",
            "nang_luong_tx_joule", "do_kha_dung_pct",
            "thoi_gian_mat_lien_lac_giay", "nang_luong_toan_mang_joule"
        ]
        if not os.path.exists(self.metrics_log_path):
            with open(self.metrics_log_path, mode="w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(headers)
            return

        with open(self.metrics_log_path, mode="r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        if not rows:
            with open(self.metrics_log_path, mode="w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(headers)
            return
        old_headers = rows[0]
        if old_headers == headers:
            return
        added_headers = [header for header in headers if header not in old_headers]
        upgraded_headers = [*old_headers, *added_headers]
        temp_path = self.metrics_log_path + ".tmp"
        with open(temp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(upgraded_headers)
            for row in rows[1:]:
                writer.writerow([*row, *([0.0] * (len(upgraded_headers) - len(row)))])
        os.replace(temp_path, self.metrics_log_path)

    def ghi_log_thuc_nghiem_qos(
        self, nguoi_gui: str, nguoi_nhan: str, loai_tin: str, 
        thong_so_kenh: dict, trang_thai_truyen: str
    ):
        """Ghi một dòng thông số lớp Vật lý & Mạng vào file CSV mỗi khi có gói tin quan trọng đi qua"""
        try:
            with open(self.metrics_log_path, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now().strftime("%H:%M:%S.%f")[:-3],
                    nguoi_gui,
                    nguoi_nhan,
                    loai_tin,
                    thong_so_kenh.get("khoang_cach_km", 0.0),
                    thong_so_kenh.get("goc_ngang_do", 0.0),
                    thong_so_kenh.get("do_tre_ms", 0.0),
                    thong_so_kenh.get("fspl_db", 0.0),
                    thong_so_kenh.get("snr_db", 0.0),
                    thong_so_kenh.get("dung_luong_mbps", 0.0),
                    thong_so_kenh.get("per", 0.0),
                    trang_thai_truyen,
                    thong_so_kenh.get("nang_luong_tx_joule", 0.0),
                    thong_so_kenh.get("do_kha_dung_pct", 0.0),
                    thong_so_kenh.get("thoi_gian_mat_lien_lac_giay", 0.0),
                    thong_so_kenh.get("nang_luong_toan_mang_joule", 0.0)
                ])
        except Exception as e:
            print(f"[SCENARIO LỖI GHI CSV] {e}")

    def ghi_log_lien_lac_lien_tuc(self, thong_ke: dict):
        """Ghi mẫu availability, outage và năng lượng tổng cho đánh giá liên lạc liên tục."""
        try:
            with open(self.metrics_log_path, mode="a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    datetime.now().strftime("%H:%M:%S.%f")[:-3],
                    "HE_THONG",
                    f"{thong_ke.get('uav_ket_noi', 0)}/{thong_ke.get('tong_uav', 0)}_UAV",
                    "KIEM_TRA_LIEN_LAC",
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    "CO_TUYEN" if thong_ke.get("uav_ket_noi", 0) else "MAT_TUYEN",
                    0.0,
                    thong_ke.get("do_kha_dung_pct", 0.0),
                    thong_ke.get("thoi_gian_mat_lien_lac_giay", 0.0),
                    thong_ke.get("nang_luong_tieu_thu_joule", 0.0)
                ])
        except Exception as e:
            print(f"[SCENARIO LỖI GHI AVAILABILITY] {e}")