"""Orbital and line-of-sight calculations using Skyfield."""
import math
from datetime import datetime, timezone, timedelta
from typing import cast
import numpy as np
from skyfield.api import load, EarthSatellite, wgs84

# Các hằng số Vật lý & Viễn thông chuẩn quốc tế
SPEED_OF_LIGHT = 299792458.0          # Vận tốc ánh sáng trong chân không (m/s)
EARTH_RADIUS_M = 6371000.0            # Bán kính trung bình Trái Đất (m)
BOLTZMANN_CONST = 1.380649e-23        # Hằng số Boltzmann (J/K)
SYSTEM_TEMP_K = 290.0                 # Nhiệt độ nhiễu hệ thống tiêu chuẩn (K)

# Ngưỡng cự ly truyền dẫn theo phân tầng địa hình SAGSINS Sơn Trà
MAX_A2G_DIRECT_LOS_M = 4200.0         # Cự ly trực xạ Không-Đất (A2G) tối đa trước khi bị núi Sơn Trà che khuất
MAX_A2A_FANET_LOS_M = 7000.0          # Cự ly trực xạ Không-Không (A2A FANET) giữa các UAV ở độ cao tuần tra

# Tọa độ tâm khu vực giám sát (Bán đảo Sơn Trà, Đà Nẵng)
SON_TRA_REF_LAT = 16.105
SON_TRA_REF_LON = 108.250


class PhysicsEngine:
    """
    Động cơ Vật lý mô phỏng Quỹ đạo SGP4, Hình học Không gian 3D (ECEF),
    và Mô hình Kênh truyền Vô tuyến (FSPL, SNR, Shannon Capacity, PER).
    """
    def __init__(self):
        self.ts = load.timescale()
        self.satellites_tle = {}
        self.satellites_time_offset_sec: dict[str, float] = {}
        self._base_utc = datetime.now(timezone.utc)

    def dang_ky_tle_ve_tinh(self, sat_id: str, line1: str, line2: str, time_offset_sec: float = 0.0):
        """Đăng ký đối tượng vệ tinh SGP4 vào hệ thống kèm độ lệch pha thời gian qua vùng giám sát."""
        self.satellites_tle[sat_id] = EarthSatellite(line1, line2, sat_id, self.ts)
        self.satellites_time_offset_sec[sat_id] = float(time_offset_sec)

    def _tim_thoi_diem_bay_qua_son_tra(self, sat: EarthSatellite, phase_shift_sec: float = 0.0) -> float:
        """
        Quét quỹ đạo SGP4 thực tế trong 24 giờ tới để tìm thời điểm vệ tinh bay qua
        vùng trời Đà Nẵng / Sơn Trà với góc ngẩng cao nhất, giúp mô phỏng SAGSINS
        luôn có vệ tinh LEO hoạt động trong khu vực mà vẫn giữ nguyên cơ học quỹ đạo SGP4.
        """
        best_offset = 0.0
        min_dist_sq = float("inf")
        # Bước nhảy thô 120 giây trong 24 giờ (720 điểm)
        for step in range(0, 86400, 120):
            dt_check = self._base_utc + timedelta(seconds=step)
            t_sf = self.ts.from_datetime(dt_check)
            subpoint = wgs84.subpoint(sat.at(t_sf))
            lat = float(cast(float, subpoint.latitude.degrees))
            lon = float(cast(float, subpoint.longitude.degrees))
            if not (math.isfinite(lat) and math.isfinite(lon)):
                continue
            d_lat = lat - SON_TRA_REF_LAT
            d_lon = ((lon - SON_TRA_REF_LON + 180.0) % 360.0) - 180.0
            dist_sq = d_lat * d_lat + d_lon * d_lon
            if dist_sq < min_dist_sq:
                min_dist_sq = dist_sq
                best_offset = float(step)
        return best_offset + phase_shift_sec

    def nap_danh_sach_tle(self, danh_sach_tle: list[dict]):
        """Nhận danh sách TLE từ ScenarioManager, nạp vào bộ giải SGP4 và căn pha qua vùng giám sát."""
        self._base_utc = datetime.now(timezone.utc)
        # Căn lệch pha giữa các vệ tinh (-120s, 0s, +150s...) để tạo chuỗi phủ sóng liên tục S2G & ISL
        phase_shifts = (-90.0, 30.0, 180.0, -210.0, 300.0)
        for idx, item in enumerate(danh_sach_tle):
            sat_id = item["ma_ve_tinh"]
            sat_obj = EarthSatellite(item["tle_line1"], item["tle_line2"], sat_id, self.ts)
            self.satellites_tle[sat_id] = sat_obj
            shift = phase_shifts[idx % len(phase_shifts)]
            self.satellites_time_offset_sec[sat_id] = self._tim_thoi_diem_bay_qua_son_tra(sat_obj, shift)

    def lay_vi_tri_ve_tinh(self, sat_id: str, bu_thoi_gian_giay: float = 0.0) -> dict:
        """
        Tính toán tọa độ (Vĩ độ, Kinh độ, Độ cao) và vận tốc 3D của vệ tinh bằng SGP4.
        Cho phép truyền `bu_thoi_gian_giay` để tăng tốc thời gian mô phỏng (Time Warp).
        """
        if sat_id not in self.satellites_tle:
            raise ValueError(f"Chưa có dữ liệu TLE cho {sat_id}")

        offset_sec = self.satellites_time_offset_sec.get(sat_id, 0.0) + float(bu_thoi_gian_giay)
        dt_now = self._base_utc + timedelta(seconds=offset_sec)
        t_skyfield = self.ts.from_datetime(dt_now)

        sat = self.satellites_tle[sat_id]
        geocentric = sat.at(t_skyfield)
        subpoint = wgs84.subpoint(geocentric)

        velocity_km_s = geocentric.velocity.km_per_s
        vx, vy, vz = float(velocity_km_s[0]), float(velocity_km_s[1]), float(velocity_km_s[2])
        speed_m_s = math.sqrt(vx * vx + vy * vy + vz * vz) * 1000.0

        lat_deg = float(cast(float, subpoint.latitude.degrees))
        lon_deg = float(cast(float, subpoint.longitude.degrees))
        alt_m = float(cast(float, subpoint.elevation.m))

        return {
            "vi_do": round(lat_deg if math.isfinite(lat_deg) else SON_TRA_REF_LAT, 5),
            "kinh_do": round(lon_deg if math.isfinite(lon_deg) else SON_TRA_REF_LON, 5),
            "do_cao": round(alt_m if math.isfinite(alt_m) and alt_m > 100000.0 else 550000.0, 2),
            "van_toc_m_s": round(speed_m_s if math.isfinite(speed_m_s) else 7600.0, 2)
        }

    @staticmethod
    def _geodetic_to_ecef_tuple(lat: float, lon: float, alt: float) -> tuple[float, float, float]:
        """Chuyển đổi nhanh tọa độ WGS84 sang ECEF (X, Y, Z) bằng toán học vô hướng."""
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        sin_lat = math.sin(lat_rad)
        cos_lat = math.cos(lat_rad)

        a = 6378137.0
        e2 = 0.0066943799901413165

        n_val = a / math.sqrt(1.0 - e2 * (sin_lat * sin_lat))
        x = (n_val + alt) * cos_lat * math.cos(lon_rad)
        y = (n_val + alt) * cos_lat * math.sin(lon_rad)
        z = (n_val * (1.0 - e2) + alt) * sin_lat
        return x, y, z

    @staticmethod
    def geodetic_to_ecef(lat: float, lon: float, alt: float) -> np.ndarray:
        """
        Chuyển đổi tọa độ Địa lý WGS84 (Lat, Lon, Alt) sang tọa độ Đề-các Địa tâm ECEF (X, Y, Z).
        Đơn vị đầu ra: mét.
        """
        x, y, z = PhysicsEngine._geodetic_to_ecef_tuple(lat, lon, alt)
        return np.array([x, y, z], dtype=float)

    def tinh_khoang_cach_3d(self, pos1: dict, pos2: dict) -> float:
        """Tính khoảng cách Euclid 3D chính xác (mét) giữa 2 node bất kỳ."""
        x1, y1, z1 = self._geodetic_to_ecef_tuple(pos1["vi_do"], pos1["kinh_do"], pos1["do_cao"])
        x2, y2, z2 = self._geodetic_to_ecef_tuple(pos2["vi_do"], pos2["kinh_do"], pos2["do_cao"])
        dx = x2 - x1
        dy = y2 - y1
        dz = z2 - z1
        return math.sqrt(dx * dx + dy * dy + dz * dz)

    def kiem_tra_tam_nhin_thang(self, pos1: dict, pos2: dict, goc_ngang_toi_thieu_deg: float = 5.0) -> tuple[bool, float]:
        """
        Kiểm tra điều kiện Tầm nhìn thẳng (Line-of-Sight - LoS) và tính Góc ngẩng (Elevation Angle).
        Phân biệt rõ:
        - ISL (Vệ tinh - Vệ tinh): Không bị Trái Đất/tầng khí quyển thấp (< 50km) che khuất.
        - A2S / S2G (Vệ tinh - UAV/Mặt đất): Góc ngẩng >= goc_ngang_toi_thieu_deg (mặc định 5 độ).
        - A2G (UAV - Trạm mặt đất): Giới hạn 4.2 km do địa hình núi Sơn Trà che khuất tầm thấp.
        - A2A (UAV - UAV FANET): Giới hạn 7.0 km ở độ cao tuần tra.
        """
        alt1 = float(pos1["do_cao"])
        alt2 = float(pos2["do_cao"])
        x1, y1, z1 = self._geodetic_to_ecef_tuple(pos1["vi_do"], pos1["kinh_do"], alt1)
        x2, y2, z2 = self._geodetic_to_ecef_tuple(pos2["vi_do"], pos2["kinh_do"], alt2)

        dx = x2 - x1
        dy = y2 - y1
        dz = z2 - z1
        dist_sq = dx * dx + dy * dy + dz * dz
        if dist_sq == 0.0:
            return True, 90.0

        # 1. Liên kết giữa 2 Vệ tinh (ISL - Inter-Satellite Link)
        if alt1 > 80000.0 and alt2 > 80000.0:
            t = -(x1 * dx + y1 * dy + z1 * dz) / dist_sq
            t_clamped = max(0.0, min(1.0, t))
            cx = x1 + t_clamped * dx
            cy = y1 + t_clamped * dy
            cz = z1 + t_clamped * dz
            khoang_cach_toi_tam = math.sqrt(cx * cx + cy * cy + cz * cz)
            co_los = khoang_cach_toi_tam >= (EARTH_RADIUS_M + 50000.0)
            return co_los, 0.0

        # 2. Liên kết giữa Tầng thấp (UAV / Mặt đất) và Tầng cao (Vệ tinh hoặc UAV khác)
        if alt1 <= alt2:
            lx, ly, lz, low_alt = x1, y1, z1, alt1
            hx, hy, hz, high_alt = x2, y2, z2, alt2
        else:
            lx, ly, lz, low_alt = x2, y2, z2, alt2
            hx, hy, hz, high_alt = x1, y1, z1, alt1

        vx, vy, vz = hx - lx, hy - ly, hz - lz
        dist = math.sqrt(dist_sq)
        low_norm = math.sqrt(lx * lx + ly * ly + lz * lz)
        if low_norm <= 0.0:
            return False, -90.0

        sin_el = max(-1.0, min(1.0, (vx * lx + vy * ly + vz * lz) / (dist * low_norm)))
        elevation_deg = math.degrees(math.asin(sin_el))

        # Liên kết Không - Vũ trụ (A2S / S2G)
        if high_alt > 80000.0:
            return (elevation_deg >= goc_ngang_toi_thieu_deg), elevation_deg

        # Liên kết Không - Đất (A2G): Một đầu là Trạm mặt đất (do_cao < 100m)
        if low_alt < 100.0:
            return (dist <= MAX_A2G_DIRECT_LOS_M), elevation_deg

        # Liên kết Nội tầng Không - Không (A2A FANET giữa các UAV)
        return (dist <= MAX_A2A_FANET_LOS_M), elevation_deg

    def danh_gia_chat_luong_kenh_truyen(
        self,
        pos1: dict,
        pos2: dict,
        tan_so_hz: float = 2.4e9,
        cong_suat_phat_dbm: float = 33.0,
        bang_thong_hz: float = 20e6
    ) -> dict:
        """
        Đánh giá toàn diện thông số Lớp Vật lý (PHY Layer) của liên kết vô tuyến:
        - Khoảng cách 3D, Độ trễ lan truyền (Propagation Delay), Suy hao FSPL, SNR, Dung lượng và PER.
        """
        co_los, goc_ngang = self.kiem_tra_tam_nhin_thang(pos1, pos2)
        khoang_cach_m = self.tinh_khoang_cach_3d(pos1, pos2)

        if not co_los or khoang_cach_m <= 0:
            return {
                "kha_dung": False,
                "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
                "goc_ngang_do": round(goc_ngang, 2),
                "do_tre_ms": 9999.0,
                "fspl_db": 999.0,
                "snr_db": -100.0,
                "dung_luong_mbps": 0.0,
                "per": 1.0
            }

        is_space_link = max(float(pos1["do_cao"]), float(pos2["do_cao"])) > 80000.0
        # Nếu là liên kết vệ tinh (A2S / S2G / ISL), đồng bộ sang băng tần Ku-band (12 GHz) và công suất phát đáp
        effective_freq_hz = 12.0e9 if is_space_link else float(tan_so_hz)
        effective_tx_dbm = max(float(cong_suat_phat_dbm), 42.0) if is_space_link else float(cong_suat_phat_dbm)
        effective_bw_hz = 40.0e6 if is_space_link else float(bang_thong_hz)

        # 1. Độ trễ truyền sóng điện từ (ms) + trễ xử lý chuyển mạch phát đáp vệ tinh (nếu có)
        do_tre_ms = (khoang_cach_m / SPEED_OF_LIGHT) * 1000.0
        if is_space_link:
            do_tre_ms += 12.0

        # 2. Suy hao không gian tự do (Free-Space Path Loss - FSPL)
        fspl_db = (
            20.0 * math.log10(khoang_cach_m)
            + 20.0 * math.log10(effective_freq_hz)
            + 20.0 * math.log10(4.0 * math.pi / SPEED_OF_LIGHT)
        )

        # Suy hao khí quyển / tán xạ tán lá rừng
        suy_hao_phu_db = 0.0
        if is_space_link and min(float(pos1["do_cao"]), float(pos2["do_cao"])) < 20000.0:
            sin_el = max(0.087, math.sin(math.radians(max(5.0, goc_ngang))))
            suy_hao_phu_db = 2.2 / sin_el
        elif not is_space_link and min(float(pos1["do_cao"]), float(pos2["do_cao"])) < 100.0:
            # Suy hao đa đường & tán xạ qua sườn rừng Sơn Trà cho liên kết A2G
            suy_hao_phu_db = 6.0 + (khoang_cach_m / 1000.0) * 1.8

        tong_suy_hao_db = fspl_db + suy_hao_phu_db

        # 3. Quỹ đường truyền (Link Budget): Độ lợi ăng-ten (Tx Gain + Rx Gain)
        tong_antenna_gain_dbi = 42.0 if is_space_link else 14.0
        cong_suat_thu_dbm = effective_tx_dbm + tong_antenna_gain_dbi - tong_suy_hao_db

        # 4. Công suất nhiễu nhiệt nền (Thermal Noise: N = k * T * B) + Noise Figure (5 dB)
        noise_watts = BOLTZMANN_CONST * SYSTEM_TEMP_K * effective_bw_hz
        noise_dbm = 10.0 * math.log10(noise_watts * 1000.0) + 5.0

        # 5. Tỷ số Tín hiệu trên Nhiễu (SNR)
        snr_db = cong_suat_thu_dbm - noise_dbm
        snr_linear = 10.0 ** (snr_db / 10.0)

        # 6. Dung lượng kênh truyền thực tế (hiệu suất phổ thực tế = 0.45 * giới hạn Shannon, trần phần cứng)
        dung_luong_bps = 0.45 * effective_bw_hz * math.log2(1.0 + max(0.0, snr_linear))
        max_hw_mbps = 65.0 if is_space_link else 54.0
        dung_luong_mbps = min(max_hw_mbps, dung_luong_bps / 1e6)

        # 7. Tỷ lệ lỗi gói tin (Packet Error Rate - PER)
        if snr_db < -2.0:
            per = 1.0
        elif snr_db > 25.0:
            per = 0.0001
        else:
            per = math.exp(-0.32 * (snr_db + 2.0))

        return {
            "kha_dung": True,
            "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
            "goc_ngang_do": round(goc_ngang, 2),
            "do_tre_ms": round(do_tre_ms, 3),
            "fspl_db": round(tong_suy_hao_db, 2),
            "snr_db": round(snr_db, 2),
            "dung_luong_mbps": round(dung_luong_mbps, 2),
            "per": round(min(1.0, max(0.0, per)), 4)
        }
