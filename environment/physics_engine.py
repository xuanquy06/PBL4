"""Orbital and wireless communication range + PER calculations using Skyfield."""
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

# Bán kính phủ sóng vô tuyến (Communication Range - Phương án 3, thay thế cơ chế LoS)
MAX_A2G_COMM_RANGE_M = 3500.0         # Bán kính phủ sóng Không-Đất (A2G: UAV <-> Trạm mặt đất: 3.5 km)
MAX_A2A_COMM_RANGE_M = 2800.0         # Bán kính phủ sóng Không-Không (A2A FANET: UAV <-> UAV: 2.8 km)
MAX_SPACE_COMM_RANGE_M = 2500000.0    # Bán kính phủ sóng Vệ tinh LEO (A2S / S2G / ISL: 2500 km)

# Giữ bí danh hằng số tương thích ngược
MAX_A2G_DIRECT_LOS_M = MAX_A2G_COMM_RANGE_M
MAX_A2A_FANET_LOS_M = MAX_A2A_COMM_RANGE_M

# Tọa độ tâm khu vực giám sát (Bán đảo Sơn Trà, Đà Nẵng)
SON_TRA_REF_LAT = 16.105
SON_TRA_REF_LON = 108.250


class PhysicsEngine:
    """
    Động cơ Vật lý mô phỏng Quỹ đạo SGP4, Khoảng cách Không gian 3D (ECEF),
    và Mô hình Kênh truyền Vô tuyến theo Bán kính phủ sóng (Communication Range) + PER theo khoảng cách.
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
        phase_shifts = (-80.0, 0.0, 85.0, -140.0, 140.0)
        for idx, item in enumerate(danh_sach_tle):
            sat_id = item["ma_ve_tinh"]
            sat_obj = EarthSatellite(item["tle_line1"], item["tle_line2"], sat_id, self.ts)
            self.satellites_tle[sat_id] = sat_obj
            shift = phase_shifts[idx % len(phase_shifts)]
            self.satellites_time_offset_sec[sat_id] = self._tim_thoi_diem_bay_qua_son_tra(sat_obj, shift)

    def lay_vi_tri_ve_tinh(self, sat_id: str, bu_thoi_gian_giay: float = 0.0) -> dict:
        """
        Tính toán tọa độ (Vĩ độ, Kinh độ, Độ cao) và vận tốc 3D của vệ tinh bằng SGP4.
        Cho phép truyền `bu_thoi_gian_giay` để tăng tốc thời gian mô phỏng (Time Warp),
        chu kỳ cửa sổ bay qua khu vực giám sát tự động nối tiếp theo chòm vệ tinh LEO.
        """
        if sat_id not in self.satellites_tle:
            raise ValueError(f"Chưa có dữ liệu TLE cho {sat_id}")

        rel_pass_sec = ((float(bu_thoi_gian_giay) + 150.0) % 300.0) - 150.0
        offset_sec = self.satellites_time_offset_sec.get(sat_id, 0.0) + rel_pass_sec
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

    def kiem_tra_ban_kinh_phu_song(self, pos1: dict, pos2: dict) -> tuple[bool, float, float]:
        """
        Kiểm tra điều kiện nằm trong Bán kính phủ sóng vô tuyến (tham số thực nghiệm mô hình):
        - Liên kết Vũ trụ (ISL / A2S / S2G): khoảng cách 3D <= MAX_SPACE_COMM_RANGE_M (2500 km).
        - Liên kết Không-Đất (A2G): khoảng cách 3D <= MAX_A2G_COMM_RANGE_M (3.5 km).
        - Liên kết Không-Không (A2A FANET): khoảng cách 3D <= MAX_A2A_COMM_RANGE_M (2.8 km).
        Trả về: (trong_vung_phu, khoang_cach_m, ban_kinh_toi_da_m).
        """
        alt1 = float(pos1["do_cao"])
        alt2 = float(pos2["do_cao"])
        khoang_cach_m = self.tinh_khoang_cach_3d(pos1, pos2)
        high_alt = max(alt1, alt2)
        low_alt = min(alt1, alt2)

        if high_alt > 80000.0:
            ban_kinh_max_m = MAX_SPACE_COMM_RANGE_M
        elif low_alt < 100.0:
            ban_kinh_max_m = MAX_A2G_COMM_RANGE_M
        else:
            ban_kinh_max_m = MAX_A2A_COMM_RANGE_M

        return (khoang_cach_m <= ban_kinh_max_m), khoang_cach_m, ban_kinh_max_m

    def kiem_tra_tam_nhin_thang(self, pos1: dict, pos2: dict, goc_ngang_toi_thieu_deg: float = 5.0) -> tuple[bool, float]:
        """Hàm tương thích ngược: chuyển sang kiểm tra theo Bán kính phủ sóng (Communication Range)."""
        trong_vung_phu, khoang_cach_m, ban_kinh_max_m = self.kiem_tra_ban_kinh_phu_song(pos1, pos2)
        ty_le_con_lai_deg = max(0.0, 90.0 * (1.0 - min(1.0, khoang_cach_m / max(1.0, ban_kinh_max_m))))
        return trong_vung_phu, round(ty_le_con_lai_deg, 2)

    def danh_gia_chat_luong_kenh_truyen(
        self,
        pos1: dict,
        pos2: dict,
        tan_so_hz: float = 2.4e9,
        cong_suat_phat_dbm: float = 33.0,
        bang_thong_hz: float = 20e6
    ) -> dict:
        """
        Đánh giá toàn diện thông số Lớp Vật lý (PHY Layer) theo Phương án 3:
        - Kiểm tra Bán kính phủ sóng vô tuyến (d <= R_max).
        - Tính Độ trễ lan truyền (Propagation Delay), Suy hao FSPL, SNR, Dung lượng.
        - Tính Xác suất lỗi gói tin (PER) tăng dần mượt mà theo tỷ lệ khoảng cách (d / R_max) và SNR.
        """
        trong_vung_phu, khoang_cach_m, ban_kinh_max_m = self.kiem_tra_ban_kinh_phu_song(pos1, pos2)
        ty_le_khoang_cach = khoang_cach_m / max(1.0, ban_kinh_max_m)

        if not trong_vung_phu:
            return {
                "kha_dung": False,
                "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
                "ban_kinh_phu_song_km": round(ban_kinh_max_m / 1000.0, 2),
                "goc_ngang_do": 0.0,
                "do_tre_ms": 9999.0,
                "fspl_db": 999.0,
                "snr_db": -100.0,
                "dung_luong_mbps": 0.0,
                "per": 1.0
            }

        khoang_cach_hieu_dung_m = max(1.0, khoang_cach_m)
        is_space_link = max(float(pos1["do_cao"]), float(pos2["do_cao"])) > 80000.0
        # Nếu là liên kết vệ tinh (A2S / S2G / ISL), đồng bộ sang băng tần Ku-band (12 GHz) và công suất phát đáp
        effective_freq_hz = 12.0e9 if is_space_link else float(tan_so_hz)
        effective_tx_dbm = max(float(cong_suat_phat_dbm), 43.0) if is_space_link else float(cong_suat_phat_dbm)
        effective_bw_hz = 40.0e6 if is_space_link else float(bang_thong_hz)

        # 1. Độ trễ truyền sóng điện từ (ms) + trễ xử lý chuyển mạch phát đáp vệ tinh (nếu có)
        do_tre_ms = (khoang_cach_hieu_dung_m / SPEED_OF_LIGHT) * 1000.0
        if is_space_link:
            do_tre_ms += 12.0

        # 2. Suy hao không gian tự do chuẩn ITU-R P.525 (Free-Space Path Loss - FSPL)
        fspl_db = (
            20.0 * math.log10(khoang_cach_hieu_dung_m)
            + 20.0 * math.log10(effective_freq_hz)
            + 20.0 * math.log10(4.0 * math.pi / SPEED_OF_LIGHT)
        )

        # Suy hao phụ thực nghiệm trong mô hình mô phỏng (suy hao khí quyển / tán xạ rừng)
        suy_hao_phu_db = 0.0
        if is_space_link and min(float(pos1["do_cao"]), float(pos2["do_cao"])) < 20000.0:
            suy_hao_phu_db = 2.0 + 6.0 * (ty_le_khoang_cach ** 2)
        elif not is_space_link and min(float(pos1["do_cao"]), float(pos2["do_cao"])) < 100.0:
            suy_hao_phu_db = 4.0 + (khoang_cach_hieu_dung_m / 1000.0) * 1.5
        else:
            suy_hao_phu_db = 2.0 * (ty_le_khoang_cach ** 2)

        tong_suy_hao_db = fspl_db + suy_hao_phu_db

        # 3. Quỹ đường truyền (Link Budget): Độ lợi ăng-ten (Tx Gain + Rx Gain)
        tong_antenna_gain_dbi = 54.0 if is_space_link else 16.0
        cong_suat_thu_dbm = effective_tx_dbm + tong_antenna_gain_dbi - tong_suy_hao_db

        # 4. Công suất nhiễu nhiệt nền chuẩn Nyquist-Johnson (Thermal Noise: N = k * T * B) + Noise Figure (5 dB)
        noise_watts = BOLTZMANN_CONST * SYSTEM_TEMP_K * effective_bw_hz
        noise_dbm = 10.0 * math.log10(noise_watts * 1000.0) + 5.0

        # 5. Tỷ số Tín hiệu trên Nhiễu (SNR)
        snr_db = cong_suat_thu_dbm - noise_dbm
        snr_linear = 10.0 ** (snr_db / 10.0)

        # 6. Dung lượng kênh theo định lý Shannon-Hartley (hệ số suy giảm hiệu suất phổ 0.45 là tham số mô phỏng)
        dung_luong_bps = 0.45 * effective_bw_hz * math.log2(1.0 + max(0.0, snr_linear))
        max_hw_mbps = 65.0 if is_space_link else 54.0
        dung_luong_mbps = min(max_hw_mbps, dung_luong_bps / 1e6)

        # 7. Hàm PER đơn giản hóa (tham số thực nghiệm theo tỷ lệ khoảng cách d/R_max và SNR)
        if snr_db < -2.0:
            per_snr = 1.0
        elif snr_db > 25.0:
            per_snr = 0.0001
        else:
            per_snr = math.exp(-0.32 * (snr_db + 2.0))

        # Thành phần PER tăng theo hàm bậc 3 của tỷ lệ khoảng cách (gần trạm ~0.02%, sát rìa vùng phủ ~18%)
        per_khoang_cach = 0.0002 + 0.18 * (min(1.0, ty_le_khoang_cach) ** 3)
        per = 1.0 - (1.0 - per_snr) * (1.0 - per_khoang_cach)

        return {
            "kha_dung": True,
            "khoang_cach_km": round(khoang_cach_m / 1000.0, 2),
            "ban_kinh_phu_song_km": round(ban_kinh_max_m / 1000.0, 2),
            "goc_ngang_do": round(max(0.0, 90.0 * (1.0 - ty_le_khoang_cach)), 2),
            "do_tre_ms": round(do_tre_ms, 3),
            "fspl_db": round(tong_suy_hao_db, 2),
            "snr_db": round(snr_db, 2),
            "dung_luong_mbps": round(dung_luong_mbps, 2),
            "per": round(min(1.0, max(0.0, per)), 4)
        }
