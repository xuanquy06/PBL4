from pydantic import BaseModel, Field
from typing import Literal, Optional

class KichHoatSuCoRequest(BaseModel):
    """Yêu cầu mô phỏng một điểm nghi cháy rừng."""
    loai_su_co: Literal["CHÁY_RỪNG"] = Field(
        default="CHÁY_RỪNG",
        description="Loại sự cố của ứng dụng: CHÁY_RỪNG"
    )
    vi_do: float = Field(..., ge=-90.0, le=90.0, description="Vĩ độ WGS84 (Độ)")
    kinh_do: float = Field(..., ge=-180.0, le=180.0, description="Kinh độ WGS84 (Độ)")

class DieuKhienThoiGianRequest(BaseModel):
    """Schema điều chỉnh tốc độ đồng hồ mô phỏng (Time Warp) và Tạm dừng"""
    time_scale: Optional[float] = Field(
        default=None, ge=0.1, le=50.0, description="Hệ số nhân thời gian (1x -> 50x)"
    )
    tam_dung: Optional[bool] = Field(
        default=None, description="True để tạm dừng đồng hồ vật lý, False để tiếp tục"
    )

class CapNhatPinNodeRequest(BaseModel):
    """Schema cho phép người dùng giả lập kịch bản sập nguồn hoặc sạc đầy pin cho UAV"""
    ma_dinh_danh: str = Field(..., description="Mã định danh của Node (VD: MÁY_BAY_UAV_01)")
    phan_tram_pin_moi: float = Field(..., ge=0.0, le=100.0, description="Mức pin mới (%)")


class ClientCommandRequest(BaseModel):
    """Schema đẩy lệnh điều khiển bất đồng bộ từ Server xuống tiến trình uav_client.py (giao tiếp TCP 2 chiều)"""
    uav_id: str = Field(..., description="Định danh UAV Client (VD: UAV_EXT_01 hoặc MÁY_BAY_UAV_EXT_01)")
    action: Literal["RTB", "RESUME_PATROL", "PING", "DISPATCH_FIRE"] = Field(
        ..., description="Lệnh điều khiển đẩy xuống Client qua kênh TCP"
    )
    vi_do: Optional[float] = Field(default=None, ge=-90.0, le=90.0, description="Vĩ độ mục tiêu (dùng cho DISPATCH_FIRE)")
    kinh_do: Optional[float] = Field(default=None, ge=-180.0, le=180.0, description="Kinh độ mục tiêu (dùng cho DISPATCH_FIRE)")