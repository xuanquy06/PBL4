import os
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from core.base_agent import AgentState
from agents.uav_agent import UAVAgent, UAVState
from api.schemas import (
    KichHoatSuCoRequest,
    DieuKhienThoiGianRequest,
    CapNhatPinNodeRequest,
    ClientCommandRequest,
)


def tao_bo_dinh_tuyen_api(sim_loop) -> APIRouter:
    """Khởi tạo APIRouter gắn kết trực tiếp với SimulationLoop phía Server."""
    router = APIRouter()

    @router.websocket("/ws/mophong")
    async def ket_noi_websocket(websocket: WebSocket):
        await websocket.accept()
        sim_loop.broker.danh_sach_client_ws.append(websocket)
        try:
            await sim_loop.broker.dong_bo_trang_thai_toan_mang()
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            if websocket in sim_loop.broker.danh_sach_client_ws:
                sim_loop.broker.danh_sach_client_ws.remove(websocket)

    @router.get("/api/init-state")
    async def lay_trang_thai_khoi_tao():
        """Trả trạng thái node đang kết nối, 7 vùng Sơn Trà, Heatmap và chỉ số mạng."""
        return sim_loop.lay_du_lieu_khoi_tao_ban_do()

    @router.get("/api/fleet-blueprint")
    async def lay_cau_hinh_35_slot():
        """Trả bảng phân bổ 35 slot UAV chuẩn (UAV_01..UAV_35) để Client tham chiếu."""
        return {
            "total": len(sim_loop.fleet_blueprint),
            "slots": list(sim_loop.fleet_blueprint.values()),
        }

    @router.get("/api/node-link/{node_id}")
    async def lay_thong_so_kenh_node(node_id: str):
        """Trả thông số kênh truyền 2 tầng (A2G / LEO) và kết nối Client của một Node."""
        agent = sim_loop.broker.lay_tac_nhan(node_id)
        if not agent:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy node {node_id}")
        profile = agent.xuat_ho_so_trang_thai()
        return {
            "id": profile["id"],
            "ip": profile.get("ip", "10.0.0.0"),
            "zone_id": profile.get("zone_id"),
            "role": profile.get("role", "PATROL"),
            "state": profile.get("state"),
            "current_link": profile.get("current_link", "OUT_OF_RANGE"),
            "relay_target_id": profile.get("relay_target_id"),
            "hops": profile.get("tuyen_ve_tram", profile.get("route_to_sink", [])),
            "distance_m": profile.get("distance_to_station_m", 0.0),
            "snr_db": profile.get("snr_db", 0.0),
            "per": profile.get("per", 0.0),
            "delay_ms": profile.get("delay_ms", 0.0),
            "capacity_mbps": profile.get("capacity_mbps", 0.0),
            "client_pid": profile.get("client_pid"),
            "client_addr": profile.get("client_addr"),
        }

    @router.post("/api/su-co")
    async def kich_hoat_su_co(req: KichHoatSuCoRequest):
        """Khởi phát sự cố cháy rừng trên bản đồ điểm nhiệt để các UAV Client xử lý."""
        ket_qua = await sim_loop.kich_hoat_su_co_khan_cap(
            loai_su_co=req.loai_su_co,
            vi_do=req.vi_do,
            kinh_do=req.kinh_do,
        )
        if not ket_qua["thanh_cong"]:
            raise HTTPException(status_code=400, detail=ket_qua["thong_bao"])
        return ket_qua

    @router.post("/api/xoa-chay")
    async def xoa_dam_chay():
        """Xóa toàn bộ đám cháy active_fires và giải phóng UAV khoanh vùng."""
        return await sim_loop.xoa_tat_ca_dam_chay()

    @router.post("/api/dieu-khien-thoi-gian")
    async def dieu_khien_thoi_gian(req: DieuKhienThoiGianRequest):
        """Thay đổi tốc độ đồng hồ mô phỏng hoặc tạm dừng/tiếp tục."""
        return await sim_loop.dieu_khien_thoi_gian(
            time_scale=req.time_scale,
            tam_dung=req.tam_dung,
        )

    @router.post("/api/cap-nhat-pin")
    async def cap_nhat_pin_tac_nhan(req: CapNhatPinNodeRequest):
        """Đẩy thay đổi mức pin xuống UAV Client qua TCP SET_BATTERY."""
        try:
            return await sim_loop.cap_nhat_pin_uav(
                req.ma_dinh_danh,
                req.phan_tram_pin_moi,
            )
        except ValueError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @router.post("/api/client-command")
    async def gui_lenh_client_ngoai(req: ClientCommandRequest):
        """Chủ động đẩy lệnh TCP 2 chiều từ Server xuống tiến trình Client UAV."""
        norm_id = sim_loop._normalize_ext_uav_id(req.uav_id)
        params = {}
        if req.vi_do is not None:
            params["lat"] = req.vi_do
        if req.kinh_do is not None:
            params["lon"] = req.kinh_do
        ok = sim_loop.broker.network_stack.send_command_to_external_client(
            norm_id, req.action, params or None
        )
        if not ok:
            raise HTTPException(
                status_code=404,
                detail=f"Client {norm_id} chưa kết nối TCP hoặc đã ngắt kết nối.",
            )
        if str(req.action or "").upper() == "RTB":
            agent = sim_loop.broker.lay_tac_nhan(norm_id)
            if isinstance(agent, UAVAgent):
                fire_pos = agent._lay_tam_chay_dang_phu_trach()
                with sim_loop.broker.os_kernel.net_state_lock:
                    agent._da_goi_du_bi_thay_ca = False
                    agent.chuyen_trang_thai(AgentState.RETURNING)
                    agent.uav_state = UAVState.RETURNING
                await agent.ban_giao_chay_khi_roi_nhiem_vu(fire_pos)
                await agent._goi_uav_du_bi_thay_ca()
                await sim_loop.broker.dong_bo_trang_thai_toan_mang()
        await sim_loop.broker.gui_log_ra_web(
            "INFO",
            f"[CLIENT_NET] Đã đẩy lệnh TCP {req.action} từ Server xuống Client [{norm_id}]."
        )
        return {"ok": True, "uav_id": norm_id, "action": req.action}

    @router.get("/api/xuat-csv")
    async def tai_file_ket_qua_csv():
        """Tải trực tiếp file CSV nhật ký thực nghiệm QoS."""
        csv_path = sim_loop.scenario.metrics_log_path
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail="Chưa có dữ liệu thực nghiệm CSV.")
        return FileResponse(
            path=csv_path,
            media_type="text/csv",
            filename="sagsins_qos_metrics_log.csv",
        )

    return router
