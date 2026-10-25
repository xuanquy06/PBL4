import asyncio
import csv
import os
import unittest
import tempfile
from unittest.mock import AsyncMock, patch
from fastapi import WebSocketDisconnect

from core.base_agent import AgentState
from core.ground_station_server import GroundStationServer
from core.message_broker import MessageBroker
from agents.uav_agent import LOW_BATTERY_THRESHOLD, UAVAgent, UAVState
from environment.scenario_manager import ScenarioManager
from environment.simulation_loop import SimulationLoop


class FakePhysics:
    def __init__(self, edges):
        self.edges = edges

    def danh_gia_chat_luong_kenh_truyen(self, pos1, pos2, **kwargs):
        node1 = pos1.get("node")
        node2 = pos2.get("node")
        if (node1, node2) in self.edges:
            return self.edges[(node1, node2)]
        if not self.edges:
            kc = self.tinh_khoang_cach_3d(pos1, pos2)
            if kc <= 7000.0:
                return reliable_link(delay=max(1.0, kc / 300000.0 * 1000.0), bandwidth=20.0)
        return {
            "kha_dung": False,
            "do_tre_ms": 9999.0,
            "dung_luong_mbps": 0.0,
            "per": 1.0,
            "snr_db": -100.0
        }

    @staticmethod
    def tinh_khoang_cach_3d(pos1, pos2):
        lat_scale = 111000.0
        lon_scale = 111000.0
        return (
            ((pos1["vi_do"] - pos2["vi_do"]) * lat_scale) ** 2
            + ((pos1["kinh_do"] - pos2["kinh_do"]) * lon_scale) ** 2
            + (pos1["do_cao"] - pos2["do_cao"]) ** 2
        ) ** 0.5


class FakeAgent:
    def __init__(self, node_id, kind, battery=100.0):
        self.ma_dinh_danh = node_id
        self.loai_tac_nhan = kind
        self.trang_thai = AgentState.IDLE
        self.pin_toi_da = 50000.0
        self.pin_hien_tai = self.pin_toi_da * battery / 100
        self.nguong_pin_yeu_pct = 20.0
        self.tai_hang_doi_pct = 0.0
        self.cong_suat_bay_w = 80.0
        self.so_nhiem_vu_da_nhan = 0
        self.hang_doi_thong_diep = asyncio.Queue()
        self.radio_config = {
            "tan_so_hz": 2.4e9,
            "cong_suat_phat_dbm": 35.0,
            "bang_thong_hz": 20e6
        }
        self.toa_do = {"node": node_id, "vi_do": 0.0, "kinh_do": 0.0, "do_cao": 1000.0}

    @property
    def phan_tram_pin(self):
        return self.pin_hien_tai / self.pin_toi_da * 100

    def chuyen_trang_thai(self, trang_thai):
        self.trang_thai = trang_thai


class FakeWebSocket:
    def __init__(self, incoming=None):
        self.sent = []
        self.accepted = False
        self.closed_with = None
        self.incoming = list(incoming or [])

    async def accept(self):
        self.accepted = True

    async def close(self, code=None, reason=None):
        self.closed_with = (code, reason)

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive_json(self):
        if self.incoming:
            return self.incoming.pop(0)
        raise WebSocketDisconnect(code=1000)


def reliable_link(delay=10.0, bandwidth=20.0):
    return {
        "kha_dung": True,
        "do_tre_ms": delay,
        "dung_luong_mbps": bandwidth,
        "per": 0.001,
        "snr_db": 20.0
    }


class NetworkRoutingTests(unittest.TestCase):
    def setUp(self):
        self.physics = FakePhysics({
            ("source", "satellite"): reliable_link(15.0, 18.0),
            ("satellite", "station"): reliable_link(20.0, 35.0),
            ("source", "station"): {
                **reliable_link(),
                "kha_dung": False,
                "per": 1.0
            }
        })
        self.broker = MessageBroker(self.physics, scenario_manager=object())
        self.source = FakeAgent("source", "MÁY_BAY")
        self.satellite = FakeAgent("satellite", "VỆ_TINH_LEO")
        self.station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        self.broker.danh_sach_tac_nhan = {
            node.ma_dinh_danh: node
            for node in (self.source, self.satellite, self.station)
        }

    def test_finds_satellite_relay_route_to_ground_station(self):
        route = self.broker.tim_tuyen_den_tram_mat_dat("source")

        self.assertIsNotNone(route)
        self.assertEqual(route["nodes"], ["source", "satellite", "station"])
        self.assertEqual(route["delay_ms"], 35.0)

    def test_offer_includes_energy_and_route_metrics(self):
        offer = self.broker.tao_bao_gia(
            "satellite",
            "source",
            reliable_link(15.0, 18.0),
            kich_thuoc_kb=250.0
        )

        self.assertIsNotNone(offer)
        self.assertEqual(offer["tuyen"], ["satellite", "station"])
        self.assertGreater(offer["nang_luong_du_kien_joule"], 0.0)
        self.assertEqual(offer["do_tre_du_kien_ms"], 35.0)

    def test_route_rejects_low_battery_relay(self):
        self.satellite.pin_hien_tai = 10.0

        self.assertIsNone(self.broker.tim_tuyen_den_tram_mat_dat("source"))

    def test_route_excludes_already_visited_nodes(self):
        route = self.broker.tim_tuyen_den_tram_mat_dat(
            "satellite",
            loai_tru={"source"}
        )

        self.assertEqual(route["nodes"], ["satellite", "station"])
        self.assertNotIn("source", route["nodes"])

    def test_charging_uav_remains_connected_through_docking_link(self):
        charging_uav = FakeAgent("charging-uav", "MÁY_BAY")
        charging_uav.trang_thai = AgentState.CHARGING
        station = FakeAgent("charging-station", "TRẠM_MẶT_ĐẤT")
        charging_uav.toa_do.update(station.toa_do)
        self.broker.danh_sach_tac_nhan = {
            charging_uav.ma_dinh_danh: charging_uav,
            station.ma_dinh_danh: station
        }

        route = self.broker.tim_tuyen_den_tram_mat_dat(charging_uav.ma_dinh_danh)

        self.assertIsNotNone(route)
        self.assertEqual(route["nodes"], ["charging-uav", "charging-station"])


class UavEnergyTests(unittest.IsolatedAsyncioTestCase):
    async def test_ground_station_tracks_active_uavs_and_broadcasts_commands(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-client",
            waypoints=[]
        )
        server = GroundStationServer(broker)
        websocket = FakeWebSocket()

        self.assertTrue(await server.connect(uav.ma_dinh_danh, websocket))
        delivered = await server.broadcast_command(
            {"loai_tin": "CFP_GOI_THAU"},
            {uav.ma_dinh_danh}
        )

        self.assertTrue(websocket.accepted)
        self.assertEqual(delivered, {uav.ma_dinh_danh})
        self.assertEqual(websocket.sent[0]["type"], "COMMAND")
        self.assertEqual(websocket.sent[0]["data"]["loai_tin"], "CFP_GOI_THAU")
        await server.disconnect(uav.ma_dinh_danh, websocket)
        self.assertNotIn(uav.ma_dinh_danh, server.clients)

    async def test_ground_station_accepts_relayed_telemetry_only_from_valid_relay(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        source = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="source",
            vi_do=0.0,
            kinh_do=0.1,
            waypoints=[]
        )
        relay = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="relay",
            vi_do=0.0,
            kinh_do=0.06,
            waypoints=[]
        )
        relay.evaluate_communication_link(0.0, 0.0)
        source.evaluate_communication_link(0.0, 0.0)
        telemetry = source._telemetry_payload()
        websocket = FakeWebSocket([{
            "type": "RELAY",
            "source_id": source.ma_dinh_danh,
            "relay_target_id": relay.ma_dinh_danh,
            "payload": telemetry
        }])
        server = GroundStationServer(broker)

        await server.receive_telemetry(relay.ma_dinh_danh, websocket)

        self.assertEqual(
            server.latest_telemetry[source.ma_dinh_danh]["relay_target_id"],
            relay.ma_dinh_danh
        )
        self.assertEqual(
            server.latest_telemetry[source.ma_dinh_danh]["current_link"],
            "FANET_RELAY"
        )

    async def test_communication_link_selects_ground_fanet_and_leo_fallback(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0, "do_cao": 25.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-source",
            vi_do=0.0,
            kinh_do=0.1,
            do_cao=1000.0,
            waypoints=[]
        )
        relay_uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-relay",
            vi_do=0.0,
            kinh_do=0.06,
            do_cao=1000.0,
            waypoints=[]
        )
        relay_uav.evaluate_communication_link(0.0, 0.0)

        self.assertEqual(
            uav.evaluate_communication_link(0.0, 0.0),
            "FANET_RELAY"
        )
        self.assertEqual(relay_uav.uav_state, UAVState.RELAYING)
        self.assertEqual(uav.xuat_ho_so_trang_thai()["current_link"], "FANET_RELAY")

        uav.toa_do.update({"vi_do": 0.0, "kinh_do": 0.01})
        self.assertEqual(
            uav.evaluate_communication_link(0.0, 0.0),
            "GROUND_LINK"
        )
        self.assertEqual(relay_uav.uav_state, UAVState.PATROLLING)

        uav.toa_do.update({"vi_do": 0.0, "kinh_do": 0.1})
        relay_uav.chuyen_trang_thai(AgentState.OFFLINE)
        self.assertEqual(
            uav.evaluate_communication_link(0.0, 0.0),
            "LEO_SATELLITE"
        )
        self.assertFalse(uav.direct_los)
        self.assertTrue(uav.relay_required)
        self.assertIsNone(uav.relay_target_id)

    async def test_los_gap_sends_telemetry_via_relay_socket(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        source = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="source",
            vi_do=0.0,
            kinh_do=0.1,
            do_cao=1000.0,
            ws_server_url="ws://localhost:8016/ws/uav",
            waypoints=[]
        )
        relay = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="relay",
            vi_do=0.0,
            kinh_do=0.06,
            do_cao=1000.0,
            ws_server_url="ws://localhost:8016/ws/uav",
            waypoints=[]
        )
        relay.evaluate_communication_link(0.0, 0.0)
        source.evaluate_communication_link(0.0, 0.0)
        relay._websocket = object()

        with (
            patch.object(relay, "_ensure_websocket_connection", new_callable=AsyncMock, return_value=True),
            patch.object(relay, "_send_websocket_json", new_callable=AsyncMock, return_value=True) as send
        ):
            await source._gui_telemetry_websocket()

        send.assert_awaited_once()
        packet = send.await_args.args[1]
        self.assertEqual(packet["type"], "RELAY")
        self.assertEqual(packet["source_id"], "source")
        self.assertEqual(packet["relay_target_id"], "relay")
        self.assertEqual(packet["payload"]["current_link"], "FANET_RELAY")

    async def test_satellite_fallback_waits_two_seconds_before_uplink(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="satellite-uav",
            ws_server_url="ws://localhost:8016/ws/uav",
            waypoints=[]
        )
        uav._pending_satellite_payload = uav._telemetry_payload()
        uav._satellite_websocket = object()

        with (
            patch.object(uav, "_ensure_websocket_connection", new_callable=AsyncMock, return_value=True),
            patch.object(uav, "_send_websocket_json", new_callable=AsyncMock, return_value=True) as send,
            patch("agents.uav_agent.asyncio.sleep", new_callable=AsyncMock) as sleep
        ):
            await uav._gui_qua_ve_tinh_sau_do_tre()

        sleep.assert_awaited_once_with(2.0)
        self.assertEqual(send.await_args.args[1]["type"], "SATELLITE_UPLINK")
        self.assertEqual(send.await_args.args[1]["id"], uav.ma_dinh_danh)

    async def test_standby_uav_charges_at_station_and_reports_fsm_state(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0, "do_cao": 25.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-charging",
            vi_do=0.0,
            kinh_do=0.0,
            do_cao=25.0,
            pin_khoi_tao_pct=50.0,
            waypoints=[]
        )
        uav.chuyen_trang_thai(AgentState.IDLE)

        await uav.update(10.0)
        profile = uav.xuat_ho_so_trang_thai()

        self.assertEqual(uav.uav_state, UAVState.STANDBY)
        self.assertEqual(uav.phan_tram_pin, 56.0)
        self.assertEqual(profile["state"], "STANDBY")
        self.assertEqual(profile["current_link"], "GROUND_LINK")

    async def test_uav_status_payload_includes_live_popup_fields_and_optional_target(self):
        broker = MessageBroker(FakePhysics({}), scenario_manager=object())
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav",
            vi_do=16.12,
            kinh_do=108.26,
            do_cao=120.0,
            cong_suat_bay_w=80.0,
            pin_khoi_tao_pct=65.0,
            waypoints=[]
        )
        uav.toa_do["van_toc_m_s"] = 12.5

        profile = uav.xuat_ho_so_trang_thai()

        self.assertEqual(
            (profile["id"], profile["state"], profile["battery"]),
            ("uav", "PATROLLING", 65.0)
        )
        self.assertEqual(
            (profile["lat"], profile["lng"], profile["speed"]),
            (16.12, 108.26, 12.5)
        )
        self.assertNotIn("target_lat", profile)
        self.assertNotIn("target_lng", profile)

        uav.toa_do_muc_tieu = {"vi_do": 16.13, "kinh_do": 108.27}
        profile = uav.xuat_ho_so_trang_thai()

        self.assertEqual((profile["target_lat"], profile["target_lng"]), (16.13, 108.27))

    async def test_uav_returns_before_long_station_route_exceeds_reserve(self):
        physics = FakePhysics({})
        broker = MessageBroker(physics, scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0, "do_cao": 0.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav",
            vi_do=0.0,
            kinh_do=0.1,
            do_cao=1000.0,
            waypoints=[]
        )
        uav.pin_hien_tai = 25000.0

        await uav.cap_nhat_vat_ly(1.0)

        self.assertEqual(uav.trang_thai, AgentState.RETURNING)
        self.assertEqual(uav.toa_do_tram_sac["kinh_do"], station.toa_do["kinh_do"])

    async def test_low_battery_uav_returns_to_station_and_charges(self):
        physics = FakePhysics({})
        broker = MessageBroker(physics, scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 0.0, "kinh_do": 0.0, "do_cao": 0.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav",
            vi_do=0.0,
            kinh_do=0.001,
            do_cao=1000.0,
            waypoints=[]
        )
        uav.pin_hien_tai = 20000.0

        await uav.cap_nhat_vat_ly(1.0)

        self.assertEqual(uav.trang_thai, AgentState.RETURNING)
        self.assertLess(uav.pin_hien_tai, 20000.0)

        uav.pin_hien_tai = uav.pin_toi_da - 100.0
        uav.chuyen_trang_thai(AgentState.CHARGING)
        await uav.cap_nhat_vat_ly(1.0)

        self.assertEqual(uav.pin_hien_tai, uav.pin_toi_da)
        self.assertEqual(uav.trang_thai, AgentState.PATROLLING)

    async def test_low_battery_uav_broadcasts_handover_only_inside_its_zone(self):
        physics = FakePhysics({})
        broker = MessageBroker(physics, scenario_manager=object())
        station = FakeAgent("station", "TRẠM_MẶT_ĐẤT")
        station.toa_do.update({"vi_do": 16.12, "kinh_do": 108.262, "do_cao": 25.0})
        broker.danh_sach_tac_nhan[station.ma_dinh_danh] = station
        uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-zone-1",
            vi_do=16.12,
            kinh_do=108.26,
            do_cao=1000.0,
            zone_id="ZONE_01",
            waypoints=[{
                "vi_do": 16.121,
                "kinh_do": 108.261,
                "do_cao": 1000.0,
                "van_toc_m_s": 40.0
            }]
        )
        same_zone_uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-zone-1-peer",
            vi_do=16.12,
            kinh_do=108.26,
            do_cao=1000.0,
            zone_id="ZONE_01",
            waypoints=[]
        )
        other_zone_uav = UAVAgent(
            bo_dinh_tuyen=broker,
            ma_dinh_danh="uav-zone-2",
            vi_do=16.12,
            kinh_do=108.26,
            do_cao=1000.0,
            zone_id="ZONE_02",
            waypoints=[]
        )
        uav.pin_hien_tai = uav.pin_toi_da * (LOW_BATTERY_THRESHOLD - 1) / 100

        with patch.object(
            broker,
            "gui_don_huong",
            new_callable=AsyncMock,
            return_value=True
        ) as send_packet:
            await uav.cap_nhat_vat_ly(1.0)
            await uav.cap_nhat_vat_ly(1.0)

        self.assertEqual(uav.trang_thai, AgentState.LOW_BATTERY_RTB)
        self.assertEqual(uav.nguong_pin_yeu_pct, LOW_BATTERY_THRESHOLD)
        self.assertEqual(uav.xuat_ho_so_trang_thai()["state"], "RETURNING")
        send_packet.assert_awaited_once()
        self.assertEqual(
            send_packet.await_args.args[:2],
            (uav.ma_dinh_danh, same_zone_uav.ma_dinh_danh)
        )
        self.assertEqual(
            send_packet.await_args.args[2]["loai_tin"],
            "YEU_CAU_BAN_GIAO_VUNG"
        )
        simulation = SimulationLoop.__new__(SimulationLoop)
        simulation.physics = physics
        simulation.broker = broker
        simulation._ban_giao_vung_uav = {}

        await simulation._lap_lich_ban_giao_vung([uav, same_zone_uav, other_zone_uav])

        self.assertIn(uav.ma_dinh_danh, same_zone_uav._vung_tuan_tra_phu)
        self.assertNotIn(uav.ma_dinh_danh, other_zone_uav._vung_tuan_tra_phu)


class ScenarioDeploymentTests(unittest.TestCase):
    @staticmethod
    def _point_in_ring(latitude, longitude, ring):
        inside = False
        for index, current in enumerate(ring):
            previous = ring[index - 1]
            x1, y1 = current
            x2, y2 = previous
            cross = (longitude - x1) * (y2 - y1) - (latitude - y1) * (x2 - x1)
            on_segment = (
                abs(cross) <= 1e-10
                and min(x1, x2) - 1e-10 <= longitude <= max(x1, x2) + 1e-10
                and min(y1, y2) - 1e-10 <= latitude <= max(y1, y2) + 1e-10
            )
            if on_segment:
                return True
            crosses_ray = (y1 > latitude) != (y2 > latitude)
            if crosses_ray and longitude < (x2 - x1) * (latitude - y1) / (y2 - y1) + x1:
                inside = not inside
        return inside

    @classmethod
    def _point_in_zone(cls, latitude, longitude, zone):
        for polygon in zone["geometry"]["coordinates"]:
            if not polygon or not cls._point_in_ring(latitude, longitude, polygon[0]):
                continue
            if not any(
                cls._point_in_ring(latitude, longitude, hole)
                for hole in polygon[1:]
            ):
                return True
        return False

    def test_default_scenario_deploys_21_uavs_across_seven_son_tra_zones(self):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        scenario = ScenarioManager(data_dir=os.path.join(project_root, "data"))
        nodes, waypoints = scenario.tai_hoac_sinh_kich_ban_mang()
        uavs = [node for node in nodes if node["loai_tac_nhan"] == "MÁY_BAY"]
        stations = [node for node in nodes if node["loai_tac_nhan"] == "TRẠM_MẶT_ĐẤT"]
        zones = scenario.patrol_zones["features"]

        self.assertEqual(len(uavs), 21)
        self.assertEqual(len(stations), 1)
        self.assertEqual(stations[0]["ma_dinh_danh"], "TRẠM_KIỂM_LÂM_SƠN_TRÀ")
        self.assertEqual((stations[0]["vi_do"], stations[0]["kinh_do"]), (16.105, 108.250))
        self.assertTrue(all(node["ma_dinh_danh"] in waypoints for node in uavs))
        self.assertEqual(len(zones), 7)
        self.assertEqual(
            scenario.ke_hoach_bo_tri_ban_dau["phan_bo_theo_tram"],
            {"TRẠM_KIỂM_LÂM_SƠN_TRÀ": 21}
        )
        self.assertEqual(scenario.ke_hoach_bo_tri_ban_dau["so_vung_geofence"], 7)
        self.assertEqual(
            [len(uavs) for uavs in scenario.ke_hoach_bo_tri_ban_dau["uav_theo_vung"].values()],
            [3] * 7
        )
        self.assertEqual(len(scenario.ke_hoach_bo_tri_ban_dau["tram_cua_uav"]), 21)

        for zone_index, zone in enumerate(zones):
            longitude, latitude = zone["properties"]["patrol_center"]
            self.assertTrue(self._point_in_zone(latitude, longitude, zone))
            zone_uavs = uavs[zone_index * 3:zone_index * 3 + 3]
            self.assertEqual({uav["zone_id"] for uav in zone_uavs}, {zone["properties"]["zone_id"]})
            battery_ranges = ((95, 100), (60, 70), (30, 35))
            for uav, (minimum, maximum) in zip(zone_uavs, battery_ranges):
                self.assertGreaterEqual(uav["pin_khoi_tao_pct"], minimum)
                self.assertLessEqual(uav["pin_khoi_tao_pct"], maximum)
            for polygon in zone["geometry"]["coordinates"]:
                for ring in polygon:
                    self.assertEqual(ring[0], ring[-1])
            for uav_index in range(zone_index * 3 + 1, zone_index * 3 + 4):
                route = waypoints[f"MÁY_BAY_UAV_{uav_index:02d}"]
                self.assertEqual(len(route), 6)
                for point in route:
                    self.assertTrue(
                        self._point_in_zone(point["vi_do"], point["kinh_do"], zone),
                        f"{zone['properties']['zone_id']} contains every waypoint for UAV {uav_index}"
                    )

        for index in range(6):
            west_zone = zones[index]
            east_zone = zones[index + 1]
            seam = west_zone["properties"]["east_boundary_lng"]
            self.assertEqual(seam, east_zone["properties"]["west_boundary_lng"])

            def seam_vertices(zone):
                vertices = set()
                for polygon in zone["geometry"]["coordinates"]:
                    for ring in polygon:
                        vertices.update(
                            (longitude, latitude)
                            for longitude, latitude, *_ in ring
                            if longitude == seam
                        )
                return vertices

            west_vertices = seam_vertices(west_zone)
            east_vertices = seam_vertices(east_zone)
            self.assertTrue(west_vertices)
            self.assertEqual(west_vertices, east_vertices)

    def test_default_forest_scenario_generates_full_fleet(self):
        with tempfile.TemporaryDirectory() as data_dir:
            scenario = ScenarioManager(data_dir=data_dir)
            nodes, waypoints = scenario.tai_hoac_sinh_kich_ban_mang()

        self.assertEqual(
            sum(node["loai_tac_nhan"] == "MÁY_BAY" for node in nodes),
            21
        )
        self.assertEqual(
            sum(node["loai_tac_nhan"] == "TRẠM_MẶT_ĐẤT" for node in nodes),
            1
        )
        self.assertEqual(len(waypoints), 21)
        self.assertTrue(all(len(route) == 6 for route in waypoints.values()))

    def test_stale_patrol_routes_outside_zone_are_regenerated(self):
        with tempfile.TemporaryDirectory() as data_dir:
            scenario = ScenarioManager(data_dir=data_dir)
            scenario.tai_hoac_sinh_kich_ban_mang()
            with open(scenario.waypoints_csv_path, mode="r", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
            rows[0]["vi_do"] = "0"
            with open(scenario.waypoints_csv_path, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            refreshed = ScenarioManager(data_dir=data_dir)
            _, routes = refreshed.tai_hoac_sinh_kich_ban_mang()

        first_uav = "MÁY_BAY_UAV_01"
        self.assertTrue(
            all(
                self._point_in_zone(
                    point["vi_do"],
                    point["kinh_do"],
                    refreshed.patrol_zones["features"][0]
                )
                for point in routes[first_uav]
            )
        )

    def test_cnp_bid_or_refusal_uses_flight_energy_not_geographic_radius(self):
        class FakeCnpBroker:
            def __init__(self):
                self.physics = FakePhysics({})
                self.sent_messages = []
                self.tao_bao_gia = lambda *args: {
                    "chi_phi": 10.0,
                    "tuyen": ["TRẠM_KIỂM_LÂM_SƠN_TRÀ"],
                    "nang_luong_du_kien_joule": 5.0,
                    "do_tre_du_kien_ms": 1.0,
                    "dung_luong_du_kien_mbps": 20.0
                }

            def dang_ky_tac_nhan(self, agent):
                return None

            async def gui_don_huong(self, sender, receiver, message):
                self.sent_messages.append(message)

            async def gui_log_ra_web(self, level, message):
                return None

        async def run_case(battery_joule):
            broker = FakeCnpBroker()
            uav = UAVAgent(
                bo_dinh_tuyen=broker,
                ma_dinh_danh="MÁY_BAY_UAV_01",
                vi_do=16.1,
                kinh_do=108.2,
                do_cao=1200.0,
                pin_max_joule=50000.0,
                cong_suat_bay_w=80.0
            )
            uav.pin_hien_tai = battery_joule
            uav.chuyen_trang_thai(AgentState.PATROLLING)
            await uav._tra_bao_gia(
                {
                    "loai_tin": "CFP_GOI_THAU",
                    "ma_phien_dau_gia": "session",
                    "toa_do_su_co": {"vi_do": 16.24, "kinh_do": 108.2}
                },
                "MÁY_BAY_UAV_02"
            )
            return broker.sent_messages

        loop = asyncio.new_event_loop()
        try:
            proposals = loop.run_until_complete(run_case(50000.0))
            refusals = loop.run_until_complete(run_case(20000.0))
        finally:
            loop.close()

        distance_km = FakePhysics.tinh_khoang_cach_3d(
            {"vi_do": 16.1, "kinh_do": 108.2, "do_cao": 1200.0},
            {"vi_do": 16.24, "kinh_do": 108.2, "do_cao": 1200.0}
        ) / 1000.0
        self.assertGreater(distance_km, 15.5)
        self.assertEqual(proposals[0]["loai_tin"], "PROPOSE_BAO_GIA")
        self.assertEqual(refusals[0]["loai_tin"], "REFUSE_BAO_GIA")


class MissionDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fire_mission_farther_than_155_km_uses_energy_not_radius(self):
        simulation = SimulationLoop.__new__(SimulationLoop)
        simulation.physics = FakePhysics({})
        uav = FakeAgent("MÁY_BAY_UAV_01", "MÁY_BAY", battery=100.0)
        uav.chuyen_trang_thai(AgentState.PATROLLING)
        uav.toa_do.update({
            "vi_do": 16.1,
            "kinh_do": 108.2,
            "do_cao": 1200.0,
            "van_toc_m_s": 45.0
        })

        class FakeBroker:
            danh_sach_tac_nhan = {uav.ma_dinh_danh: uav}

            async def gui_log_ra_web(self, level, message):
                return None

        simulation.broker = FakeBroker()
        simulation._so_nhiem_vu_dieu_phoi = 0
        simulation._chi_so_lien_lac = {"nhiem_vu_da_dieu_phoi": 0}
        target_latitude = 16.24
        distance_km = FakePhysics.tinh_khoang_cach_3d(
            uav.toa_do,
            {"vi_do": target_latitude, "kinh_do": 108.2, "do_cao": 0.0}
        ) / 1000.0

        result = await simulation.kich_hoat_su_co_khan_cap(
            "CHÁY_RỪNG", target_latitude, 108.2
        )

        self.assertGreater(distance_km, 15.5)
        self.assertTrue(result["thanh_cong"])
        self.assertEqual(result["uav_dieu_dong"], uav.ma_dinh_danh)

    async def test_low_energy_mission_reports_nearest_uav_and_required_reserve(self):
        simulation = SimulationLoop.__new__(SimulationLoop)
        simulation.physics = FakePhysics({})
        nearest_uav = FakeAgent("MÁY_BAY_UAV_01", "MÁY_BAY", battery=20.0)
        nearest_uav.trang_thai = AgentState.PATROLLING
        nearest_uav.toa_do.update({
            "vi_do": 16.0748,
            "kinh_do": 108.152,
            "do_cao": 1200.0,
            "van_toc_m_s": 45.0
        })
        simulation.broker = type(
            "FakeBroker",
            (),
            {"danh_sach_tac_nhan": {nearest_uav.ma_dinh_danh: nearest_uav}}
        )()

        result = await simulation.kich_hoat_su_co_khan_cap(
            "CHÁY_RỪNG",
            16.8508,
            103.7948
        )

        self.assertFalse(result["thanh_cong"])
        self.assertEqual(result["uav_gan_nhat"], nearest_uav.ma_dinh_danh)
        self.assertGreater(result["khoang_cach_gan_nhat_km"], 400.0)
        self.assertIn("dự trữ", result["thong_bao"])


class FleetAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_fleet_keeps_station_routes_during_long_patrol_and_charging(self):
        simulation = SimulationLoop()
        simulation.khoi_tao_kich_ban()
        uavs = [
            agent for agent in simulation.broker.danh_sach_tac_nhan.values()
            if agent.loai_tac_nhan == "MÁY_BAY"
        ]

        for _ in range(1200):
            await asyncio.gather(*(uav.cap_nhat_vat_ly(1.0) for uav in uavs))

        disconnected = [
            uav.ma_dinh_danh for uav in uavs
            if simulation.broker.tim_tuyen_den_tram_mat_dat(uav.ma_dinh_danh) is None
        ]
        self.assertEqual(disconnected, [])


if __name__ == "__main__":
    unittest.main()
