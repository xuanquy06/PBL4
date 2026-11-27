"""
SAGSINS Client Fleet Manager (Máy 2 - 100% Python Standard Library, Zero dependencies):
- Cung cấp giao diện Web điều khiển phía Client tại http://127.0.0.1:8080/
- Quản lý và khởi chạy tối đa 35+ tiến trình Hệ điều hành `uav_client.py` thật (mỗi UAV = 1 OS PID riêng biệt,
  mở kết nối TCP :9000 và luồng UDP :9001 thật tới Máy 1 Server qua loopback hoặc mạng LAN).
"""
from __future__ import annotations

import argparse
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import TextIOWrapper
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Optional
from urllib.parse import unquote, urlparse

CLIENT_DIR = Path(__file__).resolve().parent
UAV_SCRIPT = CLIENT_DIR / "uav_client.py"
UI_HTML_PATH = CLIENT_DIR / "client_ui.html"

if sys.platform == "win32":
    for _s in (sys.stdout, sys.stderr):
        if isinstance(_s, TextIOWrapper):
            _s.reconfigure(encoding="utf-8", errors="replace")


def detect_lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return str(s.getsockname()[0])
    except OSError:
        return "127.0.0.1"
    finally:
        try:
            s.close()
        except OSError:
            pass


def _build_default_slots() -> list[dict[str, str]]:
    slots: list[dict[str, str]] = []
    for i in range(1, 36):
        short_id = f"UAV_{i:02d}"
        if i <= 28:
            z_num = ((i - 1) // 4) + 1
            role = "PATROL"
        else:
            z_num = i - 28
            role = "STANDBY"
        slots.append({
            "short_id": short_id,
            "uav_id": f"MÁY_BAY_{short_id}",
            "zone_id": f"ZONE_{z_num:02d}",
            "role": role,
        })
    return slots


class ManagedUAVProcess:
    def __init__(self, short_id: str, zone_id: str, role: str):
        self.short_id = short_id
        self.uav_id = f"MÁY_BAY_{short_id}"
        self.zone_id = zone_id
        self.role = role
        self.proc: Optional[subprocess.Popen[str]] = None
        self.pid: Optional[int] = None
        self.alive = False
        self.status: dict[str, Any] = {
            "uav_id": self.uav_id,
            "short_id": short_id,
            "pid": None,
            "ip": "-",
            "zone_id": zone_id,
            "role": role,
            "state": "CHƯA_BẬT",
            "lat": 0.0,
            "lon": 0.0,
            "alt": 0.0,
            "speed": 0.0,
            "battery": 100.0 if role == "STANDBY" else 0.0,
            "rf_link": "-",
            "rf_hops": [],
            "rf_delay_ms": 0.0,
            "rf_per": 0.0,
            "rf_snr_db": 0.0,
            "distance_m": 0.0,
            "tcp_tx": 0,
            "tcp_ack": 0,
            "udp_tx": 0,
            "udp_muted": False,
            "cmd_rx": 0,
            "disconnected": True,
        }
        self.logs: deque[str] = deque(maxlen=80)
        self.io_lock = threading.Lock()


class FleetProcessManager:
    def __init__(self, server_host: str = "127.0.0.1", tcp_port: int = 9000, udp_port: int = 9001):
        self.server_host = server_host
        self.tcp_port = int(tcp_port)
        self.udp_port = int(udp_port)
        self.lock = threading.RLock()
        self.entries: dict[str, ManagedUAVProcess] = {}
        for slot in _build_default_slots():
            self.entries[slot["short_id"]] = ManagedUAVProcess(slot["short_id"], slot["zone_id"], slot["role"])
        self._launching_all = False

    def _normalize_short_id(self, raw_id: str) -> str:
        s = str(raw_id or "").strip().upper()
        if s.startswith("MÁY_BAY_"):
            s = s[len("MÁY_BAY_"):]
        return s

    def set_config(self, host: Optional[str] = None, tcp_port: Optional[int] = None, udp_port: Optional[int] = None) -> dict:
        with self.lock:
            if host is not None and str(host).strip():
                self.server_host = str(host).strip()
            if tcp_port is not None:
                self.tcp_port = int(tcp_port)
            if udp_port is not None:
                self.udp_port = int(udp_port)
            return {
                "server_host": self.server_host,
                "tcp_port": self.tcp_port,
                "udp_port": self.udp_port,
            }

    def _reader_worker(self, entry: ManagedUAVProcess, proc: subprocess.Popen[str]):
        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                if line.startswith("@@STATUS@@"):
                    try:
                        obj = json.loads(line[len("@@STATUS@@"):])
                        with self.lock:
                            entry.status.update(obj)
                            entry.zone_id = str(obj.get("zone_id") or entry.zone_id)
                    except json.JSONDecodeError:
                        pass
                elif line.startswith("@@LOG@@"):
                    msg = f"[{time.strftime('%H:%M:%S')}] {line[len('@@LOG@@'):]}"
                    with self.lock:
                        entry.logs.append(msg)
                else:
                    msg = f"[{time.strftime('%H:%M:%S')}] {line}"
                    with self.lock:
                        entry.logs.append(msg)
        finally:
            rc = proc.poll()
            with self.lock:
                if entry.proc is proc:
                    entry.alive = False
                    entry.status["disconnected"] = True
                    if entry.status.get("state") != "OFFLINE":
                        entry.status["state"] = "ĐÃ_DỪNG"
                    entry.logs.append(f"[{time.strftime('%H:%M:%S')}] [OS] Tiến trình PID={proc.pid} đã thoát (code={rc}).")

    def start_one(self, raw_id: str, zone_id: Optional[str] = None) -> dict[str, Any]:
        short_id = self._normalize_short_id(raw_id)
        with self.lock:
            entry = self.entries.get(short_id)
            if entry is None:
                z_eff = str(zone_id or "ZONE_03")
                entry = ManagedUAVProcess(short_id, z_eff, "PATROL")
                self.entries[short_id] = entry
            if entry.proc is not None and entry.proc.poll() is None:
                return {"ok": True, "already_running": True, "short_id": short_id, "pid": entry.pid}

            cmd = [
                sys.executable,
                "-u",
                str(UAV_SCRIPT),
                "--host",
                self.server_host,
                "--tcp-port",
                str(self.tcp_port),
                "--udp-port",
                str(self.udp_port),
                "--id",
                short_id,
                "--managed",
            ]
            if zone_id and "EXT" in short_id:
                cmd.extend(["--zone", str(zone_id)])

            creation_flags = 0
            if sys.platform == "win32" and hasattr(subprocess, "CREATE_NO_WINDOW"):
                creation_flags = subprocess.CREATE_NO_WINDOW

            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creation_flags,
            )
            entry.proc = proc
            entry.pid = proc.pid
            entry.alive = True
            entry.status["pid"] = proc.pid
            entry.status["disconnected"] = False
            entry.status["udp_muted"] = False
            entry.status["state"] = "ĐANG_KẾT_NỐI"
            entry.logs.append(f"[{time.strftime('%H:%M:%S')}] [OS] Khởi tạo tiến trình uav_client.py PID={proc.pid}.")

            t = threading.Thread(target=self._reader_worker, args=(entry, proc), name=f"Reader-{short_id}", daemon=True)
            t.start()
            return {"ok": True, "short_id": short_id, "pid": proc.pid}

    def restart_one(self, raw_id: str) -> dict[str, Any]:
        """Dừng lịch sự (LEAVE) hoặc Force-kill tiến trình cũ rồi khởi chạy lại PID mới."""
        short_id = self._normalize_short_id(raw_id)
        zone_id: Optional[str] = None
        proc: Optional[subprocess.Popen[str]] = None
        entry: Optional[ManagedUAVProcess] = None
        with self.lock:
            entry = self.entries.get(short_id)
            if entry is not None:
                zone_id = entry.zone_id
                if entry.proc is not None and entry.proc.poll() is None:
                    proc = entry.proc
        if proc is not None and entry is not None:
            try:
                with entry.io_lock:
                    if proc.stdin is not None:
                        proc.stdin.write("q\n")
                        proc.stdin.flush()
            except OSError:
                pass
            try:
                proc.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                    proc.wait(timeout=1.5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        res = self.start_one(short_id, zone_id=zone_id)
        return {**res, "restarted": True}

    def start_batch(self, ids: list[str], delay_ms: int = 60):
        def _run():
            self._launching_all = True
            try:
                for sid in ids:
                    self.start_one(sid)
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
            finally:
                self._launching_all = False

        threading.Thread(target=_run, name="BatchLauncher", daemon=True).start()
        return {"ok": True, "queued": len(ids)}

    def start_all_35(self) -> dict[str, Any]:
        ids = [f"UAV_{i:02d}" for i in range(1, 36)]
        return self.start_batch(ids, delay_ms=55)

    def start_zone(self, zone_id: str) -> dict[str, Any]:
        z_str = str(zone_id).upper()
        with self.lock:
            ids = [e.short_id for e in self.entries.values() if e.zone_id == z_str]
        return self.start_batch(ids, delay_ms=55)

    def send_command(self, raw_id: str, cmd_char: str) -> dict[str, Any]:
        short_id = self._normalize_short_id(raw_id)
        with self.lock:
            entry = self.entries.get(short_id)
            if entry is None or entry.proc is None or entry.proc.poll() is not None:
                return {"ok": False, "error": f"Client {short_id} hiện chưa chạy."}
            proc = entry.proc
        try:
            with entry.io_lock:
                if proc.stdin is None:
                    return {"ok": False, "error": "Stdin của tiến trình đã đóng."}
                proc.stdin.write(cmd_char.strip() + "\n")
                proc.stdin.flush()
            return {"ok": True, "short_id": short_id, "cmd": cmd_char.strip()}
        except OSError as exc:
            return {"ok": False, "error": str(exc)}

    def kill_one(self, raw_id: str) -> dict[str, Any]:
        """Ngắt tiến trình đột ngột bằng OS Kill (SIGKILL/TerminateProcess) để test Server TCP EOF."""
        short_id = self._normalize_short_id(raw_id)
        with self.lock:
            entry = self.entries.get(short_id)
            if entry is None or entry.proc is None or entry.proc.poll() is not None:
                return {"ok": False, "error": f"Client {short_id} không chạy."}
            proc = entry.proc
            pid = proc.pid
        proc.kill()
        proc.wait(timeout=2.0)
        with self.lock:
            entry.alive = False
            entry.status["disconnected"] = True
            entry.status["state"] = "OFFLINE"
            entry.logs.append(f"[{time.strftime('%H:%M:%S')}] [OS_KILL] Đã Kill đột ngột PID={pid} (test đứt TCP!).")
        return {"ok": True, "short_id": short_id, "killed_pid": pid}

    def stop_all(self, force: bool = False) -> dict[str, Any]:
        with self.lock:
            running = [e for e in self.entries.values() if e.proc is not None and e.proc.poll() is None]
        stopped = 0
        for e in running:
            proc = e.proc
            if proc is None:
                continue
            if not force:
                try:
                    with e.io_lock:
                        if proc.stdin:
                            proc.stdin.write("q\n")
                            proc.stdin.flush()
                except OSError:
                    pass
            else:
                try:
                    proc.kill()
                except OSError:
                    pass
            stopped += 1

        if not force and running:
            deadline = time.time() + 2.5
            for e in running:
                proc = e.proc
                if proc is None:
                    continue
                rem = max(0.05, deadline - time.time())
                try:
                    proc.wait(timeout=rem)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except OSError:
                        pass
        return {"ok": True, "stopped": stopped, "force": force}

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            rows = []
            running_cnt = 0
            a2g_cnt = 0
            leo_cnt = 0
            tcp_tx_total = 0
            udp_tx_total = 0

            ordered = sorted(self.entries.values(), key=lambda x: x.short_id)
            for e in ordered:
                alive = e.proc is not None and e.proc.poll() is None
                e.alive = alive
                st = dict(e.status)
                st["alive"] = alive
                rows.append(st)
                if alive:
                    running_cnt += 1
                    link = str(st.get("rf_link", ""))
                    if link == "GROUND_LINK":
                        a2g_cnt += 1
                    elif link == "LEO_SATELLITE":
                        leo_cnt += 1
                tcp_tx_total += int(st.get("tcp_tx", 0))
                udp_tx_total += int(st.get("udp_tx", 0))

            return {
                "server_host": self.server_host,
                "tcp_port": self.tcp_port,
                "udp_port": self.udp_port,
                "launching_all": self._launching_all,
                "manager_pid": os.getpid(),
                "total_slots": len(rows),
                "running_count": running_cnt,
                "a2g_count": a2g_cnt,
                "leo_count": leo_cnt,
                "total_tcp_tx": tcp_tx_total,
                "total_udp_tx": udp_tx_total,
                "clients": rows,
            }

    def get_logs(self, raw_id: str) -> list[str]:
        short_id = self._normalize_short_id(raw_id)
        with self.lock:
            e = self.entries.get(short_id)
            return list(e.logs) if e else []


def make_handler(manager: FleetProcessManager):
    class FleetRequestHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any):
            return

        def _send_json(self, code: int, payload: Any):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html_bytes: bytes):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html_bytes)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(html_bytes)

        def _read_json_body(self) -> dict[str, Any]:
            clen = int(self.headers.get("Content-Length", "0") or 0)
            if clen <= 0:
                return {}
            raw = self.rfile.read(clen)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {}

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            if path in ("/", "/index.html"):
                try:
                    self._send_html(UI_HTML_PATH.read_bytes())
                except OSError as exc:
                    self._send_json(500, {"error": str(exc)})
                return
            if path == "/favicon.ico":
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if path == "/api/fleet/status":
                self._send_json(200, manager.snapshot())
                return
            if path.startswith("/api/fleet/logs/"):
                uid = unquote(path[len("/api/fleet/logs/"):])
                self._send_json(200, {"uav_id": uid, "logs": manager.get_logs(uid)})
                return
            self._send_json(404, {"error": "Not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            body = self._read_json_body()
            if path == "/api/fleet/config":
                self._send_json(200, manager.set_config(body.get("server_host"), body.get("tcp_port"), body.get("udp_port")))
                return
            if path == "/api/fleet/start-all":
                self._send_json(200, manager.start_all_35())
                return
            if path == "/api/fleet/start-zone":
                self._send_json(200, manager.start_zone(str(body.get("zone_id") or "ZONE_01")))
                return
            if path == "/api/fleet/start-one":
                self._send_json(200, manager.start_one(str(body.get("uav_id") or "UAV_09"), body.get("zone_id")))
                return
            if path == "/api/fleet/restart-one":
                self._send_json(200, manager.restart_one(str(body.get("uav_id") or "UAV_09")))
                return
            if path == "/api/fleet/command":
                res = manager.send_command(str(body.get("uav_id") or ""), str(body.get("cmd") or ""))
                self._send_json(200 if res.get("ok") else 400, res)
                return
            if path == "/api/fleet/kill":
                res = manager.kill_one(str(body.get("uav_id") or ""))
                self._send_json(200 if res.get("ok") else 400, res)
                return
            if path == "/api/fleet/stop-all":
                self._send_json(200, manager.stop_all(force=bool(body.get("force", False))))
                return
            self._send_json(404, {"error": "Not found"})

    return FleetRequestHandler


def main():
    parser = argparse.ArgumentParser(description="SAGSINS Client Fleet Control Web UI (100% Python stdlib)")
    parser.add_argument("--port", type=int, default=8080, help="Cổng Web UI phía Client (mặc định 8080)")
    parser.add_argument("--server-host", default="127.0.0.1", help="IP Server Máy 1 (mặc định 127.0.0.1)")
    parser.add_argument("--tcp-port", type=int, default=9000, help="Cổng TCP Server (mặc định 9000)")
    parser.add_argument("--udp-port", type=int, default=9001, help="Cổng UDP Server (mặc định 9001)")
    args = parser.parse_args()

    manager = FleetProcessManager(
        server_host=args.server_host,
        tcp_port=args.tcp_port,
        udp_port=args.udp_port,
    )
    server = ThreadingHTTPServer(("0.0.0.0", int(args.port)), make_handler(manager))
    lan_ip = detect_lan_ip()
    print("=" * 72, flush=True)
    print(f"  SAGSINS CLIENT FLEET CONTROL | Manager PID={os.getpid()}", flush=True)
    print(f"  Client Web UI : http://127.0.0.1:{args.port}/ (LAN: http://{lan_ip}:{args.port}/)", flush=True)
    print(f"  Mục tiêu Server: {args.server_host} (TCP :{args.tcp_port} / UDP :{args.udp_port})", flush=True)
    print("=" * 72, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[CLIENT MANAGER] Đang dừng toàn bộ tiến trình UAV con...", flush=True)
        manager.stop_all(force=False)
        server.shutdown()


if __name__ == "__main__":
    main()
