# Project Context & Architecture Rules (PBL4: MAS - SAGSINS)

## 1. Tech Stack & Core Libraries
- **Backend:** Python 3.x (FastAPI / Flask + WebSockets/REST API, NetworkX for graph routing)
- **Frontend:** HTML5, CSS3, Vanilla JavaScript, Leaflet.js (Map rendering & `Leaflet.heat` for Heatmap)
- **Domain:** Multi-Agent System (MAS) & Space-Air-Ground-Sea Integrated Network (SAGSINS) for forest fire monitoring in Son Tra Peninsula.

## 2. Directory Map (Where to look & put files)
- `agents/` — Autonomous agent logic (`uav_agent.py`, `satellite_agent.py`, `ground_station.py`).
- `core/` — OS Kernel & Networking Stack (`os_kernel.py`, `network_stack.py`, `base_agent.py`, `message_broker.py`, `ground_station_server.py`).
- `environment/` — Simulation world, 7 zones (Z1–Z7), Heatmap grid (`heat_nodes`), and scenario triggers (`scenario_manager.py`, `simulation_loop.py`).
- `templates/` & `static/` — Web UI (`index.html`, `dashboard.html`), Leaflet map layers, OS Kernel Monitor, Packet Sniffer, and simulation controls.

## 3. Data Flow & Architectural Boundaries
- **Simulation Loop:** `scenario_manager` updates environment & filters local observations -> `UAVAgent.update()` decides autonomously (Sense-Decide-Act) -> `message_broker` computes routing and sends 26-byte binary `NetworkPacket` (IPv4, TTL, CRC32) over real OS TCP/UDP sockets -> `os_kernel` enforces Mutex `RLock` (`net_state_lock`), `BoundedPriorityQueue(P0/P1/P2)`, and OS process/thread monitoring -> Backend streams state JSON to Frontend.
- **Strict MAS & Physics Boundaries:**
  - **Local Autonomy:** UAV agents MUST make navigation and target-locking decisions based on local sensor radius (`<= 600m`) and P2P messages within FANET range (`<= 2200m`), NOT via a centralized global controller.
  - **Heatmap Rules:** `Cooling Radius = 600m` & `Cooling Rate = 0.35/s` (non-fire `heat_node` within `<= 600m` of active flying UAVs `PATROLLING`/`RELAYING`/`CONTAINING`/`RESPONDING` — ignoring `OFFLINE`/`STANDBY`/`CHARGING`/`RETURNING` — cools gradually from the UAV center outward at `0.35/s` weighted radially `[1.25x at center .. 0.75x at 600m]`, taking ~1.5–2s to clear `0.68 -> 0.05`) and `Staleness Rate = 0.025/s` (unpatrolled nodes `> 600m` increase by `0.025/s` capped at `risk_score <= 0.68`). Only user-triggered `active_fires` reach `1.0` (red) and MUST NOT auto-cool when a UAV approaches.
  - **Routing & Transport Rules:** Primary E2E routing uses multi-hop FANET (`<= 2200m` links) with per-hop TTL decrement and fallback `[UAV -> LEO_SATELLITE -> GROUND_STATION]`. Critical alerts/handovers use reliable TCP ARQ (up to 3 retransmissions + ACK), while periodic Telemetry/Beacons use UDP.
- **OS & Sync Rules:** Multi-process Server/Client + Multi-threaded TCP/UDP server threads (`SAGSINS-TCP-Server`, `SAGSINS-TCP-Client-*`, `SAGSINS-UDP-Server`); shared network state & packet sniffer ring buffer protected by `threading.RLock` (`net_state_lock`); agent queues use `BoundedPriorityQueue(P0 -> P1 -> P2)`.

## 4. Coding Conventions & Anti-Patterns
- **Naming:** `snake_case` for Python variables/functions/files; `camelCase` for JavaScript variables/functions.
- **Performance:** Throttle `heatLayer.setLatLngs()` updates to `300ms–500ms` and scale `radius` dynamically on Leaflet `zoomend` to prevent UI lag and color distortion.
- **DO NOT:**
  - Do NOT introduce Next.js, React, TypeScript, or Node.js dependencies.
  - Do NOT overwrite existing working features (Hot-swap battery handover, zoom-scaled Heatmap, continuous fire containment loop) when adding new features.
  - Do NOT modify files without reading their current structure first.

## 5. Key Domain Entities (Mental Model)
- **3-Tier SAGSINS:** Space (3 LEO Satellites) + Air (35 UAVs: 28 Active across 7 zones Z1–Z7 [4 UAVs/zone] + 7 Standby reserve at Ranger Station) + Ground (Son Tra Ranger Station + WSN Heatmap grid).
- **UAV States & Hot-Swap Lifecycle:** `PATROLLING` (45–100% staggered initial battery), `RESPONDING` / `CONTAINING` (circling active fire within `600m`), `RETURNING` (`< 25%` battery triggers P2P handover, wakes a `STANDBY` reserve UAV to replace it, and flies straight to `TRẠM_KIỂM_LÂM_SƠN_TRÀ`), `CHARGING` (`<= 100m` around Station, fast-recharging at `+15%/s -> 100%`), `STANDBY` (`100%` reserve ready at Station), `OFFLINE` (crashed/simulated failure).

## 6. Verification & Workflow Rules
- Always inspect target files (`@filename`) before proposing or applying edits.
- Keep simulation metrics logging intact for Dashboard charts (PDR %, average latency ms, battery consumption).