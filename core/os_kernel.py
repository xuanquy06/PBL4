"""
Lõi Hệ điều hành (OS Kernel Simulator) chạy thật cho hệ thống SAGSINS PBL4:
1. Counting Semaphore giới hạn tài nguyên sạc tại Trạm Kiểm Lâm (ChargingDockSemaphore).
2. Mutex (threading.RLock) bảo vệ miền găng dùng chung (khóa mục tiêu cháy, thống kê & buffer mạng).
3. Lập lịch ưu tiên 3 cấp P0 / P1 / P2 có chống tràn bộ đệm (BoundedPriorityQueue).
4. Đa luồng ThreadPoolExecutor & đo tài nguyên tiến trình thật từ Hệ điều hành (PID, Threads, CPU %, RAM MB).
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import ctypes
from ctypes import wintypes
import itertools
import os
import sys
import threading
import time
from typing import Any, Optional


CRITICAL_MSG_TYPES = frozenset({
    "FIRE_ALERT",
    "FIRE_DETECTION_REPORT",
    "GIAO_NHIEM_VU_DU_LIEU",
    "YEU_CAU_HO_TRO_CHAY_P2P",
    "FIRE_CONTAINMENT_TELEMETRY",
    "XAC_NHAN_ACK",
})

NORMAL_MSG_TYPES = frozenset({
    "TELEMETRY_REPORT",
    "YEU_CAU_BAN_GIAO_VUNG",
    "THONG_BAO_KHOI_PHUC_VUNG",
    "EMERGENCY_RETURN",
    "PHAT_HIEN_SU_CO",
})


def phan_loai_do_uu_tien(loai_tin: Optional[str]) -> int:
    """
    Ánh xạ loại bản tin sang độ ưu tiên lập lịch HĐH:
    - 0 (P0 - CRITICAL): Cảnh báo cháy, dữ liệu khoanh vùng, bàn giao cháy P2P, ACK.
    - 1 (P1 - NORMAL): Telemetry định kỳ, bàn giao vùng pin yếu, RTB.
    - 2 (P2 - LOW): Gói HELLO_BEACON quảng bá láng giềng.
    """
    kind = str(loai_tin or "UNKNOWN").upper()
    if kind in CRITICAL_MSG_TYPES:
        return 0
    if kind in NORMAL_MSG_TYPES:
        return 1
    return 2


class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("PageFaultCount", wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


class ChargingDockSemaphore:
    """
    Counting Semaphore thật (threading.Semaphore + RLock) điều phối tài nguyên
    hữu hạn tại Trạm Kiểm Lâm Sơn Trà (tối đa `capacity` đế sạc đồng thời).
    UAV đến sau khi hết cổng sẽ bị chặn vào hàng đợi FIFO (`WAITING_DOCK`).
    """

    def __init__(self, capacity: int = 3):
        self.capacity = max(1, int(capacity))
        self._sem = threading.Semaphore(self.capacity)
        self._lock = threading.RLock()
        self._active_docks: list[str] = []
        self._wait_queue: list[str] = []
        self._wait_start: dict[str, float] = {}
        self.total_acquisitions = 0
        self.total_releases = 0
        self.total_wait_events = 0
        self.total_wait_time_sec = 0.0

    def try_acquire_dock(self, uav_id: str) -> tuple[bool, int]:
        """
        Thử chiếm 1 đế sạc không nghẽn (non-blocking).
        Trả về `(acquired, wait_queue_position)`:
        - `(True, 0)` nếu UAV đang giữ cổng hoặc vừa chiếm được cổng theo thứ tự FIFO.
        - `(False, pos)` nếu cả `capacity` cổng đang bận hoặc có UAV khác xếp trước.
        """
        with self._lock:
            if uav_id in self._active_docks:
                return True, 0

            can_try = (not self._wait_queue) or (self._wait_queue[0] == uav_id)
            if can_try and self._sem.acquire(blocking=False):
                if uav_id in self._wait_queue:
                    self._wait_queue.remove(uav_id)
                start_ts = self._wait_start.pop(uav_id, None)
                if start_ts is not None:
                    self.total_wait_time_sec += max(0.0, time.monotonic() - start_ts)
                self._active_docks.append(uav_id)
                self.total_acquisitions += 1
                return True, 0

            if uav_id not in self._wait_queue:
                self._wait_queue.append(uav_id)
                self._wait_start[uav_id] = time.monotonic()
                self.total_wait_events += 1
            pos = self._wait_queue.index(uav_id) + 1
            return False, pos

    def release_dock(self, uav_id: str) -> bool:
        """Giải phóng cổng sạc (hoặc rút khỏi hàng đợi chờ) một cách an toàn và idempotent."""
        with self._lock:
            released = False
            if uav_id in self._active_docks:
                self._active_docks.remove(uav_id)
                self._sem.release()
                self.total_releases += 1
                released = True
            if uav_id in self._wait_queue:
                self._wait_queue.remove(uav_id)
                start_ts = self._wait_start.pop(uav_id, None)
                if start_ts is not None:
                    self.total_wait_time_sec += max(0.0, time.monotonic() - start_ts)
            return released

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            in_use = len(self._active_docks)
            avg_wait = (
                round(self.total_wait_time_sec / self.total_wait_events, 2)
                if self.total_wait_events > 0
                else 0.0
            )
            return {
                "capacity": self.capacity,
                "in_use": in_use,
                "available": max(0, self.capacity - in_use),
                "active_docks": list(self._active_docks),
                "wait_queue": list(self._wait_queue),
                "total_acquisitions": self.total_acquisitions,
                "total_releases": self.total_releases,
                "total_wait_events": self.total_wait_events,
                "avg_wait_sec": avg_wait,
            }


class BoundedPriorityQueue:
    """
    Hàng đợi lập lịch ưu tiên giới hạn bộ đệm (Bounded Priority Queue - Producer/Consumer):
    - Rút gói tin theo thứ tự ưu tiên P0 -> P1 -> P2, cùng mức ưu tiên theo FIFO (`seq`).
    - Tự động hủy gói mới và đếm Buffer Overflow nếu đầy để bảo vệ bộ nhớ.
    """
    _global_seq = itertools.count()

    def __init__(self, maxsize: int = 100, os_kernel: Optional["OSKernelManager"] = None):
        self.maxsize = max(0, int(maxsize))
        self._os_kernel = os_kernel
        self._queue: Optional[asyncio.PriorityQueue] = None

    def _get_queue(self) -> asyncio.PriorityQueue:
        if self._queue is None:
            self._queue = asyncio.PriorityQueue(maxsize=self.maxsize)
        return self._queue

    def qsize(self) -> int:
        if self._queue is None:
            return 0
        return self._queue.qsize()

    def empty(self) -> bool:
        if self._queue is None:
            return True
        return self._queue.empty()

    def full(self) -> bool:
        if self._queue is None or self.maxsize <= 0:
            return False
        return self._queue.full()

    def _build_entry(self, item: Any) -> tuple[int, int, float, Any]:
        loai_tin = item.get("loai_tin", "UNKNOWN") if isinstance(item, dict) else "UNKNOWN"
        prio = phan_loai_do_uu_tien(loai_tin)
        seq = next(self._global_seq)
        return (prio, seq, time.monotonic(), item)

    async def put(self, item: Any):
        self.put_nowait(item)

    def put_nowait(self, item: Any):
        q = self._get_queue()
        if self.maxsize > 0 and q.full():
            if self._os_kernel is not None:
                self._os_kernel.record_buffer_drop()
            return
        try:
            q.put_nowait(self._build_entry(item))
        except asyncio.QueueFull:
            if self._os_kernel is not None:
                self._os_kernel.record_buffer_drop()

    def _unwrap_entry(self, entry: tuple[int, int, float, Any]) -> Any:
        prio, _, enqueue_ts, item = entry
        wait_ms = max(0.0, (time.monotonic() - enqueue_ts) * 1000.0)
        if self._os_kernel is not None:
            self._os_kernel.record_scheduler_dispatch(prio, wait_ms)
        return item

    async def get(self) -> Any:
        entry = await self._get_queue().get()
        return self._unwrap_entry(entry)

    def get_nowait(self) -> Any:
        entry = self._get_queue().get_nowait()
        return self._unwrap_entry(entry)

    def task_done(self):
        self._get_queue().task_done()


class OSKernelManager:
    """
    Trình quản lý tài nguyên Hệ điều hành tập trung cho mô phỏng SAGSINS:
    - Counting Semaphore trạm sạc (`charging_semaphore`).
    - Mutex RLock bảo vệ miền găng (`fire_target_lock`, `net_state_lock`).
    - ThreadPoolExecutor thực thi tác vụ nền song song.
    - Giám sát hiệu năng tiến trình thực (PID, Threads, CPU %, RAM MB, Scheduler).
    """

    def __init__(self, max_docks: int = 3, max_workers: int = 4):
        self.charging_semaphore = ChargingDockSemaphore(capacity=max_docks)
        self.fire_target_lock = threading.RLock()
        self.net_state_lock = threading.RLock()
        self._stats_lock = threading.RLock()

        self.max_workers = max(1, int(max_workers))
        self.executor: Optional[ThreadPoolExecutor] = None

        self.mutex_acquisitions = 0
        self.race_conditions_blocked = 0
        self.buffer_drops = 0
        self.dispatched_by_prio = {0: 0, 1: 0, 2: 0}
        self.wait_ms_sum_by_prio = {0: 0.0, 1: 0.0, 2: 0.0}

        self._last_cpu_time = time.process_time()
        self._last_wall_time = time.monotonic()
        self._last_cpu_pct = 0.0

    def start(self):
        if self.executor is None:
            self.executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix="SAGSINS-Worker",
            )

    def stop(self):
        if self.executor is not None:
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.executor = None

    def record_mutex_acquire(self, conflict: bool = False):
        with self._stats_lock:
            self.mutex_acquisitions += 1
            if conflict:
                self.race_conditions_blocked += 1

    def record_buffer_drop(self):
        with self._stats_lock:
            self.buffer_drops += 1

    def record_scheduler_dispatch(self, priority: int, wait_ms: float):
        prio = priority if priority in (0, 1, 2) else 2
        with self._stats_lock:
            self.dispatched_by_prio[prio] += 1
            self.wait_ms_sum_by_prio[prio] += max(0.0, float(wait_ms))

    @staticmethod
    def get_memory_rss_mb() -> float:
        """Đọc RAM thực tế (Working Set / RSS) của tiến trình từ OS API."""
        if sys.platform == "win32":
            try:
                kernel32 = ctypes.windll.kernel32
                psapi = ctypes.windll.psapi
                handle = kernel32.GetCurrentProcess()
                pmc = _PROCESS_MEMORY_COUNTERS()
                pmc.cb = ctypes.sizeof(_PROCESS_MEMORY_COUNTERS)
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                    return round(pmc.WorkingSetSize / (1024.0 * 1024.0), 2)
            except Exception:
                pass
        else:
            try:
                import resource
                usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if sys.platform == "darwin":
                    return round(usage_kb / (1024.0 * 1024.0), 2)
                return round(usage_kb / 1024.0, 2)
            except Exception:
                pass
        return 0.0

    def _sample_cpu_percent(self) -> tuple[float, float]:
        now_cpu = time.process_time()
        now_wall = time.monotonic()
        d_wall = now_wall - self._last_wall_time
        if d_wall >= 0.2:
            d_cpu = max(0.0, now_cpu - self._last_cpu_time)
            cpu_count = max(1, os.cpu_count() or 1)
            raw_pct = (d_cpu / d_wall) * 100.0 / cpu_count
            self._last_cpu_pct = round(min(100.0, max(0.0, raw_pct)), 2)
            self._last_cpu_time = now_cpu
            self._last_wall_time = now_wall
        return self._last_cpu_pct, round(now_cpu, 2)

    def snapshot(self) -> dict[str, Any]:
        cpu_pct, cpu_sec = self._sample_cpu_percent()
        threads_list = []
        for t in threading.enumerate():
            threads_list.append({
                "name": t.name,
                "tid": getattr(t, "native_id", None) or t.ident,
                "daemon": t.daemon,
                "alive": t.is_alive(),
            })

        with self._stats_lock:
            p0_c = self.dispatched_by_prio[0]
            p1_c = self.dispatched_by_prio[1]
            p2_c = self.dispatched_by_prio[2]
            scheduler_snap = {
                "p0_count": p0_c,
                "p1_count": p1_c,
                "p2_count": p2_c,
                "p0_avg_wait_ms": round(self.wait_ms_sum_by_prio[0] / p0_c, 2) if p0_c else 0.0,
                "p1_avg_wait_ms": round(self.wait_ms_sum_by_prio[1] / p1_c, 2) if p1_c else 0.0,
                "p2_avg_wait_ms": round(self.wait_ms_sum_by_prio[2] / p2_c, 2) if p2_c else 0.0,
                "buffer_drops": self.buffer_drops,
            }
            mutex_snap = {
                "acquisitions": self.mutex_acquisitions,
                "race_conditions_blocked": self.race_conditions_blocked,
            }

        return {
            "pid": os.getpid(),
            "memory_rss_mb": self.get_memory_rss_mb(),
            "cpu_percent": cpu_pct,
            "cpu_time_sec": cpu_sec,
            "active_threads_count": len(threads_list),
            "threads": threads_list[:16],
            "charging_semaphore": self.charging_semaphore.snapshot(),
            "mutex": mutex_snap,
            "scheduler": scheduler_snap,
        }
