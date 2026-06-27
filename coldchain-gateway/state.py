"""Quản lý trạng thái đa chu kỳ per-shipment + đồng bộ Redis.
Là nơi hiện thực tiêu chí 1.4đ: 'gateway theo dõi trạng thái qua nhiều chu kỳ'.
"""
import json
import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import redis

from config import CONFIG

log = logging.getLogger("gateway.state")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _as_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

@dataclass
class ShipmentState:
    shipment_id: str
    device_id: str = ""
    # --- Giá trị đo mới nhất ---
    last_temperature: float | None = None
    last_humidity: float | None = None
    battery_percent: float | None = None
    door_open: bool = False
    # --- Bộ đếm đa chu kỳ (TRÁI TIM của 1.4đ) ---
    high_temp_streak: int = 0
    door_open_streak: int = 0
    # --- Trạng thái actuator (cập nhật từ status) ---
    cooling_unit: str = "normal"   # off / normal / high
    alarm: bool = False
    # --- Cờ dedup: mỗi vi phạm chỉ bắn event 1 lần ---
    temp_violation_active: bool = False
    door_alarm_active: bool = False
    battery_low_active: bool = False
    offline_active: bool = False
    # --- Theo dõi vòng đời ---
    online: bool = True
    last_telemetry_ts: float = field(default_factory=time.time)
    cycles: int = 0
    last_event: str = ""
    updated_at: str = field(default_factory=_now_iso)

    def to_redis_dict(self) -> dict:
        d = asdict(self)
        d["last_telemetry_iso"] = datetime.fromtimestamp(
            self.last_telemetry_ts, tz=timezone.utc
        ).isoformat()
        return d

class StateStore:
    """Giữ state của mọi shipment trong RAM + đồng bộ Redis (thread-safe)."""

    def __init__(self):
        self._states: dict[str, ShipmentState] = {}
        self._lock = threading.Lock()
        self._redis = redis.Redis(
            host=CONFIG.redis_host,
            port=CONFIG.redis_port,
            decode_responses=True,
        )

    # ---------- Truy cập ----------
    def get(self, shipment_id: str) -> ShipmentState:
        with self._lock:
            st = self._states.get(shipment_id)
            if st is None:
                st = ShipmentState(shipment_id=shipment_id)
                self._states[shipment_id] = st
            return st

    def snapshot(self) -> list[ShipmentState]:
        """Bản sao danh sách state để offline checker quét an toàn."""
        with self._lock:
            return list(self._states.values())

    # ---------- Cập nhật từ telemetry (đếm streak ở đây) ----------
    def update_from_telemetry(self, shipment_id: str, payload: dict) -> ShipmentState:
        with self._lock:
            st = self._states.setdefault(
                shipment_id, ShipmentState(shipment_id=shipment_id)
            )
            st.device_id = payload.get("device_id", st.device_id)
            temp = _as_float(payload.get("temperature"))
            st.last_temperature = temp
            st.last_humidity = _as_float(payload.get("humidity"))
            st.battery_percent = _as_float(payload.get("battery_percent"))
            st.door_open = bool(payload.get("door_open", False))

            # ===== Đếm streak đa chu kỳ =====
            if temp is not None and temp > CONFIG.temp_max:
                st.high_temp_streak += 1
            else:
                st.high_temp_streak = 0

            if st.door_open:
                st.door_open_streak += 1
            else:
                st.door_open_streak = 0

            st.last_telemetry_ts = time.time()
            st.online = True
            st.offline_active = False  # nhận telemetry => không còn offline
            st.cycles += 1
            st.updated_at = _now_iso()

            log.info(
                "[%s] cycle=%d temp=%s temp_streak=%d door=%s door_streak=%d batt=%s",
                shipment_id, st.cycles, st.last_temperature,
                st.high_temp_streak, st.door_open,
                st.door_open_streak, st.battery_percent,
            )
            return st

    # ---------- Cập nhật từ actuator status ----------
    def update_from_status(self, shipment_id: str, payload: dict) -> ShipmentState:
        with self._lock:
            st = self._states.setdefault(
                shipment_id, ShipmentState(shipment_id=shipment_id)
            )
            st.cooling_unit = payload.get("cooling_unit", st.cooling_unit)
            st.alarm = bool(payload.get("alarm", st.alarm))
            st.updated_at = _now_iso()
            return st

    # ---------- Ghi Redis ----------
    def sync_redis(self, shipment_id: str) -> None:
        st = self.get(shipment_id)
        try:
            self._redis.set(
                f"shipment:{shipment_id}:state",
                json.dumps(st.to_redis_dict(), ensure_ascii=False),
            )
            self._redis.sadd("shipments:index", shipment_id)
        except redis.RedisError as e:
            log.warning("Redis sync state lỗi (%s): %s", shipment_id, e)

    def push_event(self, shipment_id: str, event: dict) -> None:
        """Lưu event vào Redis list (LPUSH + LTRIM giữ N event gần nhất)."""
        with self._lock:
            st = self._states.setdefault(
                shipment_id, ShipmentState(shipment_id=shipment_id)
            )
            st.last_event = event.get("event_type", "")
        try:
            key = f"shipment:{shipment_id}:events"
            self._redis.lpush(key, json.dumps(event, ensure_ascii=False))
            self._redis.ltrim(key, 0, CONFIG.events_keep - 1)
        except redis.RedisError as e:
            log.warning("Redis push event lỗi (%s): %s", shipment_id, e)
