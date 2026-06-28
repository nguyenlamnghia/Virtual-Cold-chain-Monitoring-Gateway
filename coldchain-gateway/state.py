import copy
import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import redis

try:
    from .config import CONFIG
except ImportError:  # Running as /app/state.py in the container.
    from config import CONFIG


log = logging.getLogger("gateway.state")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class ShipmentState:
    shipment_id: str
    device_id: str = ""
    last_temperature: Optional[float] = None
    last_humidity: Optional[float] = None
    battery_percent: Optional[float] = None
    door_open: bool = False
    high_temp_streak: int = 0
    door_open_streak: int = 0
    cooling_unit: str = "normal"
    alarm: bool = False
    temp_violation_active: bool = False
    door_alarm_active: bool = False
    battery_low_active: bool = False
    offline_active: bool = False
    online: bool = True
    last_telemetry_ts: float = field(default_factory=time.time)
    cycles: int = 0
    last_event: str = ""
    updated_at: str = field(default_factory=utc_now)

    def to_redis_dict(self) -> Dict:
        data = asdict(self)
        data["last_telemetry_iso"] = datetime.fromtimestamp(
            self.last_telemetry_ts, tz=timezone.utc
        ).isoformat().replace("+00:00", "Z")
        return data


class StateStore:
    def __init__(self, cfg=CONFIG, redis_client=None):
        self.cfg = cfg
        self._states: Dict[str, ShipmentState] = {}
        self._lock = threading.RLock()
        self._redis = redis_client or redis.Redis(
            host=cfg.redis_host,
            port=cfg.redis_port,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        self._restore_from_redis()

    def _restore_from_redis(self) -> None:
        """Restore current state so a gateway restart does not erase streaks."""
        try:
            shipment_ids = self._redis.smembers("shipments:index")
            allowed = {item.name for item in fields(ShipmentState)}
            for shipment_id in shipment_ids:
                raw = self._redis.get(f"shipment:{shipment_id}:state")
                if not raw:
                    continue
                data = json.loads(raw)
                state_data = {key: value for key, value in data.items() if key in allowed}
                state_data["shipment_id"] = shipment_id
                self._states[shipment_id] = ShipmentState(**state_data)
            if shipment_ids:
                log.info("Restored %d shipment state(s) from Redis", len(self._states))
        except (redis.RedisError, ValueError, TypeError) as exc:
            log.warning("Could not restore state from Redis: %s", exc)

    def _get_unlocked(self, shipment_id: str) -> ShipmentState:
        state = self._states.get(shipment_id)
        if state is None:
            state = ShipmentState(shipment_id=shipment_id)
            self._states[shipment_id] = state
        return state

    def get(self, shipment_id: str) -> ShipmentState:
        with self._lock:
            return copy.deepcopy(self._get_unlocked(shipment_id))

    def snapshot(self) -> List[ShipmentState]:
        with self._lock:
            return copy.deepcopy(list(self._states.values()))

    def _apply_telemetry(self, state: ShipmentState, payload: Dict) -> None:
        state.device_id = str(payload.get("device_id", state.device_id))
        state.last_temperature = _as_float(payload.get("temperature"))
        state.last_humidity = _as_float(payload.get("humidity"))
        state.battery_percent = _as_float(payload.get("battery_percent"))
        state.door_open = payload.get("door_open", False) is True

        if state.last_temperature is not None and state.last_temperature > self.cfg.temp_max:
            state.high_temp_streak += 1
        else:
            state.high_temp_streak = 0

        if state.door_open:
            state.door_open_streak += 1
        else:
            state.door_open_streak = 0

        state.last_telemetry_ts = time.time()
        state.online = True
        state.offline_active = False
        state.cycles += 1
        state.updated_at = utc_now()
        log.info(
            "[%s] cycle=%d temp=%s temp_streak=%d door=%s door_streak=%d batt=%s",
            state.shipment_id,
            state.cycles,
            state.last_temperature,
            state.high_temp_streak,
            state.door_open,
            state.door_open_streak,
            state.battery_percent,
        )

    def process_telemetry(
        self,
        shipment_id: str,
        payload: Dict,
        evaluator: Callable[[ShipmentState], Tuple[List[Dict], List[Dict]]],
    ) -> Tuple[ShipmentState, List[Dict], List[Dict]]:
        """Update counters and evaluate rules under the same lock."""
        with self._lock:
            state = self._get_unlocked(shipment_id)
            self._apply_telemetry(state, payload)
            commands, events = evaluator(state)
            return copy.deepcopy(state), commands, events

    def update_from_telemetry(self, shipment_id: str, payload: Dict) -> ShipmentState:
        with self._lock:
            state = self._get_unlocked(shipment_id)
            self._apply_telemetry(state, payload)
            return copy.deepcopy(state)

    def update_from_status(self, shipment_id: str, payload: Dict) -> ShipmentState:
        with self._lock:
            state = self._get_unlocked(shipment_id)
            state.cooling_unit = str(payload.get("cooling_unit", state.cooling_unit))
            alarm = payload.get("alarm", state.alarm)
            state.alarm = alarm if isinstance(alarm, bool) else str(alarm).lower() == "on"
            state.updated_at = utc_now()
            return copy.deepcopy(state)

    def check_offline(
        self,
        now: float,
        evaluator: Callable[[ShipmentState, float], List[Dict]],
    ) -> List[Tuple[str, List[Dict]]]:
        results = []
        with self._lock:
            for state in self._states.values():
                events = evaluator(state, now)
                if events:
                    results.append((state.shipment_id, events))
        return results

    def sync_redis(self, shipment_id: str) -> None:
        state = self.get(shipment_id)
        try:
            pipeline = self._redis.pipeline()
            pipeline.set(
                f"shipment:{shipment_id}:state",
                json.dumps(state.to_redis_dict(), ensure_ascii=False),
            )
            pipeline.sadd("shipments:index", shipment_id)
            pipeline.execute()
        except redis.RedisError as exc:
            log.warning("Redis state sync failed for %s: %s", shipment_id, exc)

    def push_event(self, shipment_id: str, event: Dict) -> None:
        with self._lock:
            state = self._get_unlocked(shipment_id)
            state.last_event = str(event.get("event_type", ""))
            state.updated_at = utc_now()
        try:
            key = f"shipment:{shipment_id}:events"
            pipeline = self._redis.pipeline()
            pipeline.lpush(key, json.dumps(event, ensure_ascii=False))
            pipeline.ltrim(key, 0, max(1, self.cfg.events_keep) - 1)
            pipeline.execute()
        except redis.RedisError as exc:
            log.warning("Redis event push failed for %s: %s", shipment_id, exc)

    def redis_available(self) -> bool:
        try:
            return bool(self._redis.ping())
        except redis.RedisError:
            return False
