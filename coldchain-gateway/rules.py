"""R1-R6 rule engine. It decides; gateway.py performs all I/O."""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

try:
    from .config import CONFIG
    from .state import ShipmentState
except ImportError:
    from config import CONFIG
    from state import ShipmentState


log = logging.getLogger("gateway.rules")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _command(shipment_id: str, target: str, action: str, reason: str) -> Dict:
    return {
        "shipment_id": shipment_id,
        "target": target,
        "action": action,
        "reason": reason,
        "issued_by": "gateway",
        "timestamp": _now_iso(),
    }


def _event(
    shipment_id: str,
    event_type: str,
    severity: str,
    message: str,
    details: Dict,
) -> Dict:
    return {
        "shipment_id": shipment_id,
        "event_type": event_type,
        "severity": severity,
        "message": message,
        "details": details,
        "timestamp": _now_iso(),
    }


class RuleEngine:
    def __init__(self, cfg=CONFIG):
        self.cfg = cfg

    def evaluate(self, state: ShipmentState) -> Tuple[List[Dict], List[Dict]]:
        commands: List[Dict] = []
        events: List[Dict] = []
        temperature = state.last_temperature

        # R2: activate at TEMP_MAX + margin and stay active through the dead band.
        if (
            temperature is not None
            and state.temperature_high_active
            and state.cooling_unit != "high"
        ):
            commands.append(
                _command(
                    state.shipment_id,
                    "cooling_unit",
                    "increase_power",
                    f"temp {temperature}C >= ngưỡng bật {self.cfg.temp_upper}C",
                )
            )
            log.info("[%s] R2 increase_power (temp=%s)", state.shipment_id, temperature)

        # R1: temperature violation only after the configured streak.
        if (
            state.high_temp_streak >= self.cfg.temp_violation_cycles
            and not state.temp_violation_active
        ):
            state.temp_violation_active = True
            events.append(
                _event(
                    state.shipment_id,
                    "temperature_violation",
                    "critical",
                    f"Nhiệt độ vượt ngưỡng {self.cfg.temp_violation_cycles} chu kỳ liên tiếp",
                    {
                        "temperature": temperature,
                        "streak": state.high_temp_streak,
                        "threshold": self.cfg.temp_max,
                        "trigger_threshold": self.cfg.temp_upper,
                        "recovery_threshold": self.cfg.temp_lower,
                        "margin": max(0.0, self.cfg.temp_margin),
                    },
                )
            )
            log.warning(
                "[%s] R1 TEMP VIOLATION streak=%d",
                state.shipment_id,
                state.high_temp_streak,
            )

        # R6: return cooling to normal whenever temperature is safe. This also
        # clears a stale HIGH status after a device restart before R1 fired.
        temperature_is_safe = (
            temperature is not None and temperature <= self.cfg.temp_lower
        )
        if temperature_is_safe and (
            state.temp_violation_active or state.cooling_unit == "high"
        ):
            commands.append(
                _command(
                    state.shipment_id,
                    "cooling_unit",
                    "normal_power",
                    f"temp {temperature}C <= ngưỡng hồi phục {self.cfg.temp_lower}C",
                )
            )

        if state.temp_violation_active and temperature_is_safe:
            state.temp_violation_active = False
            events.append(
                _event(
                    state.shipment_id,
                    "recovered",
                    "info",
                    "Nhiệt độ trở lại bình thường",
                    {
                        "temperature": temperature,
                        "threshold": self.cfg.temp_max,
                        "recovery_threshold": self.cfg.temp_lower,
                        "margin": max(0.0, self.cfg.temp_margin),
                    },
                )
            )
            log.info("[%s] R6 recovered (temp=%s)", state.shipment_id, temperature)
        elif temperature_is_safe and state.cooling_unit == "high":
            log.info(
                "[%s] R6 reset stale cooling HIGH (temp=%s)",
                state.shipment_id,
                temperature,
            )

        # R3: door remains open for multiple cycles.
        if (
            state.door_open_streak >= self.cfg.door_open_cycles
            and not state.door_alarm_active
        ):
            state.door_alarm_active = True
            commands.append(
                _command(
                    state.shipment_id,
                    "alarm",
                    "alarm_on",
                    f"Cửa mở {state.door_open_streak} chu kỳ liên tiếp",
                )
            )
            events.append(
                _event(
                    state.shipment_id,
                    "door_open_alarm",
                    "warning",
                    f"Cửa mở liên tục {state.door_open_streak} chu kỳ",
                    {
                        "door_open_streak": state.door_open_streak,
                        "threshold": self.cfg.door_open_cycles,
                    },
                )
            )
            log.warning(
                "[%s] R3 DOOR ALARM streak=%d",
                state.shipment_id,
                state.door_open_streak,
            )
        elif not state.door_open and state.door_alarm_active:
            state.door_alarm_active = False
            commands.append(
                _command(state.shipment_id, "alarm", "alarm_off", "Cửa đã đóng")
            )
            log.info("[%s] R3 door closed -> alarm_off", state.shipment_id)

        # R4: low battery, deduplicated until the battery recovers.
        battery = state.battery_percent
        if battery is not None and battery < self.cfg.battery_min:
            if not state.battery_low_active:
                state.battery_low_active = True
                events.append(
                    _event(
                        state.shipment_id,
                        "battery_low",
                        "warning",
                        f"Pin thấp: {battery}% < {self.cfg.battery_min}%",
                        {
                            "battery_percent": battery,
                            "threshold": self.cfg.battery_min,
                        },
                    )
                )
                log.warning("[%s] R4 BATTERY LOW batt=%s", state.shipment_id, battery)
        elif battery is not None:
            state.battery_low_active = False

        return commands, events

    def check_offline(self, state: ShipmentState, now: float) -> List[Dict]:
        elapsed = now - state.last_telemetry_ts
        if elapsed > self.cfg.offline_timeout and not state.offline_active:
            state.offline_active = True
            state.online = False
            log.warning("[%s] R5 DEVICE OFFLINE (%.0fs)", state.shipment_id, elapsed)
            return [
                _event(
                    state.shipment_id,
                    "device_offline",
                    "critical",
                    f"Mất telemetry {elapsed:.0f}s > {self.cfg.offline_timeout}s",
                    {
                        "elapsed_seconds": round(elapsed, 1),
                        "threshold": self.cfg.offline_timeout,
                    },
                )
            ]
        return []
