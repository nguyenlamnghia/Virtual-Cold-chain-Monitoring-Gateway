"""Rule engine R1-R6: nhận ShipmentState đã cập nhật, sinh (commands, events).
Không tự publish/ghi — chỉ *quyết định*; gateway.py làm việc I/O.
"""
import logging
from datetime import datetime, timezone

from config import CONFIG
from state import ShipmentState

log = logging.getLogger("gateway.rules")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _command(shipment_id: str, target: str, action: str, reason: str) -> dict:
    return {
        "shipment_id": shipment_id,
        "target": target,
        "action": action,
        "reason": reason,
        "issued_by": "gateway",
        "timestamp": _now_iso(),
    }

def _event(shipment_id: str, event_type: str, severity: str,
           message: str, details: dict) -> dict:
    return {
        "shipment_id": shipment_id,
        "event_type": event_type,
        "severity": severity,
        "message": message,
        "details": details,
        "timestamp": _now_iso(),
    }

class RuleEngine:
    """Đánh giá luật trên state đã cập nhật của chu kỳ hiện tại."""

    def __init__(self, cfg=CONFIG):
        self.cfg = cfg

    # ===== Đánh giá theo mỗi telemetry (R1, R2, R3, R4, R6) =====
    def evaluate(self, st: ShipmentState) -> tuple[list[dict], list[dict]]:
        commands: list[dict] = []
        events: list[dict] = []
        cfg = self.cfg
        temp = st.last_temperature

        # ---- R2: tức thời — nhiệt vượt ngưỡng -> tăng lạnh ----
        if temp is not None and temp > cfg.temp_max and st.cooling_unit != "high":
            commands.append(_command(
                st.shipment_id, "cooling_unit", "increase_power",
                f"temp {temp}C > TEMP_MAX {cfg.temp_max}C",
            ))
            log.info("[%s] R2 increase_power (temp=%s)", st.shipment_id, temp)

        # ---- R1: đa chu kỳ — vượt ngưỡng N chu kỳ liên tiếp ----
        if (st.high_temp_streak >= cfg.temp_violation_cycles
                and not st.temp_violation_active):
            st.temp_violation_active = True
            events.append(_event(
                st.shipment_id, "temperature_violation", "critical",
                f"Nhiệt độ vượt ngưỡng {cfg.temp_violation_cycles} chu kỳ liên tiếp",
                {"temperature": temp, "streak": st.high_temp_streak,
                 "threshold": cfg.temp_max},
            ))
            log.warning("[%s] R1 TEMP VIOLATION streak=%d",
                        st.shipment_id, st.high_temp_streak)

        # ---- R6: hồi phục nhiệt độ ----
        if st.temp_violation_active and temp is not None and temp <= cfg.temp_max:
            st.temp_violation_active = False
            commands.append(_command(
                st.shipment_id, "cooling_unit", "normal_power",
                f"temp {temp}C <= TEMP_MAX {cfg.temp_max}C (hồi phục)",
            ))
            events.append(_event(
                st.shipment_id, "recovered", "info",
                "Nhiệt độ trở lại bình thường",
                {"temperature": temp, "threshold": cfg.temp_max},
            ))
            log.info("[%s] R6 recovered (temp=%s)", st.shipment_id, temp)

        # ---- R3: cửa mở nhiều chu kỳ liên tiếp -> báo động ----
        if (st.door_open_streak >= cfg.door_open_cycles
                and not st.door_alarm_active):
            st.door_alarm_active = True
            commands.append(_command(
                st.shipment_id, "alarm", "alarm_on",
                f"Cửa mở {st.door_open_streak} chu kỳ liên tiếp",
            ))
            events.append(_event(
                st.shipment_id, "door_open_alarm", "warning",
                f"Cửa mở liên tục {st.door_open_streak} chu kỳ",
                {"door_open_streak": st.door_open_streak,
                 "threshold": cfg.door_open_cycles},
            ))
            log.warning("[%s] R3 DOOR ALARM streak=%d",
                        st.shipment_id, st.door_open_streak)
        # Cửa đóng trở lại -> gỡ cờ + tắt báo động
        if not st.door_open and st.door_alarm_active:
            st.door_alarm_active = False
            commands.append(_command(
                st.shipment_id, "alarm", "alarm_off", "Cửa đã đóng",
            ))
            log.info("[%s] R3 door closed -> alarm_off", st.shipment_id)

        # ---- R4: pin thấp ----
        batt = st.battery_percent
        if batt is not None and batt < cfg.battery_min:
            if not st.battery_low_active:
                st.battery_low_active = True
                events.append(_event(
                    st.shipment_id, "battery_low", "warning",
                    f"Pin thấp: {batt}% < {cfg.battery_min}%",
                    {"battery_percent": batt, "threshold": cfg.battery_min},
                ))
                log.warning("[%s] R4 BATTERY LOW batt=%s", st.shipment_id, batt)
        elif batt is not None and batt >= cfg.battery_min:
            st.battery_low_active = False  # pin hồi phục -> gỡ cờ

        return commands, events

    # ===== R5: gọi định kỳ bởi offline checker thread =====
    def check_offline(self, st: ShipmentState, now: float) -> list[dict]:
        events: list[dict] = []
        elapsed = now - st.last_telemetry_ts
        if elapsed > self.cfg.offline_timeout and not st.offline_active:
            st.offline_active = True
            st.online = False
            events.append(_event(
                st.shipment_id, "device_offline", "critical",
                f"Mất telemetry {elapsed:.0f}s > {self.cfg.offline_timeout}s",
                {"elapsed_seconds": round(elapsed, 1),
                 "threshold": self.cfg.offline_timeout},
            ))
            log.warning("[%s] R5 DEVICE OFFLINE (%.0fs)", st.shipment_id, elapsed)
        return events
