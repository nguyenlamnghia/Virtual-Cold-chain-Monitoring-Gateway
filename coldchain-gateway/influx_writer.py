"""Ghi telemetry / actuator_status / events vào InfluxDB 2.x.
Mã hóa cờ trạng thái thành số để Grafana (M1) vẽ được.
"""
import logging

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from config import CONFIG

log = logging.getLogger("gateway.influx")

# Ánh xạ cooling_unit -> số (khớp panel state-timeline của M1)
COOLING_MAP = {"off": 0, "normal": 1, "high": 2}

class InfluxWriter:
    def __init__(self, cfg=CONFIG):
        self.cfg = cfg
        self._client = InfluxDBClient(
            url=cfg.influx_url,
            token=cfg.influx_token,
            org=cfg.influx_org,
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def _write(self, point: Point) -> None:
        try:
            self._write_api.write(bucket=self.cfg.influx_bucket, record=point)
        except Exception as e:  # không để lỗi Influx làm sập gateway
            log.warning("Ghi InfluxDB lỗi: %s", e)

    # ---------- telemetry ----------
    def write_telemetry(self, shipment_id: str, payload: dict) -> None:
        p = (
            Point("telemetry")
            .tag("shipment_id", shipment_id)
            .tag("device_id", str(payload.get("device_id", "")))
        )
        for fld in ("temperature", "humidity", "battery_percent"):
            val = payload.get(fld)
            if val is not None:
                p = p.field(fld, float(val))
        p = p.field("door_open", 1 if payload.get("door_open") else 0)
        self._write(p)

    # ---------- actuator_status ----------
    def write_status(self, shipment_id: str, payload: dict) -> None:
        cooling = COOLING_MAP.get(str(payload.get("cooling_unit", "normal")), 1)
        p = (
            Point("actuator_status")
            .tag("shipment_id", shipment_id)
            .field("cooling_unit", cooling)
            .field("alarm", 1 if payload.get("alarm") else 0)
        )
        self._write(p)

    # ---------- events ----------
    def write_event(self, event: dict) -> None:
        p = (
            Point("events")
            .tag("shipment_id", str(event.get("shipment_id", "")))
            .tag("event_type", str(event.get("event_type", "")))
            .tag("severity", str(event.get("severity", "info")))
            .field("value", 1)
        )
        self._write(p)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass
