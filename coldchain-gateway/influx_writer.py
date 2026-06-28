"""InfluxDB writer using the schema shared with the Grafana dashboard."""

import json
import logging
from typing import Dict

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

try:
    from .config import CONFIG
except ImportError:
    from config import CONFIG


log = logging.getLogger("gateway.influx")
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
        except Exception as exc:
            log.warning("InfluxDB write failed: %s", exc)

    def write_telemetry(self, shipment_id: str, payload: Dict) -> None:
        point = Point("telemetry").tag("shipment_id", shipment_id).tag(
            "device_id", str(payload.get("device_id", ""))
        )
        for field_name in ("temperature", "humidity", "battery_percent"):
            value = payload.get(field_name)
            if value is not None:
                point = point.field(field_name, float(value))
        point = point.field("door_open", 1 if payload.get("door_open") else 0)
        self._write(point)

    def write_status(self, shipment_id: str, payload: Dict) -> None:
        cooling = COOLING_MAP.get(str(payload.get("cooling_unit", "normal")), 1)
        point = (
            Point("actuator_status")
            .tag("shipment_id", shipment_id)
            .field("cooling_unit", cooling)
            .field("alarm", 1 if payload.get("alarm") else 0)
        )
        self._write(point)

    def write_event(self, event: Dict) -> None:
        details = event.get("details") or {}
        message = str(event.get("message", ""))
        details_text = json.dumps(details, ensure_ascii=False, sort_keys=True)
        point = (
            Point("events")
            .tag("shipment_id", str(event.get("shipment_id", "")))
            .tag("event_type", str(event.get("event_type", "")))
            .tag("severity", str(event.get("severity", "info")))
            .field("value", 1)
            .field("message", message)
            .field("details", details_text)
            .field("event_info", f"{message} | {details_text}")
            .field("event_timestamp", str(event.get("timestamp", "")))
        )
        self._write(point)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass
