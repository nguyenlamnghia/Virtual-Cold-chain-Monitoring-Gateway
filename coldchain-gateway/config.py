"""Đọc toàn bộ cấu hình hệ thống từ biến môi trường (không hardcode).
Mọi file khác import: ``from config import CONFIG``.
"""
import os
from dataclasses import dataclass

def _get_str(key: str, default: str) -> str:
    val = os.environ.get(key)
    return val if val not in (None, "") else default

def _get_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

def _get_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

@dataclass(frozen=True)
class Config:
    # ----- MQTT -----
    mqtt_broker: str = _get_str("MQTT_BROKER", "mosquitto")
    mqtt_port: int = _get_int("MQTT_PORT", 1883)
    mqtt_keepalive: int = _get_int("MQTT_KEEPALIVE", 60)

    # ----- InfluxDB -----
    influx_url: str = _get_str("INFLUXDB_URL", "http://influxdb:8086")
    influx_org: str = _get_str("INFLUXDB_ORG", "hust")
    influx_bucket: str = _get_str("INFLUXDB_BUCKET", "coldchain")
    influx_token: str = _get_str("INFLUXDB_TOKEN", "")

    # ----- Redis -----
    redis_host: str = _get_str("REDIS_HOST", "redis")
    redis_port: int = _get_int("REDIS_PORT", 6379)

    # ----- Tham số nghiệp vụ / rule engine -----
    temp_max: float = _get_float("TEMP_MAX", 8.0)
    temp_violation_cycles: int = _get_int("TEMP_VIOLATION_CYCLES", 3)
    door_open_cycles: int = _get_int("DOOR_OPEN_CYCLES", 2)
    battery_min: float = _get_float("BATTERY_MIN", 20.0)
    offline_timeout: int = _get_int("OFFLINE_TIMEOUT", 30)

    # ----- Vận hành gateway -----
    offline_check_interval: int = _get_int("OFFLINE_CHECK_INTERVAL", 5)
    events_keep: int = _get_int("EVENTS_KEEP", 50)
    log_level: str = _get_str("LOG_LEVEL", "INFO")

    # ----- Hằng số topic (không đổi) -----
    topic_telemetry: str = "coldchain/+/device/telemetry"
    topic_status: str = "coldchain/+/actuator/status"

    def command_topic(self, shipment_id: str) -> str:
        return f"coldchain/{shipment_id}/actuator/command"

    def event_topic(self, shipment_id: str) -> str:
        return f"coldchain/{shipment_id}/gateway/event"

    def summary(self) -> str:
        return (
            f"MQTT={self.mqtt_broker}:{self.mqtt_port} | "
            f"Influx={self.influx_url}({self.influx_org}/{self.influx_bucket}) | "
            f"Redis={self.redis_host}:{self.redis_port} | "
            f"TEMP_MAX={self.temp_max} TEMP_CYCLES={self.temp_violation_cycles} "
            f"DOOR_CYCLES={self.door_open_cycles} BAT_MIN={self.battery_min} "
            f"OFFLINE={self.offline_timeout}s"
        )

# Singleton dùng chung cho toàn bộ gateway
CONFIG = Config()
