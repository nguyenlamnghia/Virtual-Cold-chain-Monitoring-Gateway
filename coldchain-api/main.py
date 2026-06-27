"""
Coldchain REST API (M3 - Chu Tuấn Đức)
Đọc trạng thái/sự kiện từ Redis (do gateway M2 ghi) và gửi command xuống actuator qua MQTT.
"""
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import paho.mqtt.client as mqtt
import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 1) Cấu hình từ env
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
API_PORT = int(os.getenv("API_PORT", "8000"))

VALID_ACTIONS = {"increase_power", "normal_power", "alarm_on", "alarm_off"}
ACTION_TARGET = {
    "increase_power": "cooling_unit",
    "normal_power": "cooling_unit",
    "alarm_on": "alarm",
    "alarm_off": "alarm",
}

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# 2) Tài nguyên dùng chung (khởi tạo lúc startup)
# ---------------------------------------------------------------------------
clients: dict[str, Any] = {"redis": None, "mqtt": None}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    clients["redis"] = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    mqtt_client = mqtt.Client(client_id="coldchain-api")
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as exc:
        print(f"[api] Cảnh báo: chưa kết nối được MQTT ({exc}).", flush=True)
    clients["mqtt"] = mqtt_client
    print("[api] Khởi động xong (Redis + MQTT).", flush=True)
    yield
    # --- shutdown ---
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("[api] Đã đóng kết nối.", flush=True)

app = FastAPI(
    title="Coldchain Monitoring API",
    description="REST API tra cứu trạng thái shipment và gửi lệnh điều khiển (M3).",
    version="1.0.0",
    lifespan=lifespan,
)

def get_redis() -> redis.Redis:
    return clients["redis"]

# ---------------------------------------------------------------------------
# 3) Pydantic models
# ---------------------------------------------------------------------------
class CommandRequest(BaseModel):
    action: str = Field(
        ..., description="Một trong: increase_power | normal_power | alarm_on | alarm_off",
        examples=["alarm_off"],
    )
    reason: str = Field(
        "manual via API", description="Lý do phát lệnh (ghi vết)"
    )

class CommandResponse(BaseModel):
    status: str
    shipment_id: str
    published: dict

class HealthResponse(BaseModel):
    status: str
    redis: str

# ---------------------------------------------------------------------------
# 4) Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["system"])
def health():
    """Kiểm tra API sống và kết nối Redis."""
    r = get_redis()
    try:
        r.ping()
        redis_status = "ok"
    except Exception as exc:
        redis_status = f"error: {exc}"
    return HealthResponse(status="ok", redis=redis_status)

@app.get("/shipments", tags=["shipments"])
def list_shipments():
    """Danh sách shipment đang hoạt động (từ Redis set)."""
    r = get_redis()
    ids = sorted(r.smembers("shipments:index"))
    return {"count": len(ids), "shipments": ids}

@app.get("/shipments/{shipment_id}/state", tags=["shipments"])
def get_state(shipment_id: str):
    """Trạng thái hiện tại của một shipment."""
    r = get_redis()
    raw = r.get(f"shipment:{shipment_id}:state")
    if raw is None:
        raise HTTPException(status_code=404, detail=f"Không có state cho {shipment_id}")
    return json.loads(raw)

@app.get("/shipments/{shipment_id}/events", tags=["shipments"])
def get_events(shipment_id: str, limit: int = 50):
    """Danh sách sự kiện gần nhất của một shipment."""
    r = get_redis()
    raw_list = r.lrange(f"shipment:{shipment_id}:events", 0, limit - 1)
    events = [json.loads(item) for item in raw_list]
    return {"shipment_id": shipment_id, "count": len(events), "events": events}

@app.post("/shipments/{shipment_id}/command", response_model=CommandResponse,
          status_code=202, tags=["control"])
def send_command(shipment_id: str, body: CommandRequest):
    """Gửi lệnh thủ công xuống actuator của shipment."""
    if body.action not in VALID_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"action không hợp lệ. Cho phép: {sorted(VALID_ACTIONS)}",
        )
    command = {
        "shipment_id": shipment_id,
        "target": ACTION_TARGET[body.action],
        "action": body.action,
        "reason": body.reason,
        "issued_by": "api",
        "timestamp": now_iso(),
    }
    topic = f"coldchain/{shipment_id}/actuator/command"
    mqtt_client = clients["mqtt"]
    result = mqtt_client.publish(topic, json.dumps(command, ensure_ascii=False), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise HTTPException(status_code=503, detail="Không publish được command (MQTT lỗi).")
    print(f"[api] -> {topic} action={body.action}", flush=True)
    return CommandResponse(status="accepted", shipment_id=shipment_id, published=command)

@app.get("/", include_in_schema=False)
def root():
    return JSONResponse({"service": "coldchain-api", "docs": "/docs"})
