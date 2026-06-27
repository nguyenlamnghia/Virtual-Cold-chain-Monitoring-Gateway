"""
Virtual cooling actuator (M3 - Chu Tuấn Đức)
Subscribe command từ gateway/API, đổi trạng thái cooling_unit/alarm, publish status.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# 1) Cấu hình từ env
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
SHIPMENT_ID = os.getenv("SHIPMENT_ID", "shipment-01")
HEARTBEAT = int(os.getenv("STATUS_HEARTBEAT", "15"))

COMMAND_TOPIC = f"coldchain/{SHIPMENT_ID}/actuator/command"
STATUS_TOPIC = f"coldchain/{SHIPMENT_ID}/actuator/status"

# ---------------------------------------------------------------------------
# 2) Trạng thái nội bộ của actuator (bảo vệ bằng lock vì có 2 luồng)
# ---------------------------------------------------------------------------
state = {
    "cooling_unit": "normal",   # off | normal | high
    "alarm": False,
    "last_command_reason": "init",
}
state_lock = threading.Lock()

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def build_status():
    """Đóng gói status đúng schema Mục 4.4."""
    with state_lock:
        return {
            "shipment_id": SHIPMENT_ID,
            "cooling_unit": state["cooling_unit"],
            "alarm": bool(state["alarm"]),
            "last_command_reason": state["last_command_reason"],
            "timestamp": now_iso(),
        }

def publish_status(client):
    payload = build_status()
    client.publish(STATUS_TOPIC, json.dumps(payload, ensure_ascii=False), qos=0)
    print(f"[actuator] -> status cooling_unit={payload['cooling_unit']} "
          f"alarm={payload['alarm']}", flush=True)

# ---------------------------------------------------------------------------
# 3) Áp dụng command
# ---------------------------------------------------------------------------
def apply_command(cmd: dict) -> bool:
    """Trả True nếu trạng thái thay đổi / cần publish status."""
    action = cmd.get("action")
    reason = cmd.get("reason", "")
    issued_by = cmd.get("issued_by", "unknown")

    with state_lock:
        if action == "increase_power":
            state["cooling_unit"] = "high"
        elif action == "normal_power":
            state["cooling_unit"] = "normal"
        elif action == "alarm_on":
            state["alarm"] = True
        elif action == "alarm_off":
            state["alarm"] = False
        else:
            print(f"[actuator] Cảnh báo: action lạ '{action}', bỏ qua.", flush=True)
            return False
        state["last_command_reason"] = f"{reason} (by {issued_by})"

    print(f"[actuator] <- command action={action} reason='{reason}' by={issued_by}", flush=True)
    return True

# ---------------------------------------------------------------------------
# 4) MQTT callbacks (paho-mqtt 1.6.1)
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(COMMAND_TOPIC, qos=1)
        print(f"[actuator] Đã kết nối & subscribe {COMMAND_TOPIC}", flush=True)
        publish_status(client)  # status khởi tạo
    else:
        print(f"[actuator] Kết nối thất bại, rc={rc}", flush=True)

def on_message(client, userdata, msg):
    try:
        cmd = json.loads(msg.payload.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        print(f"[actuator] Command không phải JSON hợp lệ: {exc}", flush=True)
        return
    if apply_command(cmd):
        publish_status(client)  # phản hồi ngay sau khi đổi trạng thái

# ---------------------------------------------------------------------------
# 5) Heartbeat — publish status định kỳ trong luồng riêng
# ---------------------------------------------------------------------------
def heartbeat_loop(client):
    while True:
        time.sleep(HEARTBEAT)
        publish_status(client)

# ---------------------------------------------------------------------------
# 6) Main
# ---------------------------------------------------------------------------
def main():
    client = mqtt.Client(client_id=f"{SHIPMENT_ID}-actuator")
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except Exception as exc:
            print(f"[actuator] Chưa kết nối được broker ({exc}), thử lại sau 2s...", flush=True)
            time.sleep(2)

    # Luồng heartbeat chạy nền (daemon để tự tắt khi main thoát)
    threading.Thread(target=heartbeat_loop, args=(client,), daemon=True).start()

    print(f"[actuator] Sẵn sàng cho {SHIPMENT_ID} (heartbeat={HEARTBEAT}s)", flush=True)
    client.loop_forever()  # giữ kết nối + xử lý message, tự reconnect

if __name__ == "__main__":
    main()
