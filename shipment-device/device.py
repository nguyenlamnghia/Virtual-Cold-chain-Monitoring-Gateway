"""
Virtual shipment device (M3 - Chu Tuấn Đức)
Sinh telemetry theo chu kỳ và publish lên MQTT.
Hỗ trợ 4 kịch bản qua biến SCENARIO: normal | temp_rising | door_open | battery_drain
"""
import json
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# 1) Cấu hình — đọc toàn bộ từ biến môi trường (không hardcode)
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
SHIPMENT_ID = os.getenv("SHIPMENT_ID", "shipment-01")
DEVICE_ID = os.getenv("DEVICE_ID", "device-01")
SCENARIO = os.getenv("SCENARIO", "normal")
INTERVAL = int(os.getenv("TELEMETRY_INTERVAL", "5"))

TELEMETRY_TOPIC = f"coldchain/{SHIPMENT_ID}/device/telemetry"

# Toạ độ gốc (Hà Nội) + lệch nhẹ mỗi shipment cho khác nhau trên bản đồ
BASE_LAT, BASE_LON = 21.0045, 105.8456

# ---------------------------------------------------------------------------
# 2) State nội bộ — giúp giá trị biến thiên mượt (random walk)
# ---------------------------------------------------------------------------
state = {
    "temperature": random.uniform(3.0, 5.0),
    "humidity": random.uniform(75.0, 85.0),
    "door_open": False,
    "battery_percent": 100.0,
    "online": True,        # battery_drain sẽ đặt False để ngừng publish
    "door_cycles": 0,      # đếm số chu kỳ cửa đã mở (cho kịch bản door_open)
}

def clamp(value, low, high):
    return max(low, min(high, value))

def now_iso():
    """Thời gian UTC chuẩn ISO-8601 có hậu tố Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# 3) Logic từng kịch bản — mỗi chu kỳ điều chỉnh state một chút
# ---------------------------------------------------------------------------
def step_normal():
    # Nhiệt dao động quanh 2..6°C, cửa đóng, pin giảm rất chậm
    state["temperature"] = clamp(state["temperature"] + random.uniform(-0.4, 0.4), 2.0, 6.0)
    state["door_open"] = False
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_temp_rising():
    # Nhiệt leo dần: mỗi chu kỳ +0.3..0.8°C, tới khi vượt xa ngưỡng 8°C
    state["temperature"] = clamp(state["temperature"] + random.uniform(0.3, 0.8), 2.0, 15.0)
    state["door_open"] = False
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_door_open():
    # Cửa mở liên tục; nhiệt nhích lên chậm do thất thoát khí lạnh
    state["door_open"] = True
    state["door_cycles"] += 1
    state["temperature"] = clamp(state["temperature"] + random.uniform(0.1, 0.4), 2.0, 12.0)
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_battery_drain():
    # Pin tụt nhanh; khi <= 0 thì device "mất sóng" (online=False)
    state["temperature"] = clamp(state["temperature"] + random.uniform(-0.3, 0.5), 2.0, 9.0)
    state["door_open"] = False
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(4.0, 7.0), 0.0, 100.0)
    if state["battery_percent"] <= 2.0:
        state["online"] = False  # ngừng publish → kích hoạt luật offline (R5) của M2

SCENARIO_STEPS = {
    "normal": step_normal,
    "temp_rising": step_temp_rising,
    "door_open": step_door_open,
    "battery_drain": step_battery_drain,
}

def build_telemetry():
    """Đóng gói telemetry đúng schema Mục 4.2 (kiểu dữ liệu chuẩn)."""
    jitter = random.uniform(-0.0005, 0.0005)
    return {
        "device_id": DEVICE_ID,
        "shipment_id": SHIPMENT_ID,
        "temperature": round(state["temperature"], 2),
        "humidity": round(state["humidity"] + random.uniform(-1.0, 1.0), 2),
        "door_open": bool(state["door_open"]),          # LUÔN boolean JSON
        "battery_percent": round(state["battery_percent"], 1),
        "latitude": round(BASE_LAT + jitter, 6),
        "longitude": round(BASE_LON + jitter, 6),
        "timestamp": now_iso(),
    }

# ---------------------------------------------------------------------------
# 4) MQTT callbacks (paho-mqtt 1.6.1 — chữ ký callback của API v1)
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[device] Đã kết nối broker {MQTT_BROKER}:{MQTT_PORT} "
              f"(shipment={SHIPMENT_ID}, scenario={SCENARIO})", flush=True)
    else:
        print(f"[device] Kết nối thất bại, rc={rc}", flush=True)

# ---------------------------------------------------------------------------
# 5) Vòng lặp chính
# ---------------------------------------------------------------------------
def main():
    step = SCENARIO_STEPS.get(SCENARIO, step_normal)
    if SCENARIO not in SCENARIO_STEPS:
        print(f"[device] Cảnh báo: SCENARIO '{SCENARIO}' không hợp lệ, dùng 'normal'.", flush=True)

    client = mqtt.Client(client_id=f"{DEVICE_ID}-pub")
    client.on_connect = on_connect
    # Tự động kết nối lại nếu broker chưa sẵn sàng / rớt mạng
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            break
        except Exception as exc:
            print(f"[device] Chưa kết nối được broker ({exc}), thử lại sau 2s...", flush=True)
            time.sleep(2)
    client.loop_start()

    try:
        while True:
            step()  # cập nhật state theo kịch bản
            if state["online"]:
                payload = build_telemetry()
                client.publish(TELEMETRY_TOPIC, json.dumps(payload, ensure_ascii=False), qos=0)
                print(f"[device] -> {TELEMETRY_TOPIC} "
                      f"temp={payload['temperature']} door={payload['door_open']} "
                      f"batt={payload['battery_percent']}", flush=True)
            else:
                print("[device] OFFLINE (pin cạn) — ngừng gửi telemetry.", flush=True)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print("[device] Dừng theo yêu cầu.", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
