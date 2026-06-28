"""
Virtual shipment device (M3 - Chu Tuấn Đức)
Sinh telemetry theo chu kỳ và publish lên MQTT.
Hỗ trợ 4 kịch bản qua biến SCENARIO: normal | temp_rising | door_open | battery_drain
"""
import json
import os
import random
import threading
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
DOOR_OPEN_CYCLES = max(1, int(os.getenv("DOOR_OPEN_CYCLES", "2")))
TEMP_MAX = float(os.getenv("TEMP_MAX", "8"))
TEMP_VIOLATION_CYCLES = max(1, int(os.getenv("TEMP_VIOLATION_CYCLES", "3")))
OVERTEMP_MIN_CYCLES = max(
    TEMP_VIOLATION_CYCLES, int(os.getenv("OVERTEMP_MIN_CYCLES", "5"))
)
OVERTEMP_MAX_CYCLES = max(
    OVERTEMP_MIN_CYCLES, int(os.getenv("OVERTEMP_MAX_CYCLES", "8"))
)

TELEMETRY_TOPIC = f"coldchain/{SHIPMENT_ID}/device/telemetry"
ACTUATOR_STATUS_TOPIC = f"coldchain/{SHIPMENT_ID}/actuator/status"

# Toạ độ gốc (Hà Nội) + lệch nhẹ mỗi shipment cho khác nhau trên bản đồ
BASE_LAT, BASE_LON = 21.0045, 105.8456

# temp_rising xuất hiện theo từng đợt thay vì tăng liên tục.
# Với chu kỳ mặc định 5 giây, khoảng chờ này tương đương 25–75 giây.
TEMP_RISE_WAIT_MIN_CYCLES = 5
TEMP_RISE_WAIT_MAX_CYCLES = 15

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
    "cooling_unit": "normal",
    "overtemp_cycles": 0,
    "overtemp_target_cycles": random.randint(
        OVERTEMP_MIN_CYCLES, OVERTEMP_MAX_CYCLES
    ),
    "cooling_recovery_cycles": 0,
    "temp_rise_active": False,
    "temp_rise_wait_cycles": random.randint(
        TEMP_RISE_WAIT_MIN_CYCLES, TEMP_RISE_WAIT_MAX_CYCLES
    ),
    "door_open_active": True,
    "door_reopen_wait_cycles": 0,
}
state_lock = threading.Lock()

DOOR_HEAT_RAMP_CYCLES = 8

def clamp(value, low, high):
    return max(low, min(high, value))

def apply_temperature_delta(natural_delta, low, high):
    """Apply the natural heat load together with actuator cooling feedback."""
    cooling_unit = state["cooling_unit"]
    temperature = state["temperature"]

    if cooling_unit == "high" and temperature > TEMP_MAX:
        if state["overtemp_cycles"] < state["overtemp_target_cycles"]:
            # Quán tính nhiệt: giữ nhiệt trên ngưỡng đúng cửa sổ random 5–8
            # chu kỳ để gateway chắc chắn quan sát được streak >= 3.
            next_temperature = clamp(
                temperature + natural_delta,
                TEMP_MAX + 0.1,
                high,
            )
        else:
            # Hết cửa sổ tăng nhiệt thì cooling hạ từ từ, tránh nhảy thẳng từ
            # nhiệt độ đỉnh xuống dưới ngưỡng chỉ trong một telemetry.
            state["cooling_recovery_cycles"] += 1
            next_temperature = clamp(
                temperature - random.uniform(0.3, 0.6), low, high
            )
    elif cooling_unit == "high":
        next_temperature = clamp(
            temperature + natural_delta - random.uniform(0.9, 1.2), low, high
        )
    else:
        delta = natural_delta
        if cooling_unit == "off":
            delta += random.uniform(0.2, 0.5)
        next_temperature = clamp(temperature + delta, low, high)

    # Telemetry được làm tròn 2 chữ số; giữ cách ngưỡng ít nhất 0.1°C để một
    # chu kỳ nội bộ vượt ngưỡng không bị gateway đọc thành đúng TEMP_MAX.
    if next_temperature > TEMP_MAX:
        next_temperature = clamp(max(next_temperature, TEMP_MAX + 0.1), low, high)

    state["temperature"] = next_temperature
    if next_temperature > TEMP_MAX and state["cooling_recovery_cycles"] == 0:
        state["overtemp_cycles"] += 1
    elif cooling_unit != "high" and state["overtemp_cycles"] > 0:
        reset_overtemp_window()

def reset_overtemp_window():
    state["overtemp_cycles"] = 0
    state["cooling_recovery_cycles"] = 0
    state["overtemp_target_cycles"] = random.randint(
        OVERTEMP_MIN_CYCLES, OVERTEMP_MAX_CYCLES
    )

def schedule_next_temp_rise():
    state["temp_rise_active"] = False
    state["temp_rise_wait_cycles"] = random.randint(
        TEMP_RISE_WAIT_MIN_CYCLES, TEMP_RISE_WAIT_MAX_CYCLES
    )

def schedule_next_door_open():
    state["door_open_active"] = False
    state["door_reopen_wait_cycles"] = random.randint(
        TEMP_RISE_WAIT_MIN_CYCLES, TEMP_RISE_WAIT_MAX_CYCLES
    )

def stable_temperature_delta():
    """Small random walk while a shipment is stable between incidents."""
    if state["temperature"] > 6.0:
        return -random.uniform(0.2, 0.5)
    if state["temperature"] < 4.0:
        return random.uniform(0.1, 0.3)
    return random.uniform(-0.2, 0.2)

def now_iso():
    """Thời gian UTC chuẩn ISO-8601 có hậu tố Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# 3) Logic từng kịch bản — mỗi chu kỳ điều chỉnh state một chút
# ---------------------------------------------------------------------------
def step_normal():
    # Nhiệt dao động quanh 2..6°C, cửa đóng, pin giảm rất chậm
    apply_temperature_delta(random.uniform(-0.4, 0.4), 2.0, 6.0)
    state["door_open"] = False
    state["door_cycles"] = 0
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_temp_rising():
    if not state["temp_rise_active"]:
        if state["temp_rise_wait_cycles"] > 0:
            state["temp_rise_wait_cycles"] -= 1

        # Giữa hai đợt sự cố, đưa nhiệt độ về vùng 4–6°C và dao động nhẹ.
        apply_temperature_delta(stable_temperature_delta(), 2.0, 7.5)

        if state["temp_rise_wait_cycles"] == 0:
            state["temp_rise_active"] = True
            print("[device] Kích hoạt đợt temp_rising mới.", flush=True)
    else:
        # Trong đợt sự cố, nhiệt leo +0.3..0.8°C mỗi chu kỳ.
        apply_temperature_delta(random.uniform(0.3, 0.8), 2.0, 15.0)

    state["door_open"] = False
    state["door_cycles"] = 0
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_door_open():
    if not state["door_open_active"]:
        if state["door_reopen_wait_cycles"] > 0:
            state["door_reopen_wait_cycles"] -= 1
            state["door_open"] = False
            state["door_cycles"] = 0
            apply_temperature_delta(stable_temperature_delta(), 2.0, 7.5)
            state["battery_percent"] = clamp(
                state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0
            )
            return
        state["door_open_active"] = True
        print("[device] Kích hoạt đợt door_open mới.", flush=True)

    # Cửa mới mở chỉ gây tải nhiệt nhỏ. Sau ngưỡng cảnh báo mở lâu, không khí
    # ấm xâm nhập nhiều hơn nên mức tăng nhiệt tăng dần rồi đạt trần.
    state["door_open"] = True
    state["door_cycles"] += 1
    if state["door_cycles"] <= DOOR_OPEN_CYCLES:
        heat_load = random.uniform(0.03, 0.12)
    else:
        long_open_cycles = state["door_cycles"] - DOOR_OPEN_CYCLES
        heat_ramp = min(long_open_cycles / DOOR_HEAT_RAMP_CYCLES, 1.0)
        heat_load = random.uniform(0.20, 0.35) + (0.35 * heat_ramp)
    apply_temperature_delta(heat_load, 2.0, 15.0)
    state["battery_percent"] = clamp(state["battery_percent"] - random.uniform(0.0, 0.1), 0.0, 100.0)

def step_battery_drain():
    # Pin tụt nhanh; khi <= 0 thì device "mất sóng" (online=False)
    apply_temperature_delta(random.uniform(-0.3, 0.5), 2.0, 9.0)
    state["door_open"] = False
    state["door_cycles"] = 0
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
        client.subscribe(ACTUATOR_STATUS_TOPIC, qos=0)
        print(f"[device] Đã kết nối broker {MQTT_BROKER}:{MQTT_PORT} "
              f"(shipment={SHIPMENT_ID}, scenario={SCENARIO}) "
              f"& subscribe {ACTUATOR_STATUS_TOPIC}", flush=True)
    else:
        print(f"[device] Kết nối thất bại, rc={rc}", flush=True)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        cooling_unit = str(payload.get("cooling_unit", ""))
    except (AttributeError, TypeError, ValueError, UnicodeDecodeError) as exc:
        print(f"[device] Status actuator không hợp lệ: {exc}", flush=True)
        return

    if cooling_unit not in {"off", "normal", "high"}:
        print(f"[device] Bỏ qua cooling_unit lạ '{cooling_unit}'.", flush=True)
        return

    with state_lock:
        previous = state["cooling_unit"]
        if previous != cooling_unit:
            state["cooling_unit"] = cooling_unit
            if previous == "high" and cooling_unit == "normal":
                reset_overtemp_window()
                if SCENARIO == "temp_rising":
                    schedule_next_temp_rise()
                elif SCENARIO == "door_open":
                    schedule_next_door_open()
        wait_cycles = state["temp_rise_wait_cycles"]
        door_wait_cycles = state["door_reopen_wait_cycles"]
        overtemp_target = state["overtemp_target_cycles"]

    if previous != cooling_unit:
        print(f"[device] <- cooling feedback {previous} -> {cooling_unit}", flush=True)
        if cooling_unit == "high":
            print(
                f"[device] Giữ vượt ngưỡng mục tiêu "
                f"{overtemp_target} chu kỳ (event từ chu kỳ "
                f"{TEMP_VIOLATION_CYCLES}).",
                flush=True,
            )
        if SCENARIO == "temp_rising" and previous == "high" and cooling_unit == "normal":
            print(
                f"[device] Đợt sau sẽ bắt đầu sau {wait_cycles} chu kỳ.",
                flush=True,
            )
        elif SCENARIO == "door_open" and previous == "high" and cooling_unit == "normal":
            print(
                f"[device] Cửa đóng ổn định {door_wait_cycles} chu kỳ "
                "trước đợt mở tiếp theo.",
                flush=True,
            )

# ---------------------------------------------------------------------------
# 5) Vòng lặp chính
# ---------------------------------------------------------------------------
def main():
    step = SCENARIO_STEPS.get(SCENARIO, step_normal)
    if SCENARIO not in SCENARIO_STEPS:
        print(f"[device] Cảnh báo: SCENARIO '{SCENARIO}' không hợp lệ, dùng 'normal'.", flush=True)

    client = mqtt.Client(client_id=f"{DEVICE_ID}-pub")
    client.on_connect = on_connect
    client.on_message = on_message
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
            with state_lock:
                step()  # cập nhật state theo kịch bản + feedback actuator
                online = state["online"]
                payload = build_telemetry() if online else None
                cooling_unit = state["cooling_unit"]
                if state["cooling_recovery_cycles"] > 0:
                    overtemp_phase = (
                        f" cooling_down={state['cooling_recovery_cycles']}"
                        f" after_overtemp={state['overtemp_target_cycles']}"
                    )
                elif state["overtemp_cycles"] > 0:
                    overtemp_phase = (
                        f" overtemp={state['overtemp_cycles']}/"
                        f"{state['overtemp_target_cycles']}"
                    )
                else:
                    overtemp_phase = ""
                if SCENARIO == "temp_rising":
                    temp_phase = (
                        "rising"
                        if state["temp_rise_active"]
                        else f"waiting:{state['temp_rise_wait_cycles']}"
                    )
                else:
                    temp_phase = (
                        (
                            f"door_open:{state['door_cycles'] * INTERVAL}s"
                            if state["door_open_active"]
                            else f"stable:{state['door_reopen_wait_cycles']}"
                        )
                        if SCENARIO == "door_open"
                        else SCENARIO
                    )
                temp_phase += overtemp_phase
            if payload is not None:
                client.publish(TELEMETRY_TOPIC, json.dumps(payload, ensure_ascii=False), qos=0)
                print(f"[device] -> {TELEMETRY_TOPIC} "
                      f"temp={payload['temperature']} door={payload['door_open']} "
                      f"batt={payload['battery_percent']} cooling={cooling_unit} "
                      f"phase={temp_phase}", flush=True)
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
