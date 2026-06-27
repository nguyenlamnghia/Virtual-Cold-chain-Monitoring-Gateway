"""Entrypoint coldchain-gateway: MQTT loop + offline checker.
Viết cho paho-mqtt 1.6.1 (chữ ký callback cũ: on_connect(client, userdata, flags, rc)).
"""
import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

from config import CONFIG
from state import StateStore
from rules import RuleEngine
from influx_writer import InfluxWriter

logging.basicConfig(
    level=getattr(logging, CONFIG.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("gateway")

class ColdchainGateway:
    def __init__(self):
        self.cfg = CONFIG
        self.store = StateStore()
        self.rules = RuleEngine(CONFIG)
        self.influx = InfluxWriter(CONFIG)
        # paho-mqtt 1.6.1: khởi tạo Client không cần CallbackAPIVersion
        self.client = mqtt.Client(client_id="coldchain-gateway")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self._stop = threading.Event()

    # ---------- MQTT callbacks (chữ ký paho 1.6.1) ----------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("Đã kết nối MQTT broker %s:%s",
                     self.cfg.mqtt_broker, self.cfg.mqtt_port)
            client.subscribe([(self.cfg.topic_telemetry, 0),
                              (self.cfg.topic_status, 0)])
            log.info("Subscribed: %s | %s",
                     self.cfg.topic_telemetry, self.cfg.topic_status)
        else:
            log.error("Kết nối MQTT thất bại, rc=%s", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            log.warning("Payload không phải JSON hợp lệ (%s): %s", msg.topic, e)
            return

        shipment_id = self._shipment_from_topic(msg.topic)
        if not shipment_id:
            log.warning("Không trích được shipment_id từ topic: %s", msg.topic)
            return

        if msg.topic.endswith("/device/telemetry"):
            self._handle_telemetry(shipment_id, payload)
        elif msg.topic.endswith("/actuator/status"):
            self._handle_status(shipment_id, payload)

    # ---------- Xử lý telemetry ----------
    def _handle_telemetry(self, shipment_id, payload):
        self.store.update_from_telemetry(shipment_id, payload)
        self.influx.write_telemetry(shipment_id, payload)

        st = self.store.get(shipment_id)
        commands, events = self.rules.evaluate(st)
        self._dispatch(shipment_id, commands, events)
        self.store.sync_redis(shipment_id)

    # ---------- Xử lý status ----------
    def _handle_status(self, shipment_id, payload):
        self.store.update_from_status(shipment_id, payload)
        self.influx.write_status(shipment_id, payload)
        self.store.sync_redis(shipment_id)

    # ---------- Phát command + event ----------
    def _dispatch(self, shipment_id, commands, events):
        for cmd in commands:
            self.client.publish(
                self.cfg.command_topic(shipment_id),
                json.dumps(cmd, ensure_ascii=False), qos=1,
            )
            log.info("-> command %s/%s", cmd["target"], cmd["action"])
        for ev in events:
            self.client.publish(
                self.cfg.event_topic(shipment_id),
                json.dumps(ev, ensure_ascii=False), qos=1,
            )
            self.influx.write_event(ev)
            self.store.push_event(shipment_id, ev)
            log.info("-> event %s (%s)", ev["event_type"], ev["severity"])

    # ---------- Thread offline checker (R5) ----------
    def _offline_loop(self):
        while not self._stop.is_set():
            now = time.time()
            for st in self.store.snapshot():
                events = self.rules.check_offline(st, now)
                if events:
                    self._dispatch(st.shipment_id, [], events)
                    self.store.sync_redis(st.shipment_id)
            self._stop.wait(self.cfg.offline_check_interval)

    @staticmethod
    def _shipment_from_topic(topic: str):
        # coldchain/<shipment_id>/device/telemetry -> phần tử thứ 2
        parts = topic.split("/")
        return parts[1] if len(parts) >= 2 else None

    # ---------- Chạy ----------
    def run(self):
        log.info("Khởi động coldchain-gateway")
        log.info("CONFIG: %s", self.cfg.summary())
        self.client.connect(self.cfg.mqtt_broker, self.cfg.mqtt_port,
                            self.cfg.mqtt_keepalive)
        t = threading.Thread(target=self._offline_loop, daemon=True)
        t.start()
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            log.info("Nhận Ctrl-C, đang dừng...")
        finally:
            self._stop.set()
            self.influx.close()
            self.client.disconnect()

if __name__ == "__main__":
    ColdchainGateway().run()
