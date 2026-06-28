# Virtual Cold-chain Monitoring Gateway

Hệ thống giám sát chuỗi cung ứng lạnh ảo, bao gồm các thành phần:
- **M1 (Infra):** Mosquitto, Redis, InfluxDB, Grafana.
- **M2 (Gateway):** Ứng dụng xử lý luồng sự kiện MQTT, đánh giá Rule Engine và ghi InfluxDB/Redis.
- **M3 (Edge & API):** Cảm biến ảo (Shipment Device), Actuator ảo (Cooling Actuator) và REST API.

## Cách chạy hệ thống

Hệ thống được thiết kế để chạy toàn bộ với một lệnh Docker Compose duy nhất:

```bash
docker compose up -d --build
```

Sau khi chạy, bạn có thể truy cập:
- **Grafana:** `http://localhost:3000` (tài khoản lấy từ
  `GF_SECURITY_ADMIN_USER` / `GF_SECURITY_ADMIN_PASSWORD` trong `.env`)
- **REST API (Swagger UI):** `http://localhost:8000/docs`

## Các kịch bản giả lập (M3)

- `shipment-device-01`: Kịch bản `temp_rising` (Nhiệt độ tăng dần).
- `shipment-device-02`: Kịch bản `door_open` (cửa mở càng lâu thì tải nhiệt
  tăng dần; cooling actuator phản hồi khi vượt ngưỡng).
- `shipment-device-03`: Kịch bản `battery_drain` (Pin tụt nhanh dẫn đến offline).

## Cấu trúc thư mục

- `coldchain-gateway/`: Source code của Gateway (M2).
- `shipment-device/`: Source code của Virtual Sensor (M3).
- `cooling-actuator/`: Source code của Virtual Actuator (M3).
- `coldchain-api/`: Source code của REST API (M3).
- `mosquitto/`: Cấu hình cho Mosquitto MQTT broker.
- `grafana/`: Cấu hình provisioning dashboards & datasources.
