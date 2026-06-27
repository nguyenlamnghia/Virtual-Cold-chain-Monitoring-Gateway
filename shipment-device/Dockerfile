FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# python -u: log không bị buffer, thấy ngay khi docker logs
CMD ["python", "-u", "device.py"]
