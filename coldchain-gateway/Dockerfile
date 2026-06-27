FROM python:3.11-slim

WORKDIR /app

# Cài dependencies trước để tận dụng layer cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn gateway
COPY . .

# -u: unbuffered để log hiện ngay trong `docker logs`
CMD ["python", "-u", "gateway.py"]
