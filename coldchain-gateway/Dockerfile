FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# -u: unbuffered để log hiện ngay trong `docker logs`
CMD ["python", "-u", "gateway.py"]
