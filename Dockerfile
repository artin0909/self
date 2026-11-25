FROM python:3.12-slim

WORKDIR /app

# نصب وابستگی‌های سیستم (برای ping و غیره)
RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY banner_bot.py .

# Volume در /data مانت می‌شه
ENV DATA_DIR=/data
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

CMD ["python", "banner_bot.py"]
