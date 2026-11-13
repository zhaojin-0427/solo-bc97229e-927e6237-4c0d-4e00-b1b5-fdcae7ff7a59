FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY shore_station /app/shore_station
COPY collector /app/collector

EXPOSE 8000 8001

# 默认启动采集器；docker-compose 会用各自 command 覆盖
CMD ["uvicorn", "collector.app:app", "--host", "0.0.0.0", "--port", "8001"]
