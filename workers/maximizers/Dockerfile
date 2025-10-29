FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY workers/ /app/workers/
COPY utils/ /app/utils/
COPY middleware/ /app/middleware/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app/workers/maximizers
