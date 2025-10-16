FROM python:3.12-slim

WORKDIR /workers/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY workers/maximizers/ ./maximizers/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workers/

WORKDIR /workers/maximizers
