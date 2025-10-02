FROM python:3.12-slim

WORKDIR /workers
COPY workers/filter/ .

COPY requirements.txt .
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workers
