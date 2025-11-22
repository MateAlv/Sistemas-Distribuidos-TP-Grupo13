FROM python:3.12-slim

# Install Docker CLI
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-24.0.5.tgz -o docker.tgz && \
    tar xzvf docker.tgz --strip 1 -C /usr/local/bin docker/docker && \
    rm docker.tgz && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY workers/ /app/workers/
COPY utils/ /app/utils/
COPY middleware/ /app/middleware/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app/workers/maximizers
