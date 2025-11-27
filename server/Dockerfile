FROM python:3.12-slim

# Install Docker CLI
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-24.0.5.tgz -o docker.tgz && \
    tar xzvf docker.tgz --strip 1 -C /usr/local/bin docker/docker && \
    rm docker.tgz && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /server

# Copiar y instalar dependencias primero (para mejor caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente después
COPY server/ .
COPY utils/ /server/utils/
COPY middleware/ /server/middleware/

# Copy shared utils and install it
# COPY utils/ ./utils
# RUN pip install ./utils

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/server

ENTRYPOINT ["python3", "main.py"]
