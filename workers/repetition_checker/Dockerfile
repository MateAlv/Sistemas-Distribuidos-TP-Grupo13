FROM python:3.12-slim

WORKDIR /workers/

# Copiar y instalar dependencias primero (para mejor caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y curl

# Copiar el código fuente después
COPY workers/repetition_checker/ ./repetition_checker/
COPY utils/ /workers/utils/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workers/

WORKDIR /workers/repetition_checker