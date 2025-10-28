FROM python:3.12-slim

WORKDIR /workers/

# Copiar y instalar dependencias primero (para mejor caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente después
COPY workers/filter/ ./filter/
COPY utils/ /workers/utils/
COPY middleware/ /workers/middleware/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workers/

WORKDIR /workers/filter
