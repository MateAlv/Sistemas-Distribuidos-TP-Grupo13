FROM python:3.12-slim

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
