FROM python:3.12-slim

WORKDIR /client

# Copiar y instalar dependencias primero (para mejor caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente después
COPY client/ .
COPY utils/ /client/utils/
COPY middleware/ /client/middleware/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/client

ENTRYPOINT ["python3", "main.py"]
