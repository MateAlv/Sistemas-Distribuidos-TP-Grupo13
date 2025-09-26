FROM python:3.12-slim

WORKDIR /

# Copiamos el script como ejecutable /client (entrypoint del compose)
COPY client.py /client
RUN chmod +x /client

# Entrypoint lo define compose