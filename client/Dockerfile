FROM python:3.12-slim

WORKDIR /client

# Copy client code
COPY client/ .

# Copy shared utils and install it
# COPY utils/ ./utils
# RUN pip install ./utils

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/client

ENTRYPOINT ["python3", "main.py"]
