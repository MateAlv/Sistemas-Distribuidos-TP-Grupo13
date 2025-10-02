FROM python:3.12-slim

WORKDIR /server

# Copy server code
COPY server/ .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared utils and install it
# COPY utils/ ./utils
# RUN pip install ./utils

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/server

ENTRYPOINT ["python3", "main.py"]
