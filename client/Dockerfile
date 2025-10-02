FROM python:3.12-slim

WORKDIR /client

# Copy client code
COPY client/ .

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/client

ENTRYPOINT ["python3", "main.py"]
