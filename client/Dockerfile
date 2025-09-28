FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /client
COPY . /client/
ENV PYTHONPATH=/client
ENTRYPOINT ["python3", "/client/main.py"]
