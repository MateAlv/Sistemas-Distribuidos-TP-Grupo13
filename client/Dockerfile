# client/Dockerfile
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /
COPY client/ /client/
COPY common/ /common/

ENTRYPOINT ["python3", "/client/main.py"]
