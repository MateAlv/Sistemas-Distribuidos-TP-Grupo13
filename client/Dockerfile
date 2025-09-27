# client/Dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY client/ /app/client/
COPY common/ /app/common/

RUN printf '#!/bin/sh\nexec python3 /app/client/main.py\n' > /client && chmod +x /client
