FROM python:3.12-slim

WORKDIR /workers
COPY workers/ /workers/
COPY utils/ /utils/
COPY middleware/ /middleware/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workers:/utils:/middleware

ENTRYPOINT ["python3"]
