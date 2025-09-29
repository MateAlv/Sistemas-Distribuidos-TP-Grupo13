#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "" ]]; then
  echo "Uso: $0 <cantidad_clientes>" >&2
  exit 1
fi

OUTPUT_FILE="docker-compose.yaml"
CLIENT_NUMBER="$1"

echo "Nombre del archivo de salida: $OUTPUT_FILE"
echo "Cantidad de clientes: $CLIENT_NUMBER"

mkdir -p ./.data/dataset

# Cabecera + server
cat > "$OUTPUT_FILE" <<YAML
name: tp-distribuidos-grupo13
services:
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq_tests
    hostname: rabbitmq
    ports:
      - "5673:5672"
      - "15673:15672"
  server:
    container_name: server
    build: ./server
    entrypoint: ["python3", "/main.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - CLI_CLIENTS=${CLIENT_NUMBER}
    networks: [testing_net]
    volumes:
      - ./server/config.ini:/config.ini:ro
YAML

# Clients
for ((i=1; i<=CLIENT_NUMBER; i++)); do
  DATASET_DIR="./.data/client-${i}"
  if [[ -d "${DATASET_DIR}" ]]; then
    MOUNT_PATH="${DATASET_DIR}"
  else
    MOUNT_PATH="./.data/dataset"
  fi

  cat >> "$OUTPUT_FILE" <<YAML
  client${i}:
    container_name: client${i}
    build: ./client
    environment:
      - CLI_ID=${i}
      - CLI_DATA_DIR=/data
      - DATA_MODE=tree
      - SERVER_ADDRESS=server:12345
    networks: [testing_net]
    depends_on:
      server:
        condition: service_started
    volumes:
      - ${MOUNT_PATH}:/data:ro
      - ./client/config.ini:/config.ini:ro
YAML
done

# Redes
cat >> "$OUTPUT_FILE" <<'YAML'
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
YAML

echo "Archivo $OUTPUT_FILE generado con éxito."
