#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "" || "${2:-}" == "" ]]; then
  echo "Uso: $0 <archivo_salida> <cantidad_clientes>" >&2
  exit 1
fi

OUTPUT_FILE="$1"
CLIENT_NUMBER="$2"

echo "Nombre del archivo de salida: $OUTPUT_FILE"
echo "Cantidad de clientes: $CLIENT_NUMBER"

cat > "$OUTPUT_FILE" <<YAML
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - CLI_CLIENTS=${CLIENT_NUMBER}
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
YAML

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
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=${i}
      - DATA_MODE=tree
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ${MOUNT_PATH}:/data:ro
      - ./client/config.yaml:/config.yaml
YAML
done

cat >> "$OUTPUT_FILE" <<'YAML'
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
YAML

echo "✓ Compose generado en: ${OUTPUT_FILE}"
