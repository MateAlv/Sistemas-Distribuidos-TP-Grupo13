#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "" ]]; then
  echo "Uso: $0 <cantidad_clientes>" >&2
  exit 1
fi

OUTPUT_FILE="docker-compose-dev.yaml"
CLIENT_NUMBER="$1"

echo "Nombre del archivo de salida: $OUTPUT_FILE"
echo "Cantidad de clientes: $CLIENT_NUMBER"

mkdir -p ./.data/dataset

cat > "$OUTPUT_FILE" <<YAML
name: tp-distribuidos-grupo13
services:
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    hostname: rabbitmq
    ports:
      - "5673:5672"
      - "15673:15672"

  server:
    container_name: server
    build:
      context: .             # project root
      dockerfile: server/Dockerfile
    entrypoint: ["python3", "main.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - CLI_CLIENTS=${CLIENT_NUMBER}
    networks: [net]
    volumes:
      - ./server/config.ini:/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started

  # -------------------------
  # FILTERS
  # -------------------------

  filter_year:
    build:
      context: .             # project root
      dockerfile: workers/filter/Dockerfile
    container_name: filter_service
    command: ["python", "main.py", "--filter", "year"]
    volumes:
      - ./workers/filters/config_year.ini:/workers/filters/config_year.ini
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started

  filter_hour:
    build:
      context: .             # project root
      dockerfile: workers/filter/Dockerfile
    container_name: filter_service
    command: ["python", "main.py", "--filter", "hour"]
    volumes:
      - ./workers/filters/config_hour.ini:/workers/filters/config_hour.ini
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started

  filter_amount:
    build:
      context: .             # project root
      dockerfile: workers/filter/Dockerfile
    container_name: filter_service
    command: ["python", "main.py", "--filter", "amount"]
    volumes:
      - ./workers/filters/config_amount.ini:/workers/filters/config_amount.ini
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
      
  # -------------------------
  # JOINERS
  # -------------------------
  joiner_birthdates:
    build:
      context: .             # project root
      dockerfile: workers/joiners/Dockerfile
    container_name: joiner_birthdates
    command: ["/workers/joiners/join_birthdates.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/joiners/config.ini:/workers/joiners/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]

  joiner_items:
    build:
      context: .             # project root
      dockerfile: workers/joiners/Dockerfile
    container_name: joiner_items
    command: ["/workers/joiners/join_items.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/joiners/config.ini:/workers/joiners/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]

  # -------------------------
  # AGGREGATORS
  # -------------------------
  agg_products_qty_by_month:
    build:
      context: .             # project root
      dockerfile: workers/aggregators/Dockerfile
    container_name: agg_products_qty_by_month
    command: ["/workers/aggregators/agg_products_qty_by_month.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/aggregators/config.ini:/workers/aggregators/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]

  agg_products_revenue_by_month:
    build:
      context: .             # project root
      dockerfile: workers/aggregators/Dockerfile
    container_name: agg_products_revenue_by_month
    command: ["/workers/aggregators/agg_products_revenue_by_month.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/aggregators/config.ini:/workers/aggregators/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]

  agg_tpv_by_store_semester:
    build:
      context: .             # project root
      dockerfile: workers/aggregators/Dockerfile
    container_name: agg_tpv_by_store_semester
    command: ["/workers/aggregators/agg_tpv_by_store_semester.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/aggregators/config.ini:/workers/aggregators/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]

  agg_purchases_by_client_store:
    build:
      context: .             # project root
      dockerfile: workers/aggregators/Dockerfile
    container_name: agg_purchases_by_client_store
    command: ["/workers/aggregators/agg_purchases_by_client_store.py"]
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - RABBIT_HOST=rabbitmq
    volumes:
      - ./workers/aggregators/config.ini:/workers/aggregators/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
    depends_on:
      rabbitmq:
        condition: service_started
    networks: [net]
YAML

# Clients
for ((i=1; i<=CLIENT_NUMBER; i++)); do
  DATASET_DIR="./.data/client-${i}"
  if [[ -d "${DATASET_DIR}" ]]; then
    MOUNT_PATH="${DATASET_DIR}"
  else
    MOUNT_PATH="./.data/"
  fi

  cat >> "$OUTPUT_FILE" <<YAML
  client${i}:
    container_name: client${i}
    build:
      context: .             # project root
      dockerfile: client/Dockerfile
    environment:
      - CLI_ID=${i}
      - CLI_DATA_DIR=/data
      - DATA_MODE=tree
      - SERVER_ADDRESS=server:12345
    networks: [net]
    depends_on:
      - rabbitmq
      - server:
        condition: service_started
    volumes:
      - ${MOUNT_PATH}:/data:ro
      - ./client/config.ini:/config.ini:ro
      - ./utils:/server/utils:ro
      - ./middleware:/server/middleware:ro
YAML
done

# Redes
cat >> "$OUTPUT_FILE" <<'YAML'
networks:
  net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
YAML

echo "Archivo $OUTPUT_FILE generado con éxito."
