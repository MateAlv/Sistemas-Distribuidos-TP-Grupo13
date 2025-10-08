#!/usr/bin/env python3
import os, sys, signal, logging, base64, json
from collections import defaultdict, Counter
from configparser import ConfigParser

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.table_type import TableType

# ------------------------------
# Constantes
# ------------------------------
TOP3_CLIENTS_QUEUE_PATTERN = "top_3_clients_for_{}"

# ------------------------------
# Config / Logging
# ------------------------------
def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")
    return {
        "log_level": os.getenv("LOGGING_LEVEL", config["DEFAULT"].get("LOGGING_LEVEL", "INFO")),
        "rabbit_host": os.getenv("RABBIT_HOST", config["DEFAULT"].get("RABBIT_HOST", "rabbitmq")),
        "queue_tx": os.getenv("QUEUE_TRANSACTIONS", config["DEFAULT"].get("QUEUE_TRANSACTIONS", "transactions_sum_by_client")),
        "queue_users": os.getenv("QUEUE_USERS", config["DEFAULT"].get("QUEUE_USERS", "users")),
    }

def initialize_log(level_str: str):
    level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ------------------------------
# Estado en memoria
# ------------------------------
top_clients = defaultdict(Counter)  # store_id -> Counter(client_id)
birthdays = {}                      # user_id -> birth_date

# ------------------------------
# Procesamiento
# ------------------------------
def handle_transactions(batch_bytes: bytes):
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    if batch.header.table_type == TableType.TRANSACTIONS:
        for row in batch.rows:
            top_clients[row.store_id][row.user_id] += row.amount if hasattr(row, "amount") else 1
        logging.debug("action: handle_transactions | result: success | rows:%s", len(batch.rows))

def handle_users(batch_bytes: bytes):
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    if batch.header.table_type == TableType.USERS:
        for row in batch.rows:
            birthdays[row.user_id] = row.birth_date
        logging.debug("action: handle_users | result: success | rows:%s", len(batch.rows))

def join_and_publish(rabbit_host: str):
    """Para cada sucursal publica en una cola distinta top_3_clients_for_{sucursal}"""
    for store_id, counter in top_clients.items():
        top3 = counter.most_common(3)
        enriched = []
        for client_id, cnt in top3:
            enriched.append({
                "client_id": client_id,
                "purchases": cnt,
                "birth_date": str(birthdays.get(client_id))
            })

        payload = json.dumps({"store_id": store_id, "top_clients": enriched})
        queue_name = TOP3_CLIENTS_QUEUE_PATTERN.format(store_id)

        producer = MessageMiddlewareQueue(rabbit_host, queue_name)
        producer.send(payload)
        producer.close()

        logging.info(
            "action: publish_results | result: success | store_id:%s | queue:%s | top_clients:%s",
            store_id, queue_name, enriched
        )

# ------------------------------
# Main
# ------------------------------
def main():
    cfg = initialize_config()
    initialize_log(cfg["log_level"])

    logging.debug("action: config | result: success | host:%s | queues:[%s,%s]",
                  cfg["rabbit_host"], cfg["queue_tx"], cfg["queue_users"])

    consumer_tx = MessageMiddlewareQueue(cfg["rabbit_host"], cfg["queue_tx"])
    consumer_users = MessageMiddlewareQueue(cfg["rabbit_host"], cfg["queue_users"])

    def callback_tx(msg: str):
        try:
            batch_bytes = base64.b64decode(msg.encode())
            handle_transactions(batch_bytes)
        except Exception as e:
            logging.error("action: callback_tx | result: fail | error:%s", e)

    def callback_users(msg: str):
        try:
            batch_bytes = base64.b64decode(msg.encode())
            handle_users(batch_bytes)
        except Exception as e:
            logging.error("action: callback_users | result: fail | error:%s", e)

    # graceful shutdown
    def shutdown(signum, frame):
        logging.info("action: shutdown | result: start | reason:signal")
        try:
            consumer_tx.stop_consuming()
            consumer_users.stop_consuming()
            consumer_tx.close()
            consumer_users.close()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # 1. consumir todos los tx
    consumer_tx.start_consuming(callback_tx)
    consumer_tx.close()
    logging.info("action: consume_tx | result: finished")

    # 2. consumir todos los users
    consumer_users.start_consuming(callback_users)
    consumer_users.close()
    logging.info("action: consume_users | result: finished")

    # 3. join + publish por sucursal
    join_and_publish(cfg["rabbit_host"])

if __name__ == "__main__":
    main()
