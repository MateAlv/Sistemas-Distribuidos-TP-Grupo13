#!/usr/bin/env python3
import os, sys, signal, logging, base64, json
from collections import defaultdict
from configparser import ConfigParser

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.table_type import TableType

# ------------------------------
# Config / Logging
# ------------------------------
def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")
    return {
        "log_level": os.getenv("LOGGING_LEVEL", config["DEFAULT"].get("LOGGING_LEVEL", "INFO")),
        "rabbit_host": os.getenv("RABBIT_HOST", config["DEFAULT"].get("RABBIT_HOST", "rabbitmq")),
        "queue_menu": os.getenv("QUEUE_MENU_ITEMS", config["DEFAULT"].get("QUEUE_MENU_ITEMS", "menu_items")),
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
menu_items = {}   # item_id -> name
YEARS = {2024, 2025}
aggregated = defaultdict(list)  # key=(year, month) -> [ {item_id, name, quantity, revenue} ]

# ------------------------------
# Procesamiento
# ------------------------------
def handle_menu_items(batch_bytes: bytes):
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    if batch.header.table_type == TableType.MENU_ITEMS:
        for row in batch.rows:
            menu_items[row.item_id] = row.name
        logging.debug("action: handle_menu_items | result: success | rows:%s", len(batch.rows))

def handle_most_sold(batch_bytes: bytes):
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    if batch.header.table_type == TableType.MOST_SOLD_ITEMS:
        for row in batch.rows:
            if row.year in YEARS:
                key = (row.year, row.month)
                aggregated[key].append({
                    "item_id": row.item_id,
                    "quantity": getattr(row, "quantity", 0),
                    "revenue": getattr(row, "revenue", 0),
                })
        logging.debug("action: handle_most_sold | result: success | rows:%s", len(batch.rows))

def join_and_publish(rabbit_host: str, results_queue: str):
    """Hace el join: agrega el nombre a cada item y publica en results_queue"""
    output = []
    for (year, month), items in aggregated.items():
        enriched = []
        for item in items:
            name = menu_items.get(item["item_id"], "UNKNOWN")
            enriched.append({
                "item_id": item["item_id"],
                "name": name,
                "quantity": item["quantity"],
                "revenue": item["revenue"],
            })
        output.append({"year": year, "month": month, "items": enriched})
        logging.info("action: join_products | result: success | year:%s | month:%s | items:%s",
                     year, month, enriched)

    payload = json.dumps(output)
    producer = MessageMiddlewareQueue(rabbit_host, results_queue)
    producer.send(payload)
    producer.close()

    logging.info("action: publish_results | result: success | queue:%s | size:%s",
                 results_queue, len(output))

# ------------------------------
# Main
# ------------------------------
def main():
    cfg = initialize_config()
    initialize_log(cfg["log_level"])

    logging.debug("action: config | result: success | host:%s | queues:[%s,%s] | results:%s",
                  cfg["rabbit_host"], cfg["queue_menu"], cfg["queue_most_sold"], cfg["queue_results"])

    consumer_menu = MessageMiddlewareQueue(cfg["rabbit_host"], cfg["queue_menu"])
    consumer_most = MessageMiddlewareQueue(cfg["rabbit_host"], cfg["Q2_queue_most_sold"])

    def callback_menu(msg: str):
        try:
            batch_bytes = base64.b64decode(msg.encode())
            handle_menu_items(batch_bytes)
        except Exception as e:
            logging.error("action: callback_menu | result: fail | error:%s", e)

    def callback_most(msg: str):
        try:
            batch_bytes = base64.b64decode(msg.encode())
            handle_most_sold(batch_bytes)
        except Exception as e:
            logging.error("action: callback_most | result: fail | error:%s", e)

    def shutdown(signum, frame):
        logging.info("action: shutdown | result: start | reason:signal")
        try:
            consumer_menu.stop_consuming()
            consumer_most.stop_consuming()
            consumer_menu.close()
            consumer_most.close()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # 1. consumir menu_items
    consumer_menu.start_consuming(callback_menu)
    consumer_menu.close()
    logging.info("action: consume_menu | result: finished")

    # 2. consumir most_sold
    consumer_most.start_consuming(callback_most)
    consumer_most.close()
    logging.info("action: consume_most_sold | result: finished")

    # 3. join + publish
    join_and_publish(cfg["rabbit_host"], cfg["queue_results"])

if __name__ == "__main__":
    main()
