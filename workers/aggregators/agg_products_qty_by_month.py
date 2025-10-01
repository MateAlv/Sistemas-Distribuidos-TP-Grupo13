#!/usr/bin/env python3
import os
import sys
import signal
import logging
import base64
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, date

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.table_type import TableType
from utils.file_utils.process_table import TransactionsItemsProcessRow
from utils.file_utils.process_chunk import ProcessChunk, ProcessChunkHeader

# ------------------------------
# Constantes (overridable por ENV o config.ini)
# ------------------------------
DEFAULT_INPUT_QUEUE  = "transaction_items"             # batches con TransactionsItemsProcessRow
DEFAULT_OUTPUT_QUEUE = "product_quantity_by_month"     # batches agregados por (year, month, item_id)
YEARS = {2024, 2025}

# ------------------------------
# Config / Logging
# ------------------------------
def initialize_config():
    cfg = ConfigParser(os.environ)
    cfg.read("config.ini")
    return {
        "log_level": os.getenv("LOGGING_LEVEL", cfg["DEFAULT"].get("LOGGING_LEVEL", "INFO")),
        "rabbit_host": os.getenv("RABBIT_HOST", cfg["DEFAULT"].get("RABBIT_HOST", "rabbitmq")),
        "queue_in": os.getenv("QUEUE_IN", cfg["DEFAULT"].get("QUEUE_IN", DEFAULT_INPUT_QUEUE)),
        "queue_out": os.getenv("QUEUE_OUT", cfg["DEFAULT"].get("QUEUE_OUT", DEFAULT_OUTPUT_QUEUE)),
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
# agg[(year, month, item_id)] = sum_quantity
agg = defaultdict(int)
_last_client_id = 0  # usamos el último client_id visto para el header de salida

def _parse_year_month(created_at) -> tuple[int, int]:
    if isinstance(created_at, date) and not isinstance(created_at, datetime):
        return created_at.year, created_at.month
    if isinstance(created_at, datetime):
        return created_at.year, created_at.month
    # string tipo "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.fromisoformat(created_at)
    except ValueError:
        dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
    return dt.year, dt.month

# ------------------------------
# Procesamiento
# ------------------------------
def handle_batch(batch_bytes: bytes):
    global _last_client_id
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    _last_client_id = batch.header.client_id or _last_client_id

    if batch.header.table_type != TableType.TRANSACTIONS_ITEMS:
        logging.debug("action: handle_batch | result: ignored | kind:%s", batch.header.table_type.name)
        return

    processed = 0
    for row in batch.rows:
        item_id = getattr(row, "item_id", None)
        quantity = getattr(row, "quantity", None)
        created_at = getattr(row, "created_at", None)
        if item_id is None or quantity is None or created_at is None:
            continue

        year, month = _parse_year_month(created_at)
        if year not in YEARS:
            continue

        try:
            q = int(quantity)
        except Exception:
            q = int(float(quantity))

        agg[(year, month, item_id)] += q
        processed += 1

    logging.debug("action: handle_batch | result: success | kind: transaction_items | rows:%s", processed)

def build_output_chunk() -> bytes:
    """
    Construye un ProcessChunk (TableType.TRANSACTIONS_ITEMS) donde cada fila representa
    (item_id, sum(quantity) en (year, month)).
    - transaction_id: vacío
    - item_id: item
    - quantity: suma mensual
    - subtotal: 0.0 (no se usa en este agg)
    - created_at: primer día del mes (para preservar año/mes)
    """
    rows = []
    for (year, month, item_id), qty in agg.items():
        created_at = date(year, month, 1)
        rows.append(
            TransactionsItemsProcessRow(
                transaction_id="",  # agregado, no hay trx específica
                item_id=item_id,
                quantity=qty,
                subtotal=0.0,
                created_at=created_at,
            )
        )

    header = ProcessChunkHeader(client_id=_last_client_id or 0, table_type=TableType.TRANSACTIONS_ITEMS)
    chunk = ProcessChunk(header, rows)
    return chunk.serialize()

def publish_results(rabbit_host: str, queue_out: str):
    body = build_output_chunk()
    payload_b64 = base64.b64encode(body).decode("utf-8")

    producer = MessageMiddlewareQueue(rabbit_host, queue_out)
    producer.send(payload_b64)
    producer.close()

    logging.info("action: publish_results | result: success | queue:%s | groups:%s",
                 queue_out, len(agg))

# ------------------------------
# Main
# ------------------------------
def main():
    cfg = initialize_config()
    initialize_log(cfg["log_level"])

    logging.debug("action: config | result: success | rabbit_host:%s | queue_in:%s | queue_out:%s",
                  cfg["rabbit_host"], cfg["queue_in"], cfg["queue_out"])

    consumer = MessageMiddlewareQueue(cfg["rabbit_host"], cfg["queue_in"])

    def callback(msg: str):
        try:
            batch_bytes = base64.b64decode(msg.encode())
            handle_batch(batch_bytes)
        except Exception as e:
            logging.error("action: callback | result: fail | error:%s", e)

    def shutdown(signum, frame):
        logging.info("action: shutdown | result: start | reason:signal")
        try:
            consumer.stop_consuming()
            consumer.close()
        finally:
            publish_results(cfg["rabbit_host"], cfg["queue_out"])
            sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logging.info("action: aggregate_start | result: start | source:%s | target:%s",
                 cfg["queue_in"], cfg["queue_out"])

    consumer.start_consuming(callback)
    consumer.close()
    logging.info("action: consume | result: finished")

    publish_results(cfg["rabbit_host"], cfg["queue_out"])

if __name__ == "__main__":
    main()
