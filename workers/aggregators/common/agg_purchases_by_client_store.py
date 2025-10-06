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
from utils.file_utils.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.file_utils.process_table import TransactionsProcessRow  # (transaction_id, store_id, user_id, final_amount, created_at)

# ------------------------------
# Constantes (overridable por ENV / config.ini)
# ------------------------------
DEFAULT_INPUT_QUEUE  = "transactions"                 # batches con TransactionsProcessRow (2024–2025)
DEFAULT_OUTPUT_QUEUE = "transactions_sum_by_client"   # salida agregada por (store_id, user_id) con COUNT()
YEARS = {2024, 2025}

# ------------------------------
# Config & Logging
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
# Helpers
# ------------------------------
def _parse_dt(created_at):
    if isinstance(created_at, datetime):
        return created_at
    if isinstance(created_at, str):
        try:
            return datetime.fromisoformat(created_at)
        except ValueError:
            return datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
    # date simple
    return datetime(created_at.year, created_at.month, getattr(created_at, "day", 1))

# ------------------------------
# Estado en memoria
# ------------------------------
# counts[(store_id, user_id)] = cantidad_de_compras
counts = defaultdict(int)
_last_client_id = 0  # para el header saliente

# ------------------------------
# Procesamiento
# ------------------------------
def handle_batch(batch_bytes: bytes):
    """Cuenta filas por (store_id, user_id)."""
    global _last_client_id
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    _last_client_id = batch.header.client_id or _last_client_id

    if batch.header.table_type != TableType.TRANSACTIONS:
        logging.debug("action: handle_batch | result: ignored | kind:%s", batch.header.table_type.name)
        return

    processed = 0
    for row in batch.rows:
        store_id = getattr(row, "store_id", None)
        user_id = getattr(row, "user_id", None)
        created_at = getattr(row, "created_at", None)
        if store_id is None or user_id is None or created_at is None:
            continue

        dt = _parse_dt(created_at)
        if dt.year not in YEARS:
            continue

        counts[(int(store_id), int(user_id))] += 1
        processed += 1

    logging.debug("action: handle_batch | result: success | kind: transactions | rows:%s", processed)

def build_output_chunk() -> bytes:
    """
    Construye un ProcessChunk (TableType.TRANSACTIONS) donde cada fila representa
    (store_id, user_id, count). Usamos TransactionsProcessRow:
      - transaction_id: "" (agregado)
      - store_id: clave
      - user_id: clave
      - final_amount: COUNT() (guardamos el conteo aquí)
      - created_at: 2024-01-01 (marca de agregado; no se usa para re-agrupado)
    """
    rows = []
    marker_date = date(2024, 1, 1)  # simple marca; el query no requiere separar por año después
    for (store_id, user_id), cnt in counts.items():
        rows.append(
            TransactionsProcessRow(
                transaction_id="",
                store_id=store_id,
                user_id=user_id,
                final_amount=float(cnt),  # usamos este campo para el conteo
                created_at=marker_date,
            )
        )

    header = ProcessChunkHeader(client_id=_last_client_id or 0, table_type=TableType.TRANSACTIONS)
    return ProcessChunk(header, rows).serialize()

def publish_results(rabbit_host: str, queue_out: str):
    body = build_output_chunk()
    payload_b64 = base64.b64encode(body).decode("utf-8")

    producer = MessageMiddlewareQueue(rabbit_host, queue_out)
    producer.send(payload_b64)
    producer.close()

    logging.info("action: publish_results | result: success | queue:%s | groups:%s",
                 queue_out, len(counts))

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

    # consume (bloqueante) hasta terminar o recibir señal
    consumer.start_consuming(callback)
    consumer.close()
    logging.info("action: consume | result: finished")

    publish_results(cfg["rabbit_host"], cfg["queue_out"])

if __name__ == "__main__":
    main()
