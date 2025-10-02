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
from utils.file_utils.process_table import TransactionsProcessRow  # filas de transactions

# ------------------------------
# Constantes (overridable por ENV / config.ini)
# ------------------------------
DEFAULT_INPUT_QUEUE  = "transactions"        # batches con TransactionsProcessRow
DEFAULT_OUTPUT_QUEUE = "results_query_3"     # salida con TPV por (store, year, semester)
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

def _semester_of_month(m: int) -> int:
    # 1: Jan-Jun, 2: Jul-Dec
    return 1 if 1 <= m <= 6 else 2

# ------------------------------
# Estado en memoria
# ------------------------------
# tpv[(year, semester, store_id)] = sum(final_amount)
tpv = defaultdict(float)
_last_client_id = 0  # para el header saliente

# ------------------------------
# Procesamiento
# ------------------------------
def handle_batch(batch_bytes: bytes):
    """Suma final_amount por (year, semester, store_id)."""
    global _last_client_id
    batch = ProcessBatchReader.from_bytes(batch_bytes)
    _last_client_id = batch.header.client_id or _last_client_id

    if batch.header.table_type != TableType.TRANSACTIONS:
        logging.debug("action: handle_batch | result: ignored | kind:%s", batch.header.table_type.name)
        return

    processed = 0
    for row in batch.rows:
        store_id = getattr(row, "store_id", None)
        created_at = getattr(row, "created_at", None)
        final_amount = getattr(row, "final_amount", None)

        if store_id is None or created_at is None or final_amount is None:
            continue

        dt = _parse_dt(created_at)
        if dt.year not in YEARS:
            continue

        try:
            val = float(final_amount)
        except Exception:
            val = float(str(final_amount).replace(",", "."))

        key = (dt.year, _semester_of_month(dt.month), int(store_id))
        tpv[key] += val
        processed += 1

    logging.debug("action: handle_batch | result: success | kind: transactions | rows:%s", processed)

def build_output_chunk() -> bytes:
    """
    Construye un ProcessChunk (TableType.TRANSACTIONS) donde cada fila representa
    (store_id, year, semester, tpv=sum(final_amount)).
    - transaction_id: "" (agregado)
    - store_id: el de la clave
    - final_amount: TPV agregado del semestre
    - created_at: primer día del semestre (para codificar {year, semester})
    """
    rows = []
    for (year, semester, store_id), total in tpv.items():
        first_month = 1 if semester == 1 else 7
        created = date(year, first_month, 1)

        # Campos extra que TransactionsProcessRow pueda requerir (si tiene más args)
        # Se dejan en valores neutros; los indispensables arriba.
        rows.append(
            TransactionsProcessRow(
                transaction_id="",
                store_id=store_id,
                final_amount=total,
                created_at=created,
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
                 queue_out, len(tpv))

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
