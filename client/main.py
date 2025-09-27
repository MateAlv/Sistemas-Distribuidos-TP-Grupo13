#!/usr/bin/env python3

import os
import sys
import logging
import signal
from configparser import ConfigParser

from common.client import Client


def initialize_config():
    """
    Lee parámetros desde ENV > config.ini.
    Devuelve un dict listo para armar el Client.
    """
    config = ConfigParser(os.environ)
    config.read("config.ini")

    try:
        params = {
            "id": os.getenv("CLIENT_ID", config["DEFAULT"]["CLIENT_ID"]),
            "server_address": os.getenv("SERVER_ADDRESS", config["DEFAULT"]["SERVER_ADDRESS"]),
            "log_level": os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]),
            "data_dir": os.getenv("DATA_DIR", config["DEFAULT"]["DATA_DIR"]),
            "batch_max": int(os.getenv("BATCH_MAX_AMOUNT", config["DEFAULT"]["BATCH_MAX_AMOUNT"])),
            "protocol": {
                "field_separator": os.getenv("FIELD_SEPARATOR", config["PROTOCOL"]["FIELD_SEPARATOR"]),
                "batch_separator": os.getenv("BATCH_SEPARATOR", config["PROTOCOL"]["BATCH_SEPARATOR"]),
                "message_delimiter": os.getenv("MESSAGE_DELIMITER", config["PROTOCOL"]["MESSAGE_DELIMITER"]),
                "finished_header": os.getenv("FINISHED_HEADER", config["PROTOCOL"]["FINISHED_HEADER"]),
                "success_body": os.getenv("SUCCESS_BODY", config["PROTOCOL"]["SUCCESS_BODY"]),
                "failure_body": os.getenv("FAILURE_BODY", config["PROTOCOL"]["FAILURE_BODY"]),
                "finished_body": os.getenv("FINISHED_BODY", config["PROTOCOL"]["FINISHED_BODY"]),
                "hello": os.getenv("HELLO", config["PROTOCOL"]["HELLO"]),
            },
        }
    except KeyError as e:
        raise KeyError(f"Missing config key: {e}")
    except ValueError as e:
        raise ValueError(f"Invalid config value: {e}")

    return params


def initialize_log(level_str: str):
    level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    cfg = initialize_config()
    initialize_log(cfg["log_level"])

    logging.debug(
        "action: config | result: success | client_id:%s | server_address:%s | data_dir:%s | log_level:%s | batch_max:%s",
        cfg["id"], cfg["server_address"], cfg["data_dir"], cfg["log_level"], cfg["batch_max"]
    )

    client_config = {
        "id": cfg["id"],
        "server_address": cfg["server_address"],
        "data_dir": cfg["data_dir"],
        "message_protocol": {
            "batch_size": cfg["batch_max"],
            **cfg["protocol"],
        },
    }

    client = Client(client_config, cfg["data_dir"])

    # opcional: graceful shutdown
    def shutdown_handler(signum, frame):
        logging.info("SIGTERM recibido, cerrando cliente")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        client.start_client_loop()
    except Exception as e:
        logging.critical("Client loop failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
