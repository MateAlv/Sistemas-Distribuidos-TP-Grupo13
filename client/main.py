#!/usr/bin/env python3
# client/main.py
import os
import sys
import logging
from typing import Any, Dict

try:
    from common.client import Client
except Exception as e:
    print(f"[client] ERROR importando Client: {e}", file=sys.stderr)
    raise

DEFAULT_CSV_FILE_PATH = "/data/agency.csv"

def _try_load_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return {}
        return data

def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)

def init_config() -> Dict[str, Any]:
    cfg_file = os.getenv("CLIENT_CONFIG_FILE", "/config.yaml")
    file_cfg = _try_load_yaml(cfg_file)

    def read_file(path: str, default: Any = None) -> Any:
        cur = file_cfg
        for part in path.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return default
            cur = cur[part]
        return cur

    cfg: Dict[str, Any] = {}

    # id
    cfg["id"] = _get_env("CLI_ID") or read_file("id", "")

    # server.address
    cfg["server"] = {
        "address": _get_env("CLI_SERVER_ADDRESS") or read_file("server.address", "server:5000")
    }

    # log.level
    cfg["log"] = {
        "level": (_get_env("CLI_LOG_LEVEL") or read_file("log.level", "INFO")).upper()
    }

    # batch.maxAmount
    env_batch = _get_env("CLI_BATCH_MAXAMOUNT")
    if env_batch:
        try:
            batch_val = int(env_batch)
        except ValueError:
            batch_val = 100
    else:
        batch_val = int(read_file("batch.maxAmount", 100))
    cfg["batch"] = {"maxAmount": batch_val}

    # protocol.*  (todo se puede overridear por env si querés)
    proto_defaults = {
        "fieldSeparator": ";",
        "batchSeparator": "~",
        "messageDelimiter": "\n",
        "finishedHeader": "F:",
        "successBody": "OK",
        "failureBody": "FAIL",
        "finishedBody": "FINISHED",
    }
    cfg["protocol"] = {}
    for key, default in proto_defaults.items():
        env_name = "CLI_PROTOCOL_" + key.upper()
        cfg["protocol"][key] = _get_env(env_name) or read_file(f"protocol.{key}", default)

    return cfg

def init_logger(level_str: str) -> None:
    level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname).5s     %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

def print_config(cfg: Dict[str, Any]) -> None:
    logging.info(
        "action: config | result: success | client_id: %s | server_address: %s | csv_file: %s | log_level: %s",
        cfg.get("id", ""),
        cfg.get("server", {}).get("address", ""),
        DEFAULT_CSV_FILE_PATH,
        cfg.get("log", {}).get("level", "INFO"),
    )

def build_client_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": cfg.get("id", ""),
        "server_address": cfg["server"]["address"],
        "message_protocol": {
            "batch_size": cfg["batch"]["maxAmount"],
            "field_separator": cfg["protocol"]["fieldSeparator"],
            "batch_separator": cfg["protocol"]["batchSeparator"],
            "message_delimiter": cfg["protocol"]["messageDelimiter"],
            "finished_header": cfg["protocol"]["finishedHeader"],
            "success_response": cfg["protocol"]["successBody"],
            "failure_response": cfg["protocol"]["failureBody"],
            "finished_body": cfg["protocol"]["finishedBody"],
        },
    }

def main() -> None:
    cfg = init_config()
    init_logger(cfg["log"]["level"])
    print_config(cfg)

    client_config = build_client_config(cfg)

    try:
        client = Client(client_config, DEFAULT_CSV_FILE_PATH)
    except Exception as e:
        logging.critical("Failed to create client: %s", e)
        sys.exit(1)

    try:
        client.start_client_loop()
    except Exception as e:
        logging.critical("Failed to start client loop: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
