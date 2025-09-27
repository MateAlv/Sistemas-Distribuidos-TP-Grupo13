#!/usr/bin/env python3
# client/main.py
import os
import sys
import logging
from typing import Any, Dict

try:
    from common.client import Client  # tu implementación después
except Exception as e:
    print(f"[client] ERROR importando Client: {e}", file=sys.stderr)
    raise

DEFAULT_DATA_DIR = "/data"  # ahora el default es un directorio

def _try_load_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}

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

    cfg["server"] = {"address": _get_env("CLI_SERVER_ADDRESS") or read_file("server.address", "server:5000")}
    cfg["log"] = {"level": (_get_env("CLI_LOG_LEVEL") or read_file("log.level", "INFO")).upper()}

    # directorio de datos
    data_dir_env = _get_env("CLI_DATA_DIR")
    data_dir = data_dir_env if data_dir_env else read_file("data.dir", DEFAULT_DATA_DIR)
    cfg["data"] = {"dir": data_dir}

    # batch
    env_batch = _get_env("CLI_BATCH_MAXAMOUNT")
    batch_val = int(env_batch) if env_batch.isdigit() else int(read_file("batch.maxAmount", 100))
    cfg["batch"] = {"maxAmount": batch_val}

    # protocolo (lo mantenemos configurable aunque el handshake lo veamos después)
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
        "action: config | result: success | client_id: %s | server_address: %s | data_dir: %s | log_level: %s",
        cfg.get("id", ""),
        cfg.get("server", {}).get("address", ""),
        cfg.get("data", {}).get("dir", DEFAULT_DATA_DIR),
        cfg.get("log", {}).get("level", "INFO"),
    )

def build_client_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "server_address": cfg["server"]["address"],
        "data_dir": cfg["data"]["dir"],  # <<<<<< ahora le pasamos un directorio
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

    try:
        client = Client(build_client_config(cfg), DEFAULT_DATA_DIR)
        # Sugerencia: tu Client puede ignorar el segundo argumento y usar config["data_dir"]
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
