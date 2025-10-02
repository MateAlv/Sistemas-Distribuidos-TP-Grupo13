#!/usr/bin/env python3

import os
import logging
import argparse
from configparser import ConfigParser
from common import Filter

def initialize_config(file_name):
    """Parsea archivo .ini y devuelve la configuración"""
    config = ConfigParser(os.environ)
    config.read(file_name)

    cfg = {}
    try:
        logging_level = os.getenv("LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"])
        cfg["filter_type"] = os.getenv("FILTER_TYPE", config["DEFAULT"]["FILTER_TYPE"])

        if cfg["filter_type"] == "year":
            cfg["year_start"] = int(os.getenv("YEAR_START", config["DEFAULT"]["YEAR_START"]))
            cfg["year_end"] = int(os.getenv("YEAR_END", config["DEFAULT"]["YEAR_END"]))
        elif cfg["filter_type"] == "hour":
            cfg["hour_start"] = int(os.getenv("HOUR_START", config["DEFAULT"]["HOUR_START"]))
            cfg["hour_end"] = int(os.getenv("HOUR_END", config["DEFAULT"]["HOUR_END"]))
        elif cfg["filter_type"] == "amount":
            cfg["min_amount"] = float(os.getenv("MIN_AMOUNT", config["DEFAULT"]["MIN_AMOUNT"]))
        else:
            raise ValueError(f"Tipo de filtro inválido: {cfg['filter_type']}")

    except KeyError as e:
        raise KeyError(f"Key no encontrada. Error: {e}. Abortando.")
    except ValueError as e:
        raise ValueError(f"Error de parseo. {e}. Abortando.")

    return (logging_level, cfg)


def initialize_log(logging_level):
    """Inicializa logging"""
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(description="Procesador de transacciones con filtros.")
    parser.add_argument(
        "--filter", choices=["year", "hour", "amount"], required=True,
        help="Tipo de filtro a aplicar (year | hour | amount)"
    )
    args = parser.parse_args()

    config_file = f"config/config_{args.filter}.ini"
    (logging_level, cfg) = initialize_config(config_file)
    initialize_log(logging_level)

    logging.debug(f"Config cargada desde {config_file}: {cfg}")

    filter = Filter(cfg)

    logging.debug(f"filter: ", str(filter))

    filter.run()


if __name__ == "__main__":
    main()
