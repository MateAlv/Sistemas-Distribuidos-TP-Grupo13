#!/usr/bin/env python3

import os
import signal
import logging
import argparse
from configparser import ConfigParser

# Importar Aggregator después de configurar logging
from common import Aggregator
    
# Configurar logging de pika MUY temprano y de forma agresiva
logging.getLogger('pika').setLevel(logging.CRITICAL)
logging.getLogger('pika').disabled = True

def initialize_config():

    try:
        logging_level = os.getenv("LOGGING_LEVEL", "DEBUG")
        agg_type = os.getenv("AGGREGATOR_TYPE")
        agg_id = int(os.getenv("AGGREGATOR_ID", "1"))

        if agg_type != "PRODUCTS" and agg_type != "PURCHASES" and agg_type != "TPV":
            raise ValueError(f"Tipo de agregador inválido: {agg_type}")

    except KeyError as e:
        raise KeyError(f"Key no encontrada. Error: {e}. Abortando.")
    except ValueError as e:
        raise ValueError(f"Error de parseo. {e}. Abortando.")

    return (logging_level, agg_type, agg_id)


def initialize_log(logging_level):
    """Inicializa logging"""
    # Convertir string a constante de logging
    if isinstance(logging_level, str):
        logging_level = getattr(logging, logging_level.upper())
    
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Silenciar logs de pika completamente si el nivel es INFO o superior
    logging.getLogger('pika').setLevel(logging.ERROR)
    # También silenciar otros loggers verbosos
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Procesador de transacciones con agregación.")
    args = parser.parse_args()

    (logging_level, agg_type, agg_id) = initialize_config()
    initialize_log(logging_level)
    
    logging.debug(f"action: config | result: success | agg_type:{agg_type} | agg_id:{agg_id} | log_level:{logging_level}")
    
    aggregator = Aggregator(agg_type, agg_id)

    signal.signal(signal.SIGTERM, aggregator.shutdown)
    
    aggregator.run()


if __name__ == "__main__":
    main()
