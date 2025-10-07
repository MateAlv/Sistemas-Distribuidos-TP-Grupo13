#!/usr/bin/env python3

import os
import logging
import argparse
from configparser import ConfigParser

# Importar Joiners
from common import Joiner
    
# Configurar logging de pika MUY temprano y de forma agresiva
logging.getLogger('pika').setLevel(logging.CRITICAL)
logging.getLogger('pika').disabled = True

def initialize_config():

    try:
        logging_level = os.getenv("LOGGING_LEVEL", "DEBUG")
        join_type = os.getenv("JOINER_TYPE")

        if join_type != "ITEMS" and join_type != "STORES_TPV" and join_type != "STORES_TOP3" and join_type != "USERS":
            raise ValueError(f"Tipo de joiner inválido: {join_type}")

    except KeyError as e:
        raise KeyError(f"Key no encontrada. Error: {e}. Abortando.")
    except ValueError as e:
        raise ValueError(f"Error de parseo. {e}. Abortando.")

    return (logging_level, join_type)


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
    parser = argparse.ArgumentParser(description="Procesador de transacciones con maximizador.")
    args = parser.parse_args()

    (logging_level, joiner_type) = initialize_config()
    initialize_log(logging_level)

    logging.debug(f"action: config | result: success | joiner_type:{joiner_type} | log_level:{logging_level}")

    joiner = Joiner(joiner_type)

    joiner.run()


if __name__ == "__main__":
    main()
