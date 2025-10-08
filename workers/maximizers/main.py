#!/usr/bin/env python3

import os
import logging
import argparse
from configparser import ConfigParser

# Importar Maximizer
from common import Maximizer
    
# Configurar logging de pika MUY temprano y de forma agresiva
logging.getLogger('pika').setLevel(logging.CRITICAL)
logging.getLogger('pika').disabled = True

def initialize_config():
    try:
        config = ConfigParser()
        config.read('config.ini')
        
        logging_level = config['DEFAULT']['LOGGING_LEVEL']
        max_type = os.getenv('MAXIMIZER_TYPE')
        max_range = os.getenv('MAXIMIZER_RANGE')
        
        if max_type is None:
            raise ValueError(f"Tipo de maximizer inválido: {max_type}")
        
        if max_range is None:
            raise ValueError(f"Rango de maximizer inválido: {max_range}")
        
        # Validar que sea uno de los tipos válidos
        valid_types = ["MAX", "TOP3"]
        if max_type not in valid_types:
            raise ValueError(f"Tipo de maximizer inválido: {max_type}")
        
        # Validar que sea uno de los rangos válidos
        valid_ranges = ["0", "1", "4", "7"]
        if max_range not in valid_ranges:
            raise ValueError(f"Rango de maximizer inválido: {max_range}")
        
        logging.info(f"action: config | result: success | max_type:{max_type} | max_range:{max_range} | log_level:{logging_level}")
        
        return (logging_level, max_type, max_range)
        
    except Exception as e:
        logging.error(f"Error cargando configuración: {e}")
        raise ValueError(f"Error de parseo. {e}. Abortando.")

    except KeyError as e:
        raise KeyError(f"Key no encontrada. Error: {e}. Abortando.")
    except ValueError as e:
        raise ValueError(f"Error de parseo. {e}. Abortando.")


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

    (logging_level, max_type, max_range) = initialize_config()
    initialize_log(logging_level)

    logging.debug(f"action: config | result: success | max_type:{max_type} | max_range:{max_range} | log_level:{logging_level}")

    maximizer = Maximizer(max_type, max_range)

    maximizer.run()


if __name__ == "__main__":
    main()
