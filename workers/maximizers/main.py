#!/usr/bin/env python3

import os
import logging
import argparse
from configparser import ConfigParser

# Importar Maximizer
from common.maximizer import Maximizer
    
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
    
    # Test log inmediato
    logging.info("action: logging_initialized | result: success")


def main():
    print("MAXIMIZER: Starting up...")  # Print inmediato para debug
    
    try:
        parser = argparse.ArgumentParser(description="Procesador de transacciones con maximizador.")
        args = parser.parse_args()

        print("MAXIMIZER: Loading config...")
        (logging_level, max_type, max_range) = initialize_config()
        
        print("MAXIMIZER: Initializing logging...")
        initialize_log(logging_level)

        logging.info(f"action: maximizer_startup | result: success | max_type:{max_type} | max_range:{max_range} | log_level:{logging_level}")

        print(f"MAXIMIZER: Creating instance - type:{max_type} range:{max_range}")
        maximizer = Maximizer(max_type, max_range)

        logging.info(f"action: maximizer_created | type:{max_type} | range:{max_range}")
        print(f"MAXIMIZER: Starting run loop...")
        maximizer.run()
        
    except Exception as e:
        print(f"MAXIMIZER ERROR: {e}")
        import traceback
        print(f"MAXIMIZER TRACEBACK: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()
