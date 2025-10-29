#!/usr/bin/env python3

import os
import sys
import signal
import logging
import argparse
from pathlib import Path
from configparser import ConfigParser

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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

        if max_type is None:
            raise ValueError(f"Tipo de maximizer inválido: {max_type}")

        # Validar que sea uno de los tipos válidos
        valid_types = ["MAX", "TOP3"]
        if max_type not in valid_types:
            raise ValueError(f"Tipo de maximizer inválido: {max_type}")

        if max_type == "MAX":
            shard_env = "MAX_SHARD_ID"
            partial_shards_env = "MAX_PARTIAL_SHARDS"
        else:
            shard_env = "TOP3_SHARD_ID"
            partial_shards_env = "TOP3_PARTIAL_SHARDS"

        shard_id = os.getenv(shard_env)
        partial_shards_spec = os.getenv(partial_shards_env)

        if shard_id and partial_shards_spec:
            raise ValueError(
                f"Configuración inválida: defina solo {shard_env} para maximizers parciales o {partial_shards_env} para maximizers absolutos."
            )

        role = None
        partial_shards: list[str] = []

        if shard_id:
            shard_id = shard_id.strip()
            if not shard_id:
                raise ValueError(f"{shard_env} no puede ser vacío.")
            role = "partial"
        elif partial_shards_spec:
            candidates = [entry.strip() for entry in partial_shards_spec.split(",") if entry.strip()]
            if not candidates:
                raise ValueError(f"{partial_shards_env} no puede ser vacío.")
            role = "absolute"
            partial_shards = candidates
        else:
            raise ValueError(
                f"Debe configurarse {shard_env} para un maximizer parcial o {partial_shards_env} para uno absoluto."
            )

        return (logging_level, max_type, role, shard_id, partial_shards)
        
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

    (logging_level, max_type, role, shard_id, partial_shards) = initialize_config()
    initialize_log(logging_level)

    logging.info(
        f"action: config | result: success | max_type:{max_type} | role:{role} | shard_id:{shard_id} | partial_shards:{partial_shards} | log_level:{logging_level}"
    )

    maximizer = Maximizer(max_type, role, shard_id, partial_shards)

    signal.signal(signal.SIGTERM, maximizer.shutdown)
        
    maximizer.run()


if __name__ == "__main__":
    main()
