import os
import signal
import logging
from common import RepetitionChecker

# Configurar logging de pika
logging.getLogger('pika').setLevel(logging.CRITICAL)
logging.getLogger('pika').disabled = True

def initialize_config():

    cfg = {}
    try:
        logging_level = os.getenv("LOGGING_LEVEL")

    except KeyError as e:
        raise KeyError(f"Key no encontrada. Error: {e}. Abortando.")
    except ValueError as e:
        raise ValueError(f"Error de parseo. {e}. Abortando.")

    return (logging_level, cfg)


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
    
    logging.getLogger('pika').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def main():
    (logging_level, cfg) = initialize_config()
    initialize_log(logging_level)
    repetition_checker = RepetitionChecker(3600, data_dir="./repetition_checker_data")
    
    signal.signal(signal.SIGTERM, repetition_checker.shutdown)

    repetition_checker.start()


if __name__ == "__main__":
    main()
