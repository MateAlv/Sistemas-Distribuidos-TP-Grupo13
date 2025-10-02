# common/client.py
import os
import logging
from typing import Dict, Tuple

from utils.communication.directory_reader import DirectoryReader
from utils.communication.batch_reader import BatchReader
from common.sender import Sender


# ============================
# Constantes del cliente
# ============================
# Conexión: SIEMPRE 1 socket TCP persistente
CONNECT_TIMEOUT_S: float = 10.0
IO_TIMEOUT_S: float = 30.0

class Client:
   
    def __init__(self, config: Dict, default_data_dir: str = "/data") -> None:
        # Identidad del cliente
        self.id: str = str(config.get("id", ""))

        # Tamaño de batch (bytes) para leer/enviar en chunks (se mantiene configurable)
        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 64 * 1024))

        # Dirección del servidor "host:port"
        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        # Directorio de datos
        self.data_dir: str = str(config.get("data_dir", default_data_dir))
        
        # Lector de batches
        self.reader = BatchReader(
            client_id=int(self.id),
            root=self.data_dir,
            max_batch_size=self.batch_size,
        )
            
        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.batch_size
        )

    # ---------------------------
    # API pública
    # ---------------------------
    def start_client_loop(self) -> None:
        """
        Único modo soportado: conexión TCP persistente para TODO el directorio.
        Solo procesa archivos con extensión .csv (case-insensitive).
        """

        # Conexión persistente + handshake obligatorio
        with Sender(
            self.server_host,
            self.server_port,
            connect_timeout=CONNECT_TIMEOUT_S,
            io_timeout=IO_TIMEOUT_S,
        ) as sender:
            try:
                logging.info("Cliente %s: conectando a %s:%s", self.id, self.server_host, self.server_port)
                # Handshake SIEMPRE encendido
                sender.send_handshake_request(self.id)
                
                logging.info("Cliente %s: handshake OK con %s:%s", self.id, self.server_host, self.server_port)
                
                logging.info("Cliente %s: comenzando envío de datos a %s:%s", self.id, self.server_host, self.server_port)
                
                # Envío de batches (iterativo)
                for chunk in self.reader.iter():
                    sender.send_file_chunk(chunk.serialize())
                    if chunk.is_last_file_chunk():
                        # Espera ACK de fin de archivo
                        sender.wait_end_file_ack()

                # Señal de fin - Todos los archivos enviados
                sender.send_finished()

                logging.info("Cliente %s: envío completado (single).", self.id)
            except Exception as e:
                logging.error("Cliente %s: error en el envío: %s", self.id, e)
                raise
    # ---------------------------
    # Helpers
    # ---------------------------
    def _parse_host_port(self, s: str) -> Tuple[str, int]:
        if ":" not in s:
            return s, 5000
        host, port = s.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            logging.warning("Puerto inválido en %s; uso 5000", s)
            return host, 5000
