# common/client.py
import os
import logging
from typing import Dict, Tuple

from common.directory_batch_reader import DirectoryBatchReader
from common.sender import Sender


# ============================
# Constantes del cliente
# ============================
# Conexión: SIEMPRE 1 socket TCP persistente
CONNECT_TIMEOUT_S: float = 10.0
IO_TIMEOUT_S: float = 30.0

# Archivos: solo CSV
ALLOWED_EXTS = {".csv"}


class Client:
    """
    Protocolo (socket persistente):
      I:H <id>\n           → I:O
      Por cada archivo CSV:
        F:\n
        CLI_ID: <id>\n
        FILENAME: <rel_path>\n
        SIZE: <size>\n
        \n
        <size bytes>        → I:O
      I:F\n                 → I:O
    """

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
        # Lector de directorio en modo binario por chunks
        reader = DirectoryBatchReader(
            self.data_dir,
            self.batch_size,
            mode="bytes",
            extensions=list(ALLOWED_EXTS),
        )

        total_files_hint = self._count_csv_files(self.data_dir)
        logging.info("Conexión TCP persistente para ~%d archivo(s)", total_files_hint)

        # Conexión persistente + handshake obligatorio
        with Sender(
            self.server_host,
            self.server_port,
            connect_timeout=CONNECT_TIMEOUT_S,
            io_timeout=IO_TIMEOUT_S,
        ) as sender:
            # Handshake SIEMPRE encendido
            sender.send_hello(self.id)

            # Stream de archivos
            for fc in reader:
                if fc.first:
                    sender.start_file(self.id, fc.rel_path, fc.file_size)
                if fc.data:
                    sender.send_batch(fc.data)
                if fc.last:
                    sender.end_file_and_wait_ack()

            # Señal de fin
            sender.send_finished()

        logging.info("Cliente %s: envío completado (single).", self.id)

    # ---------------------------
    # Utilitarios
    # ---------------------------
    def _count_csv_files(self, root: str) -> int:
        cnt = 0
        for _, _, files in os.walk(os.path.abspath(root)):
            for name in files:
                _, ext = os.path.splitext(name)
                if ext.lower() in ALLOWED_EXTS:
                    cnt += 1
        return cnt

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
