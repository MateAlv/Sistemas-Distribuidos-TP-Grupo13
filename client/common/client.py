# common/client.py
import os
import logging
from typing import Dict, List, Optional, Tuple

from common.batch_reader import DirectoryBatchReader, BatchReader
from common.sender import Sender


class Client:
    """
    - Modo 'single' (default): 1 conexión TCP persistente para TODO el directorio.
      Flujo:
        I:H <id>\n  → I:O
        Repetir por archivo:
          F:\n CLI_ID:.. FILENAME:.. SIZE:..\n\n + cuerpo (SIZE bytes) → I:O
        I:F\n → I:O (opcional)
    - Modo 'per_file': 1 conexión por archivo (útil si el server aún no soporta multiarchivo).
    """

    def __init__(self, config: Dict, default_data_dir: str = "/data") -> None:
        self.id: str = str(config.get("id", ""))

        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 64 * 1024))  # tamaño de chunk en bytes

        # Dirección del servidor "host:port"
        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        # Directorio de datos
        self.data_dir: str = str(config.get("data_dir", default_data_dir))

        # Conexión / timeouts
        conn_cfg = config.get("connection", {}) or {}
        self.connection_mode: str = str(conn_cfg.get("mode", "single")).lower().strip()
        if self.connection_mode not in {"single", "per_file"}:
            self.connection_mode = "single"
        self.connect_timeout: float = float(conn_cfg.get("connect_timeout", 10))
        self.io_timeout: float = float(conn_cfg.get("io_timeout", 30))

        # Filtros de archivos
        data_cfg = config.get("data", {}) or {}
        self.allowed_exts: List[str] = [e.lower() for e in data_cfg.get("extensions", [])]

        # Handshake (on por defecto)
        hs_cfg = config.get("handshake", {}) or {}
        self.handshake_enabled: bool = bool(hs_cfg.get("enabled", True))

        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s mode=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.connection_mode, self.batch_size
        )

    # ---------------------------
    # API pública
    # ---------------------------
    def start_client_loop(self) -> None:
        # Modo conexión persistente: usa DirectoryBatchReader + Sender
        if self.connection_mode == "single":
            reader = DirectoryBatchReader(
                self.data_dir,
                self.batch_size,
                mode="bytes",
                extensions=self.allowed_exts or None,
            )
            total_files_hint = self._count_files(self.data_dir)
            logging.info("Conexión TCP persistente para ~%d archivo(s)", total_files_hint)

            with Sender(
                self.server_host,
                self.server_port,
                connect_timeout=self.connect_timeout,
                io_timeout=self.io_timeout,
            ) as sender:
                if self.handshake_enabled:
                    sender.send_hello(self.id)

                for fc in reader:
                    if fc.first:
                        sender.start_file(self.id, fc.rel_path, fc.file_size)
                    if fc.data:
                        sender.send_batch(fc.data)
                    if fc.last:
                        sender.end_file_and_wait_ack()

                sender.send_finished()

            logging.info("Cliente %s: envío completado (single).", self.id)
            return

        # Modo una conexión por archivo (compatibilidad)
        files = self._list_files(self.data_dir)
        if not files:
            logging.warning("No se encontraron archivos en %s", self.data_dir)
            return

        logging.info("Una conexión por archivo para %d archivo(s)", len(files))
        for abs_path, rel_path in files:
            size = os.path.getsize(abs_path)
            with Sender(
                self.server_host,
                self.server_port,
                connect_timeout=self.connect_timeout,
                io_timeout=self.io_timeout,
            ) as sender:
                if self.handshake_enabled:
                    sender.send_hello(self.id)

                sender.start_file(self.id, rel_path, size)
                for chunk in BatchReader(abs_path, self.batch_size):
                    sender.send_batch(chunk)
                sender.end_file_and_wait_ack()
        logging.info("Cliente %s: envío completado (per_file).", self.id)

    # ---------------------------
    # Utilitarios de archivos
    # ---------------------------
    def _list_files(self, root: str) -> List[Tuple[str, str]]:
        files: List[Tuple[str, str]] = []
        root_abs = os.path.abspath(root)
        for dirpath, _, filenames in os.walk(root_abs):
            for name in sorted(filenames):
                abs_path = os.path.join(dirpath, name)
                if not os.path.isfile(abs_path):
                    continue
                if self.allowed_exts:
                    _, ext = os.path.splitext(name)
                    if ext.lower() not in self.allowed_exts:
                        continue
                rel_path = os.path.relpath(abs_path, root_abs)
                files.append((abs_path, rel_path))
        files.sort(key=lambda t: t[1])
        return files

    def _count_files(self, root: str) -> int:
        cnt = 0
        for _, _, files in os.walk(root):
            if self.allowed_exts:
                files = [f for f in files if os.path.splitext(f)[1].lower() in self.allowed_exts]
            cnt += len(files)
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
