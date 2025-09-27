# common/client.py
import os
import socket
import logging
from typing import Dict, Iterable, List, Optional, Tuple

from common.batch_reader import BatchReader  # implementar luego

class Client:
    """
    Cliente que:
      - Abre 1 conexión TCP (por defecto) o 1 por archivo (configurable)
      - Handshake simple (opcional)
      - Recorre un directorio y envía todos los archivos que cumplan el filtro
      - Por cada archivo, envía un header (CLI_ID, FILENAME, SIZE) y luego el contenido por batches
      - Espera 'OK\\n' del servidor después de cada archivo
    """

    def __init__(self, config: Dict, default_data_dir: str = "/data") -> None:
        self.id: str = str(config.get("id", ""))
        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 100))
        self.success_resp: bytes = str(mp.get("success_response", "OK")).encode("utf-8")
        self.message_delim: bytes = str(mp.get("message_delimiter", "\n")).encode("utf-8")

        self.finished_header: str = str(mp.get("finished_header", "F:"))
        self.finished_body: str = str(mp.get("finished_body", "FINISHED"))

        self.field_separator: str = str(mp.get("field_separator", ";"))
        self.batch_separator: str = str(mp.get("batch_separator", "~"))

        # Dirección del servidor "host:port"
        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        # Directorio de datos
        self.data_dir: str = str(config.get("data_dir", default_data_dir))

        # Modo de conexión: "single" (persistente) o "per_file"
        conn_cfg = config.get("connection", {}) or {}
        self.connection_mode: str = str(conn_cfg.get("mode", "single")).lower().strip()
        if self.connection_mode not in {"single", "per_file"}:
            self.connection_mode = "single"

        # Filtro extensiones (opcional): e.g., [".csv"] — si vacío, toma todos los archivos
        data_cfg = config.get("data", {}) or {}
        self.allowed_exts: List[str] = [e.lower() for e in data_cfg.get("extensions", [])]

        # Timeouts opcionales (segundos)
        self.connect_timeout: Optional[float] = float(conn_cfg.get("connect_timeout", 10)) if "connect_timeout" in conn_cfg else 10.0
        self.io_timeout: Optional[float] = float(conn_cfg.get("io_timeout", 30)) if "io_timeout" in conn_cfg else 30.0

        # Handshake opcional
        hs_cfg = config.get("handshake", {}) or {}
        self.handshake_enabled: bool = bool(hs_cfg.get("enabled", True))
        self.handshake_hello: str = str(hs_cfg.get("hello", "HELLO"))
        # Si handshake espera un OK, reusamos success_resp + message_delim

        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s mode=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.connection_mode, self.batch_size
        )

    # ---------------------------
    # API pública
    # ---------------------------
    def start_client_loop(self) -> None:
        files = self._list_files(self.data_dir)
        if not files:
            logging.warning("No se encontraron archivos en %s", self.data_dir)
            return

        if self.connection_mode == "single":
            logging.info("Usando conexión TCP persistente (single) para %d archivo(s)", len(files))
            with self._open_socket() as sock:
                self._maybe_handshake(sock)
                for abs_path, rel_path in files:
                    self._send_one_file(sock, abs_path, rel_path)
                # (Opcional) enviar un mensaje de FIN del lote/directorio
                self._send_finished(sock)
        else:
            logging.info("Usando 1 conexión por archivo (per_file) para %d archivo(s)", len(files))
            for abs_path, rel_path in files:
                with self._open_socket() as sock:
                    self._maybe_handshake(sock)
                    self._send_one_file(sock, abs_path, rel_path)
                # socket se cierra al salir del with

        logging.info("Cliente %s: envío completado.", self.id)

    # ---------------------------
    # Envío de archivos
    # ---------------------------
    def _send_one_file(self, sock: socket.socket, abs_path: str, rel_path: str) -> None:
        size = os.path.getsize(abs_path)
        logging.info("Enviando archivo: rel=%s size=%d", rel_path, size)

        # Header estilo texto (compatible con el server que propusimos antes)
        header = (
            f"CLI_ID: {self.id}\n"
            f"FILENAME: {rel_path}\n"
            f"SIZE: {size}\n"
            f"\n"
        ).encode("utf-8")
        self._sendall(sock, header)

        # Cuerpo en batches (sin cargar el archivo completo en memoria)
        reader = BatchReader(abs_path, self.batch_size)
        total_sent = 0
        for chunk in reader:
            # chunk debe ser bytes (si tu BatchReader devuelve str, encode acá)
            if not isinstance(chunk, (bytes, bytearray)):
                chunk = str(chunk).encode("utf-8")
            self._sendall(sock, chunk)
            total_sent += len(chunk)

        # Señalamos fin de escritura del archivo (manteniendo lectura para ACK)
        try:
            sock.shutdown(socket.SHUT_WR)
        except Exception:
            pass

        # Esperamos ACK del servidor (OK\n)
        ack = self._recv_line(sock)
        logging.debug("ACK recibido para %s: %r", rel_path, ack)
        if not ack or not ack.startswith(self.success_resp):
            raise RuntimeError(f"ACK inválido para {rel_path}: {ack!r}")

        logging.info("Archivo enviado OK: %s (%d bytes; último batch=%d bytes)",
                     rel_path, total_sent, len(reader.last_batch or b""))

    def _send_finished(self, sock: socket.socket) -> None:
        """
        Mensaje opcional para indicar fin del lote/directorio.
        Útil si el server soporta múltiples archivos por conexión.
        """
        msg = (f"{self.finished_header}{self.finished_body}{self._delim_str()}").encode("utf-8")
        try:
            self._sendall(sock, msg)
        except Exception as e:
            logging.debug("Fallo al enviar FIN opcional: %r (puede no ser necesario)", e)

    # ---------------------------
    # Utilitarios de red
    # ---------------------------
    def _open_socket(self) -> socket.socket:
        sock = socket.create_connection((self.server_host, self.server_port), timeout=self.connect_timeout)
        if self.io_timeout is not None:
            sock.settimeout(self.io_timeout)
        logging.debug("Conexión abierta a %s:%s", self.server_host, self.server_port)
        return sock

    def _maybe_handshake(self, sock: socket.socket) -> None:
        if not self.handshake_enabled:
            return
        hello = f"{self.handshake_hello} {self.id}{self._delim_str()}".encode("utf-8")
        self._sendall(sock, hello)
        # Intentamos leer 1 línea; si no responde, seguimos (server puede no implementar handshake)
        try:
            line = self._recv_line(sock)
            logging.debug("Handshake respuesta: %r", line)
            if line and not line.startswith(self.success_resp):
                logging.warning("Handshake no devolvió OK; continúo de todos modos: %r", line)
        except Exception as e:
            logging.debug("Handshake sin respuesta (%r); continúo", e)

    def _sendall(self, sock: socket.socket, data: bytes) -> None:
        view = memoryview(data)
        while view:
            sent = sock.send(view)
            view = view[sent:]

    def _recv_line(self, sock: socket.socket) -> bytes:
        """
        Lee hasta el delimitador de mensaje (por defecto '\\n').
        """
        buf = bytearray()
        delim = self.message_delim
        while True:
            b = sock.recv(1)
            if not b:
                break
            buf += b
            if buf.endswith(delim):
                break
        return bytes(buf).rstrip(b"\r\n")

    # ---------------------------
    # Utilitarios de archivos
    # ---------------------------
    def _list_files(self, root: str) -> List[Tuple[str, str]]:
        """
        Devuelve lista de (abs_path, rel_path) ordenada alfabéticamente.
        Filtra por extensiones si self.allowed_exts no está vacía.
        """
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
        files.sort(key=lambda t: t[1])  # orden por ruta relativa
        return files

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

    def _delim_str(self) -> str:
        try:
            return self.message_delim.decode("utf-8")
        except Exception:
            return "\n"
