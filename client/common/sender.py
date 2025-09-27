# common/sender.py
from __future__ import annotations

import socket
import logging
from typing import Optional

# ============================
# Protocolo (constantes)
# ============================

# Identificadores de control
HELLO_HEADER   = "I:H"   # Handshake HELLO
FINISHED_HEADER = "I:F"  # Finished
OK_HEADER       = "I:O"  # OK genérico

# Archivos
FILE_HEADER     = "F:"   # Inicio de archivo (seguido de metadatos en líneas)
MESSAGE_DELIM   = b"\n"


class Sender:
    """
    Implementa el protocolo estandarizado:
      - I:H <id>      → I:O
      - F: + header   → body → I:O
      - I:F           → I:O
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        connect_timeout: float = 10.0,
        io_timeout: float = 30.0,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.connect_timeout = float(connect_timeout)
        self.io_timeout = float(io_timeout)
        self._sock: Optional[socket.socket] = None

    # ---------------- API pública ----------------

    def connect(self) -> None:
        if self._sock is not None:
            return
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        sock.settimeout(self.io_timeout)
        logging.debug("sender_connect | peer=%s:%s", self.host, self.port)
        self._sock = sock

    def close(self) -> None:
        if self._sock is None:
            return
        try:
            fd = self._sock.fileno()
        except Exception:
            fd = "unknown"
        try:
            self._sock.close()
            logging.debug("sender_close | fd=%s", fd)
        finally:
            self._sock = None

    # Context manager
    def __enter__(self) -> "Sender":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # -------- Handshake --------

    def send_hello(self, client_id: str) -> None:
        """
        Envía 'I:H <id>\\n'. Espera 'I:O\\n'.
        """
        self._ensure_socket()
        msg = f"{HELLO_HEADER} {client_id}\n".encode("utf-8")
        self._sendall(msg)
        line = self._recv_line()
        if line != OK_HEADER.encode("utf-8"):
            raise RuntimeError(f"Handshake inválido, esperaba {OK_HEADER}, recibí {line!r}")
        logging.debug("handshake_ok | id=%s", client_id)

    # -------- Archivos --------

    def start_file(self, client_id: str, rel_path: str, size: int) -> None:
        """
        Envía header de archivo:
            F:\n
            CLI_ID: <id>\n
            FILENAME: <rel_path>\n
            SIZE: <size>\n
            \n
        """
        self._ensure_socket()
        header = (
            f"{FILE_HEADER}\n"
            f"CLI_ID: {client_id}\n"
            f"FILENAME: {rel_path}\n"
            f"SIZE: {size}\n"
            f"\n"
        ).encode("utf-8")
        self._sendall(header)
        logging.debug("file_header_sent | file=%s size=%s", rel_path, size)

    def send_batch(self, data: bytes) -> None:
        """Envía un chunk del cuerpo binario del archivo."""
        self._ensure_socket()
        if not data:
            return
        self._sendall(data)

    def end_file_and_wait_ack(self) -> None:
        """
        Espera 'I:O\\n' tras enviar el archivo completo.
        """
        self._ensure_socket()
        line = self._recv_line()
        if line != OK_HEADER.encode("utf-8"):
            raise RuntimeError(f"ACK inválido, esperaba {OK_HEADER}, recibí {line!r}")
        logging.debug("file_ack_ok")

    # -------- Fin de directorio --------

    def send_finished(self) -> None:
        """Envía 'I:F\\n' y espera 'I:O\\n' (si llega)."""
        self._ensure_socket()
        msg = f"{FINISHED_HEADER}\n".encode("utf-8")
        self._sendall(msg)
        logging.debug("finished_sent")
        try:
            line = self._recv_line()
            if line == OK_HEADER.encode("utf-8"):
                logging.debug("finished_ack_ok")
            else:
                logging.warning("finished_ack_unexpected | recv=%r", line)
        except Exception as e:
            logging.debug("finished_no_response | err=%r", e)

    # ---------------- Internos ----------------

    def _ensure_socket(self) -> None:
        if self._sock is None:
            raise RuntimeError("Sender no conectado. Llamá a connect() primero.")

    def _sendall(self, data: bytes) -> None:
        assert self._sock is not None
        view = memoryview(data)
        while view:
            sent = self._sock.send(view)
            if sent == 0:
                raise OSError("socket send devolvió 0 (conexión rota)")
            view = view[sent:]

    def _recv_line(self) -> bytes:
        """
        Lee hasta '\\n'. Devuelve línea sin \\r\\n final.
        """
        assert self._sock is not None
        buf = bytearray()
        while True:
            b = self._sock.recv(1)
            if not b:
                break
            buf += b
            if buf.endswith(MESSAGE_DELIM):
                break
        return bytes(buf).rstrip(b"\r\n")
