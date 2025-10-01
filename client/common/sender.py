# common/sender.py
from logging import log

import socket
from utils.communication.socket_utils import ensure_socket, sendall, recv_exact
import logging
from typing import Optional

# ============================
# Protocolo (constantes)
# ============================

# Identificadores de control
H_ID_HANDSHAKE: int = 1  # Handshake HELLO
H_ID_DATA: int = 2    # File header
H_ID_FINISH: int = 3  # Finished
H_ID_OK: int = 4      # OK genérico
# ----------------------------
# Delimitador de mensajes
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
    
    def send_handshake_request(self, client_id: str) -> None:
        """
        Envía 'H'. Espera 'O'.
        """
        ensure_socket(self._sock)
        
        logging.debug("send_handshake_request | id=%s", client_id)
        hello = self.header_id_to_bytes(H_ID_HANDSHAKE)
        sendall(self._sock, hello)
        logging.debug("handshake_sent | id=%s | size=%d", client_id, len(hello))
        header = self._recv_header_id(self._sock)
        logging.debug("handshake_recv | id=%s", client_id)
        
        
        if header != H_ID_OK:
            raise RuntimeError(f"Handshake inválido, esperaba {H_ID_OK}, recibí {header!r}")
        
        logging.debug("handshake_ok | id=%s", client_id)
    
    # -------- Envío de batches --------
    def send_file_chunk(self, data: bytes) -> None:
        """Envía un chunk del cuerpo binario del archivo."""
        ensure_socket(self._sock)
        if not data:
            return
        logging.debug("send_file_chunk | size=%s", len(data))
        data = self.header_id_to_bytes(H_ID_DATA) + data
        sendall(self._sock, data)

    def wait_end_file_ack(self) -> None:
        """
        Espera un ACK H_ID_OK tras enviar el archivo completo.
        """
        ensure_socket(self._sock)
        
        header = self._recv_header_id(self._sock)
        
        if header != H_ID_OK:
            raise RuntimeError(f"ACK inválido, esperaba {H_ID_OK}, recibí {header!r}")
        logging.debug("file_ack_ok")

    # -------- Fin de directorio --------

    def send_finished(self) -> None:
        """Envía H_ID_FINISH y espera un H_ID_OK (si llega)."""
        ensure_socket(self._sock)
        sendall(self._sock, self.header_id_to_bytes(H_ID_FINISH))
        logging.debug("finished_sent")
        try:
            header = self._recv_header_id(self._sock)
            if header == H_ID_OK:
                logging.debug("finished_ack_ok")
            else:
                logging.warning("finished_ack_unexpected | recv=%r", header)
        except Exception as e:
            logging.debug("finished_no_response | err=%r", e)

    # ---------------- Internos ----------------

    def _recv_header_id(self, sock: socket.socket) -> int:
        """
        Lee exactamente un byte de cabecera.
        Devuelve None si EOF limpio sin datos.
        """
        ensure_socket(sock)
        b = recv_exact(sock, 1)
        header_int = self.header_id_from_bytes(b)
        return header_int

    def header_id_to_bytes(self, header: int) -> bytes:
        if not isinstance(header, int):
            raise TypeError(f"header debe ser int, no {type(header).__name__}")
        if not (0 <= header <= 255):
            raise ValueError(f"header fuera de rango [0,255]: {header}")
        return header.to_bytes(1, byteorder='big')
    
    def header_id_from_bytes(self, data: bytes) -> int:
        return int.from_bytes(data, byteorder='big')