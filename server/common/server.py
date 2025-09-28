# server/common/server.py
from __future__ import annotations

import os
import socket
import threading
import logging
from typing import Dict, Tuple, Optional

_MESSAGE_DELIM = b"\n"
_HEADER_BLANKLINE = b"\n\n"

DEFAULT_BIND_IP = os.getenv("SERVER_IP", "0.0.0.0")
DEFAULT_CONNECT_TIMEOUT = float(os.getenv("SERVER_CONNECT_TIMEOUT", "10"))
DEFAULT_IO_TIMEOUT = float(os.getenv("SERVER_IO_TIMEOUT", "120"))
SAVE_DIR = os.getenv("SERVER_SAVE_DIR", "").strip()  # si vacío => no guarda a disco


class Server:
    """
    Server TCP que soporta:
      - Handshake opcional: 'HELLO <id>\\n' -> 'OK\\n'
      - Múltiples archivos por conexión:
          Header (CLI_ID/FILENAME/SIZE + blank line) y luego SIZE bytes
          Responde 'OK\\n' por cada archivo
      - Mensaje final opcional: 'F:FINISHED\\n'
    """

    def __init__(self, port: int, listen_backlog: int) -> None:
        self.port = int(port)
        self.listen_backlog = int(listen_backlog)
        self.host = DEFAULT_BIND_IP

        self._running = True
        self._server_socket: Optional[socket.socket] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog)
        logging.debug(
            "action: fd_open | result: success | kind: listen_socket | fd:%s | ip:%s | port:%s",
            self._server_socket.fileno(), self.host, self.port
        )

        self._threads = []

        if SAVE_DIR:
            try:
                os.makedirs(SAVE_DIR, exist_ok=True)
                logging.info("action: save_dir_ready | dir: %s", SAVE_DIR)
            except Exception as e:
                logging.warning("action: save_dir_create_fail | dir: %s | error: %r", SAVE_DIR, e)

    # ---------------------------------------------------------------------

    def run(self) -> None:
        logging.info("action: accept_loop | result: start | ip:%s | port:%s | backlog:%s",
                     self.host, self.port, self.listen_backlog)
        try:
            while self._running:
                try:
                    client_sock, addr = self._server_socket.accept()
                except OSError as e:
                    if self._running:
                        logging.error("action: accept_fail | error: %r", e)
                    break

                if not client_sock:
                    logging.error("action: accept_null_socket")
                    continue

                logging.debug("action: fd_open | result: success | kind: client_socket | fd:%s | peer:%s:%s",
                              client_sock.fileno(), addr[0], addr[1])

                t = threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True)
                t.start()
                self._threads.append(t)
                self._threads = [th for th in self._threads if th.is_alive()]
        finally:
            self.__graceful_shutdown()

    # ---------------------------------------------------------------------

    def _handle_client(self, sock: socket.socket, addr: Tuple[str, int]) -> None:
        peer = f"{addr[0]}:{addr[1]}"
        try:
            sock.settimeout(DEFAULT_IO_TIMEOUT)
            logging.info("action: client_connected | peer:%s", peer)

            # -------- Handshake opcional --------
            self._maybe_handshake(sock)

            # -------- Loop de recepción de archivos --------
            while True:
                headers = self._recv_headers(sock)
                if headers is None:
                    # EOF limpio del cliente
                    logging.info("action: client_eof | peer:%s", peer)
                    break

                # Soporte de FIN opcional
                if len(headers) == 1 and headers.get("F", "") == "FINISHED":
                    logging.info("action: client_finished | peer:%s", peer)
                    self._send_line(sock, b"OK")
                    continue

                cli_id = headers.get("CLI_ID", "?")
                fname = headers.get("FILENAME", "unknown")
                size_s = headers.get("SIZE", "0")
                try:
                    size = int(size_s)
                except Exception:
                    size = 0

                logging.info("action: recv_file_start | peer:%s | cli_id:%s | file:%s | size:%s",
                             peer, cli_id, fname, size)

                # Recibir exactamente 'size' bytes
                received = 0
                if SAVE_DIR:
                    # Guardar en disco preservando subdirectorios
                    safe_path = os.path.normpath(fname).lstrip("/\\")
                    out_path = os.path.join(SAVE_DIR, safe_path)
                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    with open(out_path, "wb") as f:
                        received = self._recv_exact(sock, size, sink=f.write)
                else:
                    received = self._recv_exact(sock, size)  # descarta

                logging.info("action: recv_file_end | peer:%s | cli_id:%s | file:%s | bytes:%s",
                             peer, cli_id, fname, received)

                # ACK por archivo
                self._send_line(sock, b"OK")

        except Exception as e:
            logging.error("action: client_handler_error | peer:%s | error:%r", peer, e)
        finally:
            try:
                fd = sock.fileno()
            except Exception:
                fd = "unknown"
            try:
                sock.close()
                logging.debug("action: fd_close | result: success | kind: client_socket | fd:%s", fd)
            except Exception:
                pass

    # ---------------------------------------------------------------------
    # Lectura / Escritura de protocolo
    # ---------------------------------------------------------------------

    def _maybe_handshake(self, sock: socket.socket) -> None:
        """
        Si el cliente envía inmediatamente 'HELLO <id>\\n', respondemos 'OK\\n'.
        Si llega otra cosa (o tiempo), seguimos normal (no obligatorio).
        """
        sock.settimeout(1.5)  # breve timeout para handshake
        try:
            line = self._recv_line(sock)
        except socket.timeout:
            # No hubo handshake; seguimos
            sock.settimeout(DEFAULT_IO_TIMEOUT)
            return
        except Exception:
            sock.settimeout(DEFAULT_IO_TIMEOUT)
            return

        sock.settimeout(DEFAULT_IO_TIMEOUT)

        if not line:
            return

        try:
            text = line.decode("utf-8", errors="replace")
        except Exception:
            text = ""

        if text.startswith("HELLO"):
            logging.debug("action: handshake_ok | line:%r", text)
            self._send_line(sock, b"OK")
        else:
            # No era handshake: probablemente ya sea el comienzo de headers
            # reinyectamos en un buffer local simulando que no lo consumimos.
            # Para simplificar, si no es HELLO, asumimos que era ruido y seguimos.
            logging.debug("action: handshake_skip | line:%r", text)

    def _recv_headers(self, sock: socket.socket) -> Optional[Dict[str, str]]:
        """
        Lee cabeceras tipo:
            KEY: VALUE\\n
            ...
            \\n
        Devuelve dict con keys en mayúsculas.
        Si el cliente cerró la conexión en silencio, devuelve None.
        """
        buf = bytearray()
        while True:
            chunk = sock.recv(1)
            if not chunk:
                # conexión cerrada; si no hay nada, es EOF limpio
                if not buf:
                    return None
                break
            buf += chunk
            if buf.endswith(_HEADER_BLANKLINE):
                break

        raw = buf.decode("utf-8", errors="replace")
        headers: Dict[str, str] = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().upper()] = v.strip()
            elif line.startswith("F:"):
                # Soporte de 'F:FINISHED'
                headers["F"] = line[2:].strip()
        return headers

    def _recv_exact(self, sock: socket.socket, nbytes: int, *, sink=None) -> int:
        """
        Lee exactamente nbytes del socket. Si 'sink' es una función (p. ej. f.write),
        envía cada chunk allí; si no, descarta. Devuelve bytes recibidos.
        """
        remaining = nbytes
        total = 0
        CHUNK = 64 * 1024
        while remaining > 0:
            to_read = CHUNK if remaining > CHUNK else remaining
            data = sock.recv(to_read)
            if not data:
                raise OSError(f"connection closed early; remaining={remaining}")
            total += len(data)
            remaining -= len(data)
            if sink:
                sink(data)
        return total

    def _recv_line(self, sock: socket.socket) -> bytes:
        buf = bytearray()
        while True:
            b = sock.recv(1)
            if not b:
                break
            buf += b
            if buf.endswith(_MESSAGE_DELIM):
                break
        return bytes(buf).rstrip(b"\r\n")

    def _send_line(self, sock: socket.socket, line: bytes) -> None:
        """Envía `line + \\n` asegurando escritura completa."""
        data = line + _MESSAGE_DELIM
        view = memoryview(data)
        while view:
            sent = sock.send(view)
            if sent == 0:
                raise OSError("socket send returned 0")
            view = view[sent:]

    # ---------------------------------------------------------------------

    def _begin_shutdown(self, signum, frame) -> None:
        logging.info("action: sigterm_received | result: success")
        self._running = False
        if self._server_socket:
            try:
                fd = self._server_socket.fileno()
            except Exception:
                fd = "unknown"
            try:
                self._server_socket.close()
                logging.debug("action: fd_close | result: success | kind: listen_socket | fd:%s", fd)
            except Exception as e:
                logging.warning("action: listen_close_fail | error:%r", e)

    def __graceful_shutdown(self) -> None:
        logging.info("action: shutdown | result: in_progress")
        self._running = False
        try:
            if self._server_socket:
                try:
                    fd = self._server_socket.fileno()
                except Exception:
                    fd = "unknown"
                self._server_socket.close()
                logging.debug("action: fd_close | result: success | kind: listen_socket | fd:%s", fd)
        except Exception:
            pass

        alive = [t for t in self._threads if t.is_alive()]
        for t in alive:
            t.join(timeout=30)
        logging.info("action: server_shutdown | result: success")
