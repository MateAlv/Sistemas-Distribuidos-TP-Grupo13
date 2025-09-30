# server/common/server.py
from __future__ import annotations

import os
import socket
import threading
import logging
from typing import Dict, Tuple, Optional

# Delimitadores / framing
_MESSAGE_DELIM = b"\n"
_HEADER_BLANKLINE = b"\n\n"

# Bind/Timeouts
DEFAULT_BIND_IP = os.getenv("SERVER_IP", "0.0.0.0")
DEFAULT_IO_TIMEOUT = 120.0

# Persistencia opcional de archivos recibidos
SAVE_DIR = os.getenv("SERVER_SAVE_DIR", "").strip()  # vacío => no guarda

class Server:
    """
    Server TCP que habla el protocolo 'I:*' de tu cliente:

      Handshake:
        C -> S:  I:H <id>\\n
        S -> C:  I:O\\n

      Envío de archivos (múltiples por conexión):
        C -> S:  F:\\n
                 CLI_ID: <id>\\n
                 FILENAME: <rel_path>\\n
                 SIZE: <size>\\n
                 \\n
                 <size bytes>
        S -> C:  I:O\\n     (ACK por archivo)

      Fin de stream:
        C -> S:  I:F\\n
        S -> C:  I:O\\n
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

            # -------- Handshake obligatorio según protocolo I:* --------
            self._do_handshake(sock)

            # -------- Loop de recepción de archivos / fin --------
            while True:
                headers = self._recv_headers(sock)
                if headers is None:
                    logging.info("action: client_eof | peer:%s", peer)
                    break

                # Fin explícito: 'I:F'
                if headers.get("I") == "F":
                    logging.info("action: client_finished | peer:%s", peer)
                    self._send_line(sock, b"I:O")
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

                received = 0
                if SAVE_DIR:
                    safe_path = os.path.normpath(fname).lstrip("/\\")
                    out_path = os.path.join(SAVE_DIR, safe_path)
                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    with open(out_path, "wb") as f:
                        received = self._recv_exact(sock, size, sink=f.write)
                else:
                    """
                    # Recive el tamaño del batch (int) y luego los datos del batch
                    batch_size = self._recv_exact(sock, 4)
                    payload_received = self._recv_exact(sock, batch_size)

                    # Deserializa el batch recibido para convertirlo en objeto ProcessBatch
                    process_batch = ProcessBatch.from_file_rows(payload_received[12:], fname, cli_id)
                    # Aquí puedes procesar el objeto process_batch según sea necesario
                    if process_batch.table_type() == TableType.TRANSACTIONS or process_batch.table_type() == TableType.TRANSACTIONS_ITEMS:
                        # Envia al filtro 1
                        pass
                    elif process_batch.table_type() == TableType.STORES:
                        # Envia al join Stores 
                        # Envia al TOP 3
                        pass
                    elif process_batch.table_type() == TableType.USERS:
                        # Envia al join Users
                        pass
                    elif process_batch.table_type() == TableType.MENU_ITEMS:
                        # Envia al join MenuItems
                        pass
                    """
                    
                logging.info("action: recv_file_end | peer:%s | cli_id:%s | file:%s | bytes:%s",
                             peer, cli_id, fname, received)

                # ACK por archivo (protocolo I:O)
                self._send_line(sock, b"I:O")

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
    # Handshake / Lectura / Escritura
    # ---------------------------------------------------------------------

    def _do_handshake(self, sock: socket.socket) -> None:
        """
        Espera línea 'I:H <id>\\n' y responde 'I:O\\n'.
        Si no llega a tiempo o llega otra cosa, se considera protocolo inválido.
        """
        sock.settimeout(3.0)  
        line = self._recv_line(sock)
        sock.settimeout(DEFAULT_IO_TIMEOUT)

        if not line:
            raise RuntimeError("handshake_empty")

        try:
            text = line.decode("utf-8", errors="replace")
        except Exception:
            text = ""

        if not text.startswith("I:H"):
            raise RuntimeError(f"handshake_invalid: {text!r}")

        logging.debug("action: handshake_ok | line:%r", text)
        self._send_line(sock, b"I:O")

    def _recv_headers(self, sock: socket.socket) -> Optional[Dict[str, str]]:
        """
        Lee cabeceras hasta blank line ('\\n\\n') o una sola línea 'I:F\\n'.
        Devuelve dict con keys en mayúsculas. None si EOF limpio sin datos.
        """
        buf = bytearray()
        raw: Optional[str] = None

        while True:
            chunk = sock.recv(1)
            if not chunk:
                if not buf:
                    return None
                break
            buf += chunk

            # Caso fin: 'I:F\\n'
            if buf.endswith(_MESSAGE_DELIM) and buf.strip() == b"I:F":
                raw = "I:F"
                break

            # Caso header con blank line
            if buf.endswith(_HEADER_BLANKLINE):
                raw = buf.decode("utf-8", errors="replace")
                break

        if raw is None:
            raw = buf.decode("utf-8", errors="replace")

        # Fin explícito
        if raw.strip() == "I:F":
            return {"I": "F"}

        headers: Dict[str, str] = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().upper()] = v.strip()
            elif line == "F:":
                # se ignora la línea 'F:' de apertura (sin valor)
                continue
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
