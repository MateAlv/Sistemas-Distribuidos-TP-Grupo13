# server/common/server.py
from logging import log

import os
import socket
import threading
import logging
from typing import Dict, Tuple, Optional
from utils.communication.socket_utils import ensure_socket, recv_exact, sendall
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.communication.file_chunk import FileChunk, FileChunkHeader
from utils.file_utils.table_type import TableType


# Delimitadores / framing
_MESSAGE_DELIM = b"\n"
_HEADER_BLANKLINE = b"\n\n"

# Bind/Timeouts
DEFAULT_BIND_IP = os.getenv("SERVER_IP", "0.0.0.0")
DEFAULT_IO_TIMEOUT = 120.0

# Persistencia opcional de archivos recibidos
SAVE_DIR = os.getenv("SERVER_SAVE_DIR", "").strip()  # vacío => no guarda

# ============================
# Protocolo (constantes)
# ============================

# Identificadores de control

H_ID_HANDSHAKE: int = 1   # Handshake HELLO
H_ID_DATA: int = 2    # File header
H_ID_FINISH: int = 3  # Finished
H_ID_OK: int = 4       # OK genérico
# ----------------------------
class Server:
    """
    Server TCP que habla el protocolo de tu cliente:

      Handshake:
        C -> S:  H 
        S -> C:  O

      Envío de archivos (múltiples por conexión):
        C -> S:  F:
                 FileChunk
        S -> C:  O    (ACK por chunk)

      Fin de stream:
        C -> S:  F
        S -> C:  O
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

            # -------- Handshake obligatorio según protocolo --------
            self._do_handshake(sock)

            # -------- Loop de recepción de archivos / fin --------
            while True:
                header = self._recv_header_id(sock)
                if header is None:
                    break

                if header == H_ID_DATA:
                    logging.debug("action: recv_file_chunk_header | peer:%s", peer)
                    # Recibe y procesa el chunk
                    self._handle_file_chunks(sock)
                    # ACK por chunk
                    sendall(sock, self.header_id_to_bytes(H_ID_OK))
                    continue

                if header == H_ID_FINISH:
                    logging.info("action: recv_finished | peer:%s", peer)
                    # ACK final
                    sendall(sock, self.header_id_to_bytes(H_ID_OK))
                    break
    
                    
                logging.info("action: recv_file_end | peer:%s | cli_id:%s | file:%s | bytes:%s",
                             peer, cli_id, fname, received)

                # ACK por archivo (protocolo I:O)
                sendall(sock, self.header_id_to_bytes(H_ID_OK))

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
        Espera un solo byte 'H'. Responde 'O'.
        """
        sock.settimeout(3.0)
        logging.debug("action: handshake_wait | timeout:3s")
        header = self._recv_header_id(sock)
        logging.debug("action: handshake_recv | byte:%r", header)
        sock.settimeout(DEFAULT_IO_TIMEOUT)

        if not header:
            raise RuntimeError("handshake_empty")

        if header != H_ID_HANDSHAKE:
            raise RuntimeError(f"handshake_invalid: {header!r}")

        logging.debug("action: handshake_ok | byte:%r", header)
        sendall(sock, self.header_id_to_bytes(H_ID_OK))

    def _handle_file_chunks(self, sock: socket.socket) -> None:
        # Recive el tamaño del batch (int) y luego los datos del batch
        chunk = FileChunk.recv(sock)
        logging.info("action: recv_file_chunk | peer:%s | cli_id:%s | file:%s | bytes:%s",
                     peer, chunk.client_id(), chunk.path(), chunk.payload_size())
        # Deserializa el batch recibido para convertirlo en objeto ProcessBatch
        process_chunk = ProcessBatchReader.from_file_rows(chunk.payload(), chunk.path(), chunk.client_id())
        # Aquí puedes procesar el objeto process_chunk según sea necesario
        if process_chunk.table_type() == TableType.TRANSACTIONS or process_batch.table_type() == TableType.TRANSACTIONS_ITEMS:
            # Envia al filtro 1
            logging.info("action: send_to_filter1 | peer:%s | cli_id:%s | file:%s | bytes:%s",
                         peer, chunk.client_id(), chunk.path(), chunk.payload_size())
            pass
        elif process_chunk.table_type() == TableType.STORES:
            # Envia al join Stores 
            logging.info("action: send_to_join_stores | peer:%s | cli_id:%s | file:%s | bytes:%s",
                         peer, chunk.client_id(), chunk.path(), chunk.payload_size())
            # Envia al TOP 3
            logging.info("action: send_to_top3 | peer:%s | cli_id:%s | file:%s | bytes:%s",
                         peer, chunk.client_id(), chunk.path(), chunk.payload_size())
            pass
        elif process_chunk.table_type() == TableType.USERS:
            # Envia al join Users
            logging.info("action: send_to_join_users | peer:%s | cli_id:%s | file:%s | bytes:%s",
                         peer, chunk.client_id(), chunk.path(), chunk.payload_size())
            pass
        elif process_chunk.table_type() == TableType.MENU_ITEMS:
            # Envia al join MenuItems
            logging.info("action: send_to_join_menu_items | peer:%s | cli_id:%s | file:%s | bytes:%s",
                         peer, chunk.client_id(), chunk.path(), chunk.payload_size())
            pass
    
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
