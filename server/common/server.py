# server/common/server.py
import os
import socket
import threading
import logging
from typing import Dict, Tuple, Optional
from utils.communication.socket_utils import ensure_socket, recv_exact, sendall
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.result_batch_reader import ResultBatchReader
from utils.communication.file_chunk import FileChunk, FileChunkHeader
from utils.file_utils.table_type import TableType, ResultTableType
from utils.file_utils.end_messages import MessageEnd, MessageQueryEnd
from middleware.middleware_interface import MessageMiddlewareQueue, TIMEOUT

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
H_ID_Q1_RESULT: int = 5 # Resultado de Query 1
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

    def __init__(self, port: int, listen_backlog: int, max_number_of_chunks_in_batch: int) -> None:
        self.port = int(port)
        self.listen_backlog = int(listen_backlog)
        self.host = DEFAULT_BIND_IP
        self.number_of_chunks_per_file_per_client = {}
        self.middleware_queue_senders = {}
        self.max_number_of_chunks_in_batch = max_number_of_chunks_in_batch
        self.middleware_queue_senders["to_filter_1"] = MessageMiddlewareQueue("rabbitmq", "to_filter_1")
        self.middleware_queue_senders["to_join_stores"] = MessageMiddlewareQueue("rabbitmq", "to_join_stores")
        self.middleware_queue_senders["to_join_users"] = MessageMiddlewareQueue("rabbitmq", "to_join_users")
        self.middleware_queue_senders["to_join_menu_items"] = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
        self.middleware_queue_senders["to_top3"] = MessageMiddlewareQueue("rabbitmq", "to_top3")
        self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
        # Diccionario para mantener threads activos de clientes  
        self.client_threads = {}  # client_id -> thread
        # Lock para acceso seguro a estructuras compartidas
        self.clients_lock = threading.Lock()
        
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

                # Crear thread dedicado para cada cliente
                t = threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True)
                t.start()
                self._threads.append(t)
                self._threads = [th for th in self._threads if th.is_alive()]
        finally:
            self.__graceful_shutdown()

    # ---------------------------------------------------------------------

    def _handle_client(self, sock: socket.socket, addr: Tuple[str, int]) -> None:
        """
        Thread dedicado para manejar un cliente específico.
        Mantiene el socket abierto y escucha resultados después del finish.
        """
        peer = f"{addr[0]}:{addr[1]}"
        client_id = None
        
        try:
            sock.settimeout(DEFAULT_IO_TIMEOUT)
            logging.info("action: client_connected | peer:%s", peer)

            # -------- Handshake obligatorio --------
            self._do_handshake(sock)

            # -------- Recepción de archivos --------
            files_received = 0
            while True:
                header = self._recv_header_id(sock)
                if header is None:
                    break

                if header == H_ID_DATA:
                    # Recibe y procesa chunk
                    client_id = self._handle_file_chunks(sock, peer)
                    files_received += 1
                    # ACK por chunk
                    sendall(sock, self.header_id_to_bytes(H_ID_OK))
                    continue

                elif header == H_ID_FINISH:
                    logging.info("action: recv_finished | peer:%s | client_id:%s | files:%d", 
                               peer, client_id, files_received)
                    for table_type, count in self.number_of_chunks_per_file_per_client[client_id].items():
                        message = MessageEnd(client_id, table_type=table_type, count=count).encode()
                        logging.info("action: sending_end_message | peer:%s | client_id:%s | table_type:%s | count:%d", 
                                   peer, client_id, table_type.name, count)
                        if table_type == TableType.TRANSACTIONS or table_type == TableType.TRANSACTION_ITEMS:
                            self.middleware_queue_senders["to_filter_1"].send(message)
                        elif table_type == TableType.STORES:
                            self.middleware_queue_senders["to_join_stores"].send(message)
                            self.middleware_queue_senders["to_top3"].send(message)
                        elif table_type == TableType.USERS:
                            self.middleware_queue_senders["to_join_users"].send(message)
                        elif table_type == TableType.MENU_ITEMS:
                            self.middleware_queue_senders["to_join_menu_items"].send(message)
                        
                    sendall(sock, self.header_id_to_bytes(H_ID_OK))
                    
                    if client_id is not None:
                        logging.info("action: waiting_for_results | peer:%s | client_id:%s", peer, client_id)
                        self._listen_and_send_results(sock, client_id, peer)
                    break
                
                else:
                    logging.warning("action: unknown_header | peer:%s | header:%s", peer, header)
                    break

        except Exception as e:
            logging.error("action: client_handler_error | peer:%s | client_id:%s | error:%r", peer, client_id, e)
        finally:
            # Cleanup
            current_thread = threading.current_thread()
            with self.clients_lock:
                if client_id is not None and client_id in self.client_threads:
                    del self.client_threads[client_id]
                    logging.info("action: client_thread_cleanup | peer:%s | client_id:%s", peer, client_id)
            
            try:
                fd = sock.fileno()
            except Exception:
                fd = "unknown"
            try:
                sock.close()
                logging.debug("action: fd_close | result: success | kind: client_socket | fd:%s", fd)
            except Exception:
                pass
            
            logging.info("action: client_thread_finished | peer:%s | client_id:%s | thread:%s", 
                        peer, client_id, current_thread.name)

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

    def _handle_file_chunks(self, sock: socket.socket, peer: str) -> int:
        """
        Recibe y procesa un FileChunk del cliente.
        Retorna el client_id.
        """
        # Recibir el FileChunk
        chunk = FileChunk.recv(sock)
        client_id = chunk.client_id()
        
        logging.debug("action: recv_file_chunk | cli_id:%s | file:%s | bytes:%s ", client_id, chunk.path(), chunk.payload_size())
        
        # Deserializar el batch para convertirlo en ProcessChunk
        process_chunk = ProcessBatchReader.from_file_rows(chunk.payload(), chunk.path(), client_id)
        
        # Enrutar según el tipo de tabla
        table_type = process_chunk.table_type()
        
        if table_type == TableType.TRANSACTIONS or table_type == TableType.TRANSACTION_ITEMS:
            logging.debug("action: send_to_filter1 | peer:%s | cli_id:%s | file:%s | table:%s",
                         peer, client_id, chunk.path(), table_type)
            self.middleware_queue_senders["to_filter_1"].send(process_chunk.serialize())

        elif table_type == TableType.STORES:
            logging.debug("action: send_to_join_stores | peer:%s | cli_id:%s | file:%s | table:%s",
                         peer, client_id, chunk.path(), table_type)
            self.middleware_queue_senders["to_join_stores"].send(process_chunk.serialize())
            
            logging.debug("action: send_to_top3 | peer:%s | cli_id:%s | file:%s | table:%s",
                         peer, client_id, chunk.path(), table_type)
            self.middleware_queue_senders["to_top3"].send(process_chunk.serialize())

        elif table_type == TableType.USERS:
            logging.debug("action: send_to_join_users | peer:%s | cli_id:%s | file:%s | table:%s",
                         peer, client_id, chunk.path(), table_type)
            self.middleware_queue_senders["to_join_users"].send(process_chunk.serialize())

        elif table_type == TableType.MENU_ITEMS:
            logging.debug("action: send_to_join_menu_items | peer:%s | cli_id:%s | file:%s | table:%s",
                         peer, client_id, chunk.path(), table_type)
            self.middleware_queue_senders["to_join_menu_items"].send(process_chunk.serialize())
        
        else:
            logging.warning("action: unknown_table_type | peer:%s | cli_id:%s | file:%s | table:%s",
                           peer, client_id, chunk.path(), table_type)
        
        if table_type in (TableType.TRANSACTIONS, TableType.TRANSACTION_ITEMS, TableType.STORES, TableType.USERS, TableType.MENU_ITEMS):
            if client_id not in self.number_of_chunks_per_file_per_client:
                self.number_of_chunks_per_file_per_client[client_id] = {}
            if table_type not in self.number_of_chunks_per_file_per_client[client_id]:
                self.number_of_chunks_per_file_per_client[client_id][table_type] = 0
            self.number_of_chunks_per_file_per_client[client_id][table_type] += 1

        return client_id
    
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

    def _listen_and_send_results(self, sock: socket.socket, client_id: int, peer: str) -> None:
        """
        Escucha resultados de la cola to_merge_data para este cliente específico.
        Envía resultados en lotes y detecta fin automáticamente.
        """
        
        maximum_chunks = self._max_number_of_chunks_in_batch()
        all_data_received = False
        all_data_received_per_query = {
            ResultTableType.QUERY_1: False,  # No habilitado
            ResultTableType.QUERY_2_1: True,  # Habilitar Query 2.1 (productos más vendidos)
            ResultTableType.QUERY_2_2: True,  # Habilitar Query 2.2 (productos más rentables)
            ResultTableType.QUERY_3: True,
            ResultTableType.QUERY_4: True,
        }
        results_for_client = []
        middleware_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        number_of_chunks_received = {
            ResultTableType.QUERY_1: 0,
            ResultTableType.QUERY_2_1: 0,
            ResultTableType.QUERY_2_2: 0,
            ResultTableType.QUERY_3: 0,
            ResultTableType.QUERY_4: 0,
        }
        chunks_received = {
            ResultTableType.QUERY_1: [],
            ResultTableType.QUERY_2_1: [],
            ResultTableType.QUERY_2_2: [],
            ResultTableType.QUERY_3: [],
            ResultTableType.QUERY_4: [],
        }
        expected_total_chunks = {
            ResultTableType.QUERY_1: None,
            ResultTableType.QUERY_2_1: None,
            ResultTableType.QUERY_2_2: None,
            ResultTableType.QUERY_3: None,
            ResultTableType.QUERY_4: None,
        }

        def callback(msg):
            results_for_client.append(msg)

        def stop():
            middleware_queue.stop_consuming()

        while not all_data_received:
            middleware_queue.connection.call_later(TIMEOUT, stop)
            middleware_queue.start_consuming(callback)

            for msg in list(results_for_client):
                try:
                    if msg.startswith(b"QUERY_END;"):
                        query_end_message = MessageQueryEnd.decode(msg)
                        query = query_end_message.query()
                        total_chunks = query_end_message.total_chunks()
                        expected_total_chunks[query] = total_chunks

                        if number_of_chunks_received[query] == total_chunks:
                            all_data_received_per_query[query] = True
                            logging.info(
                                "action: all_data_received_for_query | client_id:%s | query:%s",
                                client_id,
                                query.name,
                            )
                            if chunks_received[query]:
                                self._send_batch_results_to_client(sock, client_id, chunks_received[query])
                                chunks_received[query] = []
                    else:
                        result_chunk = ResultBatchReader.from_bytes(msg)
                        query = result_chunk.query_type()
                        number_of_chunks_received[query] += 1
                        chunks_received[query].append(result_chunk)

                        if len(chunks_received[query]) >= maximum_chunks:
                            self._send_batch_results_to_client(sock, client_id, chunks_received[query])
                            chunks_received[query] = []
                            logging.info(f"action: result_sent | client_id:{client_id} | rows:{len(result_chunk.rows)} | query:{query.name}")

                        if expected_total_chunks[query] is not None and number_of_chunks_received[query] == expected_total_chunks[query]:
                            all_data_received_per_query[query] = True
                            logging.info(f"action: all_data_received_for_query | client_id:{client_id} | query:{query.name}")
                            if chunks_received[query]:
                                self._send_batch_results_to_client(sock, client_id, chunks_received[query])
                                chunks_received[query] = []

                except Exception as e:
                    logging.error("Unexpected error decoding result msg | client_id:%s | error:%r", client_id, e)
                finally:
                    if msg in results_for_client:
                        results_for_client.remove(msg)

            all_data_received = all(all_data_received_per_query.values())

        sendall(sock, self.header_id_to_bytes(H_ID_FINISH))
        logging.info("action: results_finished_signal_sent | client_id:%s", client_id)
        

    def _max_number_of_chunks_in_batch(self) -> int:
        with self.clients_lock:
            return self.max_number_of_chunks_in_batch

    def _send_batch_results_to_client(self, sock: socket.socket, client_id: int, results: list) -> None:
        """
        Envía los resultados al cliente como ProcessChunk directamente.
        Cada resultado se envía como el chunk completo serializado.
        """
        try:
            for i, result_chunk in enumerate(results):
                # Enviar el ProcessChunk serializado directamente
                results_data = result_chunk.serialize()
                
                # Enviar header + datos del chunk
                sendall(sock, self.header_id_to_bytes(H_ID_DATA))
                sendall(sock, results_data)

                logging.info(f"action: result_chunk_sent | client_id:{client_id} | chunk:{i+1}/{len(results)} | bytes:{len(results_data)} | query:{result_chunk.query_type().name}")

        except Exception as e:
            logging.error(f"action: send_batch_results_error | client_id:{client_id} | error:{e}")
            raise

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
        
        # Cerrar server socket
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

        # Esperar a que terminen los threads de cliente
        with self.clients_lock:
            active_threads = list(self.client_threads.values())
        
        for thread in active_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        # Esperar threads generales
        alive = [t for t in self._threads if t.is_alive()]
        for t in alive:
            t.join(timeout=30)
            
        logging.info("action: server_shutdown | result: success")
