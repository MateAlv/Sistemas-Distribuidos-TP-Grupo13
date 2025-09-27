import os
import socket
import logging
import threading
from .utils import (deserialize_batch, store_bets, load_winning_bets, BATCH_SEPARATOR)

# File lock for thread-safe file operations
_FILE_LOCK = threading.Lock()

# Message protocol constants (two-line messages everywhere)
MESSAGE_DELIMITER = b"\n"

BATCH_PREFIX    = "S:"   # header: S:<count>
FINISHED_PREFIX = "F:"   # header: F:1          | body: FINISHED
WINNERS_PREFIX  = "W:"   # header: W:<count>    | body: dni1~dni2 or 'N'
RESP_PREFIX     = "R:"   # header: R:1          | body: OK/FAIL

OK_BODY         = "OK"
FAIL_BODY       = "FAIL"
FINISHED_BODY   = "FINISHED"

class Server:
    def __init__(self, port, listen_backlog):
        self.port = port
        self.listen_backlog = listen_backlog
        
        # Socket setup
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        logging.debug(
            f"action: fd_open | result: success | kind: listen_socket | fd:{self._server_socket.fileno()} | port:{port}"
        )
        
        # Server state
        self._running = True
        
        # Thread management
        self.client_threads = []
        
        # Get expected agencies from environment
        expected_agencies = int(os.getenv('CLI_CLIENTS', '5'))

        # Barrier and Locks for lottery synchronization
        self.lottery_barrier = threading.Barrier(expected_agencies)
        
        logging.info(f"action: server_init | result: success | expected_agencies: {expected_agencies}")
        

    def run(self):
        """Server loop - accept connections and spawn threads"""
        logging.info("action: accept_connections | result: in_progress")
        
        try:
            while self._running:
                try:
                    client_sock, addr = self._server_socket.accept()
                    if not client_sock:
                        logging.error("action: accept_connection | result: fail | reason: null_socket")
                        continue
                    logging.debug(
                        f"action: fd_open | result: success | kind: client_socket | fd:{client_sock.fileno()} | peer:{addr[0]}:{addr[1]}"
                    )
                    logging.debug(f"action: accept_connection | result: success | ip: {addr[0]}")
                    
                    # Create thread to handle client
                    client_thread = threading.Thread(
                        target=self.__handle_client_connection,
                        args=(client_sock,)
                    )
                    client_thread.daemon = False  # Must finish properly
                    client_thread.start()
                    
                    # Track active threads
                    self.client_threads.append(client_thread)
                    
                    # Cleanup finished threads
                    self.__cleanup_finished_threads()
                    
                except OSError:
                    # Socket closed during shutdown
                    if self._running:
                        logging.error("action: accept_connection | result: fail")
                    break
                    
        except Exception as e:
            logging.error(f"action: server_loop | result: fail | error: {e}")
        finally:
            self.__graceful_shutdown()

    def __cleanup_finished_threads(self):
        """Remove finished threads from tracking list"""
        self.client_threads = [t for t in self.client_threads if t.is_alive()]
        
    def __handle_client_connection(self, client_sock):
        """Handle communication with a connected client"""
        client_agency = None
        
        try:
            while True:
                # Receive a complete message (header + body)         
                header, body = self.__receive_two_lines(client_sock)
                if header.startswith(BATCH_PREFIX):
                    # header = "S:<n>", body = "bet1~...~betN"
                    bets = deserialize_batch(f"{header}\n{body}")
                    if bets:
                        client_agency = bets[0].agency
                        with _FILE_LOCK:
                            store_bets(bets)
                        logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
                    self.__send_message(client_sock, f"{RESP_PREFIX}1", OK_BODY)

                elif header.startswith(FINISHED_PREFIX) and body == FINISHED_BODY:
                    # header = "F:1", body = "FINISHED"
                    self.__handle_finished_and_return_winners(client_sock, client_agency)
                    break

                else:
                    logging.warning(f"action: unknown_message | header: {header} | body: {body}")
                    self.__send_message(client_sock, f"{RESP_PREFIX}1", FAIL_BODY)
                    
        except Exception as e:
            logging.error(f"action: client_handler_error | error: {e}")
        finally:
            try:
                fd = client_sock.fileno()
            except Exception:
                fd = "unknown"
            try:
                client_sock.close()
                logging.debug(f"action: fd_close | result: success | kind: client_socket | fd:{fd}")
            except Exception:
                pass

    def __handle_finished_and_return_winners(self, client_sock, client_agency):
        """Handle FINISHED message - wait for lottery and return winners"""
        winning_dnis = [] 
        try:
            try:
                # Wait for all agencies to reach this point
                index = self.lottery_barrier.wait(timeout=120)
                if index == 0:  # First past barrier logs lottery
                    logging.info("action: sorteo | result: success")
            except Exception as e:
                logging.error(f"action: lottery_barrier_error | error: {e}")

            # Lottery is complete - load and return winners for this agency - THREAD SAFETY FIRST
            with _FILE_LOCK:
                winning_dnis = load_winning_bets(client_agency)

            if winning_dnis:
                header = f"{WINNERS_PREFIX}{len(winning_dnis)}"  # e.g., "W:3"
                body   = BATCH_SEPARATOR.join(winning_dnis)      # "dni1~dni2~dni3"
            else:
                header = f"{WINNERS_PREFIX}0"                    # "W:0"
                body   = "N"                                     # "N"

            self.__send_message(client_sock, header, body)

            logging.info(
                f"action: winners_sent | result: success | agency: {client_agency} | cant_ganadores: {len(winning_dnis)}"
            )

        except Exception as e:
            logging.error(f"action: client_handler_error | error: {e}")
            try:
                self.__send_message(client_sock, f"{RESP_PREFIX}1", FAIL_BODY)  # "R:1\nFAIL\n"
            except:
                pass

    def __receive_line(self, client_sock, initial_buffer=b""):
        """
        Read bytes from the socket until a newline ('\n') is found.
        Returns (decoded_line, remaining_buffer).
        """
        buffer = initial_buffer
        while b"\n" not in buffer:
            chunk = client_sock.recv(1024)
            if not chunk:
                raise OSError("Connection closed before complete line was received")
            buffer += chunk

        newline_idx = buffer.find(b"\n")
        line = buffer[:newline_idx].decode("utf-8").strip()
        rest = buffer[newline_idx + 1:]
        return line, rest

    def __receive_two_lines(self, client_sock):
        """
        Receive exactly two lines (header + body) from the socket.
        """
        header, rest = self.__receive_line(client_sock)
        body, _ = self.__receive_line(client_sock, initial_buffer=rest)
        return header, body

    def __send_message(self, client_sock, header: str, body: str):
        """
        Send two lines (header + body) ensuring all bytes are written.
        Example:
        "W:2\n12345678~87654321\n"
        "W:0\nN\n"
        """
        full = f"{header}\n{body}\n".encode("utf-8")
        total_sent = 0
        while total_sent < len(full):
            sent = client_sock.send(full[total_sent:])
            if sent == 0:
                raise OSError("Socket connection broken")
            total_sent += sent

    def _begin_shutdown(self, signum, frame):
        """
        Handle shutdown signal

        If the server receives a SIGTERM signal, this handler ensures it
        starts the shutdown process.
        """
        logging.info("action: sigterm_received | result: success")
        self._running = False
        try:
            self.lottery_barrier.abort()
        except Exception:
            pass
        if self._server_socket:
            try:
                fd = self._server_socket.fileno()
            except Exception:
                fd = "unknown"
            try:
                self._server_socket.close()
                logging.debug(f"action: fd_close | result: success | kind: listen_socket | fd:{fd}")
            except Exception as e:
                logging.warning(f"action: listen_close | result: fail | error:{e}")

    def __graceful_shutdown(self):
        """Wait for all client threads to finish"""
        logging.info("action: shutdown | result: in_progress")

        # Stop accepting new connections
        self._running = False
        
        # Close server socket if not already closed
        try:
            if self._server_socket:
                try:
                    fd = self._server_socket.fileno()
                except Exception:
                    fd = "unknown"
                self._server_socket.close()
                logging.debug(f"action: fd_close | result: success | kind: listen_socket | fd:{fd}")
        except Exception:
            pass
            
        # Wait for all client threads to complete their work
        active_threads = [t for t in self.client_threads if t.is_alive()]
        if active_threads:
            logging.info(f"action: waiting_for_threads | count: {len(active_threads)}")
            
            for thread in active_threads:
                thread.join(timeout=30)  # Wait max 30 seconds per thread
                if thread.is_alive():
                    logging.warning("action: thread_timeout | result: warning")

        logging.info("action: server_shutdown | result: success")