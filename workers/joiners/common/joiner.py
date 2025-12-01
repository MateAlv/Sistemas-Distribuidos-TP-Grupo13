
from utils.processing.process_table import TableProcessRow
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.eof_protocol.end_messages import MessageEnd, MessageForceEnd
from middleware.middleware_interface import MessageMiddlewareMessageError
import logging
import threading
import sys
import os
import pickle
import uuid
from utils.tolerance.persistence_service import PersistenceService

TIMEOUT = 3

ITEMS_JOINER = "ITEMS"
STORES_TPV_JOINER = "STORES_TPV"
STORES_TOP3_JOINER = "STORES_TOP3"
USERS_JOINER = "USERS"

from .joiner_working_state import JoinerMainWorkingState, JoinerJoinWorkingState

class Joiner:
    
    def __init__(self, join_type: str, monitor=None):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        self.monitor = monitor

        self.joiner_type = join_type
        
        # State is now managed by WorkingState classes
        self.working_state_main = JoinerMainWorkingState()
        self.working_state_join = JoinerJoinWorkingState()

        self.lock = threading.Lock()
        self.data_receiver = None
        self.data_join_receiver = None
        self.data_sender = None
        self.data_receiver_timer = None
        self.data_join_receiver_timer = None

        self.__running = True
        
        # Iniciar hilos para manejar data y join_data (eliminamos end_message_handler_thread)
        self.data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        self.join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")

        # Persistence
        # Main persistence (Maximizer data)
        base_dir = f"data/persistence/joiner_{self.joiner_type}_{self.id}" if hasattr(self, "id") else f"data/persistence/joiner_{self.joiner_type}"
        self.persistence_main = PersistenceService(directory=os.path.join(base_dir, "main"))
        
        # Join persistence (Server data)
        self.persistence_join = PersistenceService(directory=os.path.join(base_dir, "join"))

        self.define_queues()

    def _save_state_main(self, last_processed_id):
        self.persistence_main.commit_working_state(self.working_state_main.to_bytes(), last_processed_id)

    def _save_state_join(self, last_processed_id):
        self.persistence_join.commit_working_state(self.working_state_join.to_bytes(), last_processed_id)

    def _recover_state(self):
        # Recover Main State
        state_data_main = self.persistence_main.recover_working_state()
        if state_data_main:
            try:
                self.working_state_main = JoinerMainWorkingState.from_bytes(state_data_main)
                logging.info(f"Main state recovered for joiner {self.joiner_type}")
            except Exception as e:
                logging.error(f"Error recovering main state: {e}")

        # Recover Join State
        state_data_join = self.persistence_join.recover_working_state()
        if state_data_join:
            try:
                self.working_state_join = JoinerJoinWorkingState.from_bytes(state_data_join)
                logging.info(f"Join state recovered for joiner {self.joiner_type}")
            except Exception as e:
                logging.error(f"Error recovering join state: {e}")

    def handle_processing_recovery(self):
        self._recover_state()
        
        # Recover Main Chunk
        last_chunk_main = self.persistence_main.recover_last_processing_chunk()
        if last_chunk_main:
            logging.info("Recovering last main chunk...")
            self._handle_data_chunk(last_chunk_main.serialize())

        # Recover Join Chunk
        last_chunk_join = self.persistence_join.recover_last_processing_chunk()
        if last_chunk_join:
            logging.info("Recovering last join chunk...")
            # We need a method to handle join chunk bytes similar to _handle_data_chunk
            self._handle_join_chunk_bytes(last_chunk_join.serialize())

    def handle_join_data(self):
        """Maneja datos de join (tabla de productos del server)"""
        results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_join_receiver.stop_consuming()

        while self.__running:
            # Escuchar datos de join (productos)
            self.data_join_receiver_timer = self.data_join_receiver.connection.call_later(TIMEOUT, stop)
            try:
                self.data_join_receiver.start_consuming(callback)
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo: {e}")

            for data in results:
                try:
                    if data.startswith(b"END;"):
                        end_message = MessageEnd.decode(data)
                        logging.info(f"action: received_server_end_message_for_join_data | type:{self.joiner_type} | client_id:{end_message.client_id()} | table_type:{end_message.table_type()} | count:{end_message.total_chunks()}")
                        with self.lock:
                            self.working_state_main.set_ready_to_join(end_message.client_id)
                    else:
                        self._handle_join_chunk_bytes(data)
                except ValueError as e:
                    logging.error(f"action: error_parsing_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)

                results.remove(data)

    def _handle_join_chunk_bytes(self, data: bytes):
        chunk = ProcessBatchReader.from_bytes(data)
        
        with self.lock:
            # Idempotency
            if self.working_state_join.is_processed(chunk.message_id()):
                return

            self.persistence_join.commit_processing_chunk(chunk)
            
            logging.info(f"action: receive_join_table_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
            self.save_data_join(chunk)
            
            self.working_state_join.add_processed_id(chunk.message_id())
            self._save_state_join(chunk.message_id())
                
    def handle_data(self):
        """Maneja datos del maximizer y END messages del exchange"""
        results = []
        end_results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()

        while self.__running:

            # Escuchar datos del maximizer con timeout
            try:
                self.data_receiver_timer = self.data_receiver.connection.call_later(TIMEOUT, stop)
                self.data_receiver.start_consuming(callback)
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo: {e}")

            # Procesar datos del maximizer
            for data in results:

                try:
                    if data.startswith(b"FORCE_END;"):
                        force_end = MessageForceEnd.decode(data)
                        client_id = force_end.client_id()
                        logging.info(f"action: received_force_end_message | type:{self.joiner_type} | client_id:{client_id}")
                        
                        with self.lock:
                            self.working_state_main.clean_client_data(client_id)
                            logging.debug(f"action: force_end_processing_complete | type:{self.joiner_type} | client_id:{client_id}")
                        
                    if data.startswith(b"END;"):
                        end_message = MessageEnd.decode(data)
                        client_id = end_message.client_id()
                        table_type = end_message.table_type()
                        
                        with self.lock:
                            # Solo agregar el client_id si no está ya en la lista de END messages recibidos
                            if not self.working_state_main.is_end_message_received(client_id):
                                self.working_state_main.mark_end_message_received(client_id)
                                logging.info(f"action: received_end_message | type:{self.joiner_type} | client_id:{client_id} | table_type:{table_type} | count:{end_message.total_chunks()}")
                                
                                # Solo procesar si está listo para join
                                if self.is_ready_to_join_for_client(client_id):
                                    logging.info(f"action: ready_to_join_after_end | type:{self.joiner_type} | client_id:{client_id}")
                                    try:
                                        logging.debug(f"action: starting_apply_for_client | type:{self.joiner_type} | client_id:{client_id}")
                                        self.apply_for_client(client_id)
                                        logging.debug(f"action: completed_apply_for_client | type:{self.joiner_type} | client_id:{client_id}")
                                        
                                        logging.debug(f"action: starting_publish_results | type:{self.joiner_type} | client_id:{client_id}")
                                        self.publish_results(client_id)
                                        logging.debug(f"action: completed_publish_results | type:{self.joiner_type} | client_id:{client_id}")
                                        
                                        logging.debug(f"action: starting_clean_client_data | type:{self.joiner_type} | client_id:{client_id}")
                                        self.clean_client_data(client_id)
                                        logging.debug(f"action: completed_clean_client_data | type:{self.joiner_type} | client_id:{client_id}")
                                        
                                        self.working_state_main.mark_client_completed(client_id)
                                        self.working_state_main.add_pending_end_message(client_id)
                                        logging.debug(f"action: processing_complete | type:{self.joiner_type} | client_id:{client_id}")
                                    except Exception as inner_e:
                                        logging.error(f"action: error_in_join_processing | type:{self.joiner_type} | client_id:{client_id} | error:{inner_e} | error_type:{type(inner_e).__name__}")
                                        raise inner_e
                            else:
                                logging.debug(f"action: duplicate_end_message_ignored | type:{self.joiner_type} | client_id:{client_id}")

                    else:
                        self._handle_data_chunk(data)
                    
                    # Enviar END messages para clientes recién completados
                    pending_msgs = self.working_state_main.get_pending_end_messages()
                    if pending_msgs:
                        for client_id in pending_msgs:
                            try:
                                logging.debug(f"action: starting_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                                self.send_end_query_msg(client_id)
                                logging.debug(f"action: completed_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                            except Exception as end_msg_error:
                                logging.error(f"action: error_sending_end_message | type:{self.joiner_type} | client_id:{client_id} | error:{end_msg_error} | error_type:{type(end_msg_error).__name__}")
                                raise end_msg_error
                        self.working_state_main.clear_pending_end_messages()
                        logging.debug(f"action: cleared_pending_end_messages | type:{self.joiner_type}")
                    
                except ValueError as e:
                    logging.error(f"action: error_parsing_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)
 
                results.remove(data)

    def _handle_data_chunk(self, data: bytes):
        self._check_crash_point("CRASH_BEFORE_PROCESS")
        chunk = ProcessBatchReader.from_bytes(data)
        
        with self.lock:
            # Idempotency
            if self.working_state_main.is_processed(chunk.message_id()):
                return

            self.persistence_main.commit_processing_chunk(chunk)

            logging.info(f"action: receive_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                        
        with self.lock:
            self.save_data(chunk)
            # Check if we're ready to join for this specific client
            client_id = chunk.client_id()
            if self.is_ready_to_join_for_client(client_id):
                logging.info(f"action: ready_to_join | type:{self.joiner_type} | client_id:{client_id}")
                try:
                    logging.debug(f"action: starting_apply_for_client_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    # Aplica el join
                    self.apply_for_client(client_id)
                    logging.debug(f"action: completed_apply_for_client_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    
                    self._check_crash_point("CRASH_AFTER_PROCESS_BEFORE_COMMIT")

                    logging.debug(f"action: starting_publish_results_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    # Publica los resultados al to_merge_data
                    self.publish_results(client_id)
                    logging.debug(f"action: completed_publish_results_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    
                    logging.debug(f"action: starting_clean_client_data_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    # Limpiar datos del cliente después de procesar
                    self.clean_client_data(client_id)
                    logging.debug(f"action: completed_clean_client_data_from_data | type:{self.joiner_type} | client_id:{client_id}")
                    
                    # Mark this client as processed
                    self.working_state_main.mark_client_completed(client_id)
                    self.working_state_main.add_pending_end_message(client_id)
                    logging.debug(f"action: processing_complete_from_data | type:{self.joiner_type} | client_id:{client_id}")
                except Exception as inner_e:
                    logging.error(f"action: error_in_join_processing_from_data | type:{self.joiner_type} | client_id:{client_id} | error:{inner_e} | error_type:{type(inner_e).__name__}")
                    raise inner_e
            
            else:
                logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{client_id} | file_type:{chunk.table_type()}")

            self.working_state_main.add_processed_id(chunk.message_id())
            self._save_state_main(chunk.message_id())

    def _check_crash_point(self, point_name):
        if os.environ.get("CRASH_POINT") == point_name:
            logging.critical(f"Simulating crash at {point_name}")
            sys.exit(1)

    def save_data(self, chunk) -> bool:
        """
        Guarda los datos para la tabla que debe joinearse.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        self.working_state_main.add_data(client_id, rows)
        self.working_state_main.add_chunk(client_id, chunk)
        
        return True

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de productos).
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        # Guardar mapping item_id → nombre
        for row in rows:
            if hasattr(row, 'item_id') and hasattr(row, 'item_name'):
                self.working_state_join.add_join_data(client_id, row.item_id, row.item_name)
                logging.debug(f"action: save_join_data | type:{self.joiner_type} | item_id:{row.item_id} | name:{row.item_name}")
            else:
                logging.warning(f"action: invalid_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_item_id:{hasattr(row, 'item_id')} | has_item_name:{hasattr(row, 'item_name')}")
            
        logging.info(f"action: saved_join_data | type:{self.joiner_type} | client_id:{client_id} | products_loaded:{self.working_state_join.get_join_data_count(client_id)}")
        return True

    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        self.handle_processing_recovery()
        
        self.data_handler_thread.start() 
        self.join_data_handler_thread.start()

    def define_queues(self):
        raise NotImplementedError("Subclasses must implement define_queues method")

    def save_data_join_fields(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement save_data_join_fields method")
    
    def join_result(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement join_result method")
    
    def publish_results(self):
        raise NotImplementedError("Subclasses must implement publish_results method")
    
    def send_end_query_msg(self, client_id):
        raise NotImplementedError("Subclasses must implement send_end_query_msg method")
    
    def send_force_end_msg(self, client_id):
        raise NotImplementedError("Subclasses must implement send_force_end_msg method")
    
    def is_ready_to_join_for_client(self, client_id):
        """Check if we have received all necessary data and end messages for this client"""
        # Check if we have data from maximizers for this client
        has_maximizer_data = self.working_state_main.has_chunks(client_id)
        # Check if we have received end message for this client  
        has_end_message = self.working_state_main.is_end_message_received(client_id)
        # Check if we haven't already processed this client
        not_processed = not self.working_state_main.is_client_completed(client_id)
        
        # Detailed logging to debug join readiness
        logging.debug(f"action: checking_join_readiness | type:{self.joiner_type} | client_id:{client_id} | has_maximizer_data:{has_maximizer_data} | has_end_message:{has_end_message} | not_processed:{not_processed}")
        
        if has_maximizer_data and has_end_message and not_processed:
            logging.info(f"action: ready_to_join | client_id:{client_id} | has_data:{has_maximizer_data} | has_end:{has_end_message}")
        else:
            if not has_maximizer_data:
                logging.debug(f"action: not_ready_to_join | reason:no_maximizer_data | client_id:{client_id}")
            if not has_end_message:
                logging.debug(f"action: not_ready_to_join | reason:no_end_message | client_id:{client_id}")
            if not not_processed:
                logging.debug(f"action: not_ready_to_join | reason:already_processed | client_id:{client_id}")
        
        return has_maximizer_data and has_end_message and not_processed
    
    def apply_for_client(self, client_id):
        """Apply join operation for a specific client"""
        if not self.working_state_main.has_chunks(client_id):
            logging.warning(f"action: no_data_to_join | client_id:{client_id}")
            return
            
        chunks = self.working_state_main.get_chunks(client_id)
        logging.debug(f"action: processing_chunks | type:{self.joiner_type} | client_id:{client_id} | chunks_count:{len(chunks)}")
        
        for chunk_idx, chunk in enumerate(chunks):

            logging.debug(f"action: processing_chunk | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | rows_count:{len(chunk.rows)}")
            
            for row_idx, row in enumerate(chunk.rows):
                try:
                    logging.debug(f"action: joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                    joined_row = self.join_result(row, client_id)
                    
                    self.working_state_main.add_result(client_id, joined_row)
                    
                    logging.debug(f"action: row_joined_successfully | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                except Exception as row_error:
                    logging.error(f"action: error_joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx} | error:{row_error} | error_type:{type(row_error).__name__}")
                    raise row_error
        
        logging.info(f"action: applied_join | client_id:{client_id} | joined_rows:{len(self.working_state_main.get_results(client_id))}")
    
    def clean_client_data(self, client_id):
        """Clean client data after processing to prevent reprocessing"""
        self.working_state_main.clean_client_data(client_id)
        logging.info(f"action: client_data_cleaned | type:{self.joiner_type} | client_id:{client_id}")
    
    def reset_for_new_session(self):
        """Reset joiner state for a new processing session (new client batch)"""
        with self.lock:
            self.working_state_main.reset()
            logging.info(f"action: joiner_reset_for_new_session | type:{self.joiner_type}")

    def shutdown(self, signum=None, frame=None):
        logging.info(f"SIGTERM recibido: cerrando joiner {self.joiner_type}")

        try:
            if self.data_receiver_timer is not None:
                self.stats_timer.cancel()
        except Exception:
            pass

        try:
            if self.data_join_receiver_timer is not None:
                self.consume_timer.cancel()
        except Exception:
            pass

        # Detener consumos
        try:
            self.data_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_join_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Cerrar conexiones
        try:
            self.data_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_join_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_sender.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Detener bucle de manejo de datos
        self.__running = False

        # Esperar hilos
        try:
            if self.data_handler_thread.is_alive():
                self.data_handler_thread.join(timeout=5)
        except (OSError, RuntimeError, AttributeError):
            pass

        try:
            if self.data_handler_thread.is_alive():
                self.join_data_handler_thread.join(timeout=5)
        except (OSError, RuntimeError, AttributeError):
            pass

        # Limpiar estructuras
        try:
            self.working_state_main.reset()
            self.working_state_join.reset()
        except Exception:
            pass

        logging.info(f"Joiner {self.joiner_type} cerrado correctamente.")