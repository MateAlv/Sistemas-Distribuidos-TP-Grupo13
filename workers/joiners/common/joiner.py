
from utils.processing.process_table import TableProcessRow
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.eof_protocol.end_messages import MessageEnd
from middleware.middleware_interface import MessageMiddlewareMessageError
import logging
import threading

TIMEOUT = 3

ITEMS_JOINER = "ITEMS"
STORES_TPV_JOINER = "STORES_TPV"
STORES_TOP3_JOINER = "STORES_TOP3"
USERS_JOINER = "USERS"

class Joiner:
    
    def __init__(self, join_type: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)

        self.joiner_type = join_type
        self.data = {}
        self.joiner_data = {}
        self.joiner_results = {}
        self.joiner_data_chunks = {}  # Store chunks from maximizers per client
        self.client_end_messages_received = []  # Track which clients have sent end messages
        self.completed_clients = []  # Track which clients have been processed
        self._pending_end_messages = []  # Track clients that need END messages sent
        self.lock = threading.Lock()
        self.ready_to_join = {}
        self.data_receiver = None
        self.data_join_receiver = None
        self.data_sender = None
        self.data_receiver_timer = None
        self.data_join_receiver_timer = None

        self.__running = True
        
        # Iniciar hilos para manejar data y join_data (eliminamos end_message_handler_thread)
        self.data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        self.join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")

        self.define_queues()

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
                            self.ready_to_join[end_message.client_id] = True
                    else:
                        chunk = ProcessBatchReader.from_bytes(data)
                        logging.info(f"action: receive_join_table_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")

                        self.save_data_join(chunk)
                except ValueError as e:
                    logging.error(f"action: error_parsing_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error_join_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)

                results.remove(data)
                
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
                    if data.startswith(b"END;"):
                        end_message = MessageEnd.decode(data)
                        client_id = end_message.client_id()
                        table_type = end_message.table_type()
                        
                        with self.lock:
                            # Solo agregar el client_id si no está ya en la lista de END messages recibidos
                            if client_id not in self.client_end_messages_received:
                                self.client_end_messages_received.append(client_id)
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
                                        
                                        self.completed_clients.append(client_id)
                                        self._pending_end_messages.append(client_id)
                                        logging.debug(f"action: processing_complete | type:{self.joiner_type} | client_id:{client_id}")
                                    except Exception as inner_e:
                                        logging.error(f"action: error_in_join_processing | type:{self.joiner_type} | client_id:{client_id} | error:{inner_e} | error_type:{type(inner_e).__name__}")
                                        raise inner_e
                            else:
                                logging.debug(f"action: duplicate_end_message_ignored | type:{self.joiner_type} | client_id:{client_id}")

                    else:
                        chunk = ProcessBatchReader.from_bytes(data)
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
                                    
                                    logging.debug(f"action: starting_publish_results_from_data | type:{self.joiner_type} | client_id:{client_id}")
                                    # Publica los resultados al to_merge_data
                                    self.publish_results(client_id)
                                    logging.debug(f"action: completed_publish_results_from_data | type:{self.joiner_type} | client_id:{client_id}")
                                    
                                    logging.debug(f"action: starting_clean_client_data_from_data | type:{self.joiner_type} | client_id:{client_id}")
                                    # Limpiar datos del cliente después de procesar
                                    self.clean_client_data(client_id)
                                    logging.debug(f"action: completed_clean_client_data_from_data | type:{self.joiner_type} | client_id:{client_id}")
                                    
                                    # Mark this client as processed
                                    self.completed_clients.append(client_id)
                                    self._pending_end_messages.append(client_id)
                                    logging.debug(f"action: processing_complete_from_data | type:{self.joiner_type} | client_id:{client_id}")
                                except Exception as inner_e:
                                    logging.error(f"action: error_in_join_processing_from_data | type:{self.joiner_type} | client_id:{client_id} | error:{inner_e} | error_type:{type(inner_e).__name__}")
                                    raise inner_e
                            else:
                                logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{client_id} | file_type:{chunk.table_type()}")
                    
                    # Enviar END messages para clientes recién completados
                    if hasattr(self, '_pending_end_messages'):
                        for client_id in self._pending_end_messages:
                            try:
                                logging.debug(f"action: starting_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                                self.send_end_query_msg(client_id)
                                logging.debug(f"action: completed_send_end_query_msg | type:{self.joiner_type} | client_id:{client_id}")
                            except Exception as end_msg_error:
                                logging.error(f"action: error_sending_end_message | type:{self.joiner_type} | client_id:{client_id} | error:{end_msg_error} | error_type:{type(end_msg_error).__name__}")
                                raise end_msg_error
                        self._pending_end_messages.clear()
                        logging.debug(f"action: cleared_pending_end_messages | type:{self.joiner_type}")
                    
                except ValueError as e:
                    logging.error(f"action: error_parsing_data | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | data_preview:{data[:100] if len(data) > 100 else data}")
                except Exception as e:
                    logging.error(f"action: unexpected_error | type:{self.joiner_type} | error:{e} | error_type:{type(e).__name__} | error_traceback:", exc_info=True)
 
                results.remove(data)

    def save_data(self, chunk) -> bool:
        """
        Guarda los datos para la tabla que debe joinearse.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        # Se guarda en memoria
        if client_id not in self.data:
            self.data[client_id] = []
        self.data[client_id].extend(rows)
        
        # Also store chunks for the new coordination logic
        if client_id not in self.joiner_data_chunks:
            self.joiner_data_chunks[client_id] = []
        self.joiner_data_chunks[client_id].append(chunk)
        
        return True

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de productos).
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        # Inicializar diccionario para este cliente si no existe
        if client_id not in self.joiner_data:
            self.joiner_data[client_id] = {}
            
        # Guardar mapping item_id → nombre
        for row in rows:
            if hasattr(row, 'item_id') and hasattr(row, 'item_name'):
                self.joiner_data[client_id][row.item_id] = row.item_name
                logging.debug(f"action: save_join_data | type:{self.joiner_type} | item_id:{row.item_id} | name:{row.item_name}")
            else:
                logging.warning(f"action: invalid_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_item_id:{hasattr(row, 'item_id')} | has_item_name:{hasattr(row, 'item_name')}")
            
        logging.info(f"action: saved_join_data | type:{self.joiner_type} | client_id:{client_id} | products_loaded:{len(self.joiner_data[client_id])}")
        return True

    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        
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
    
    def is_ready_to_join_for_client(self, client_id):
        """Check if we have received all necessary data and end messages for this client"""
        # Check if we have data from maximizers for this client
        has_maximizer_data = client_id in self.joiner_data_chunks
        # Check if we have received end message for this client  
        has_end_message = client_id in self.client_end_messages_received
        # Check if we haven't already processed this client
        not_processed = client_id not in self.completed_clients
        
        # Detailed logging to debug join readiness
        logging.debug(f"action: checking_join_readiness | type:{self.joiner_type} | client_id:{client_id} | has_maximizer_data:{has_maximizer_data} | has_end_message:{has_end_message} | not_processed:{not_processed}")
        logging.debug(f"action: join_state_details | type:{self.joiner_type} | client_id:{client_id} | joiner_data_chunks_keys:{list(self.joiner_data_chunks.keys())} | end_messages_received:{list(self.client_end_messages_received)} | completed_clients:{list(self.completed_clients)}")
        
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
        if client_id not in self.joiner_data_chunks:
            logging.warning(f"action: no_data_to_join | client_id:{client_id}")
            return
            
        logging.debug(f"action: processing_chunks | type:{self.joiner_type} | client_id:{client_id} | chunks_count:{len(self.joiner_data_chunks[client_id])}")
        
        for chunk_idx, chunk in enumerate(self.joiner_data_chunks[client_id]):
            logging.debug(f"action: processing_chunk | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | rows_count:{len(chunk.rows)}")
            
            for row_idx, row in enumerate(chunk.rows):
                try:
                    logging.debug(f"action: joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                    joined_row = self.join_result(row, client_id)
                    
                    if client_id not in self.joiner_results:
                        self.joiner_results[client_id] = []
                    self.joiner_results[client_id].append(joined_row)
                    
                    logging.debug(f"action: row_joined_successfully | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx}")
                except Exception as row_error:
                    logging.error(f"action: error_joining_row | type:{self.joiner_type} | client_id:{client_id} | chunk_idx:{chunk_idx} | row_idx:{row_idx} | error:{row_error} | error_type:{type(row_error).__name__}")
                    raise row_error
        
        logging.info(f"action: applied_join | client_id:{client_id} | joined_rows:{len(self.joiner_results.get(client_id, []))}")
    
    def clean_client_data(self, client_id):
        """Clean client data after processing to prevent reprocessing"""
        # Limpiar chunks de datos del maximizer
        if client_id in self.joiner_data_chunks:
            del self.joiner_data_chunks[client_id]
            logging.debug(f"action: cleaned_joiner_data_chunks | client_id:{client_id}")
        
        # Limpiar datos procesados
        if client_id in self.data:
            del self.data[client_id]
            logging.debug(f"action: cleaned_data | client_id:{client_id}")
        
        # Limpiar resultados del join (opcional, pero ayuda con memoria)
        if client_id in self.joiner_results:
            del self.joiner_results[client_id]
            logging.debug(f"action: cleaned_joiner_results | client_id:{client_id}")
        
        logging.info(f"action: client_data_cleaned | type:{self.joiner_type} | client_id:{client_id}")
    
    def reset_for_new_session(self):
        """Reset joiner state for a new processing session (new client batch)"""
        with self.lock:
            self.data.clear()
            self.joiner_data_chunks.clear()
            self.joiner_results.clear()
            self.client_end_messages_received.clear()
            self.completed_clients.clear()
            self._pending_end_messages.clear()
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
        for attr in [
            "data",
            "joiner_data",
            "joiner_results",
            "joiner_data_chunks",
            "client_end_messages_received",
            "completed_clients",
            "_pending_end_messages",
            "ready_to_join"
        ]:
            try:
                obj = getattr(self, attr, None)
                if isinstance(obj, (dict, list, set)) and hasattr(obj, "clear"):
                    obj.clear()
            except (OSError, RuntimeError, AttributeError):
                pass

        logging.info(f"Joiner {self.joiner_type} cerrado correctamente.")