from utils.file_utils.table_type import TableType, ResultTableType
from utils.file_utils.process_table import TransactionItemsProcessRow, PurchasesPerUserStoreRow, TPVProcessRow
from utils.file_utils.result_table import Query2_1ResultRow, Query2_2ResultRow, Query3ResultRow, Query4ResultRow
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.result_chunk import ResultChunk, ResultChunkHeader
from utils.file_utils.file_table import DateTime
from utils.file_utils.end_messages import MessageEnd, MessageQueryEnd
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
import logging
from collections import defaultdict
import datetime
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

        self.define_queues()

    def handle_join_data(self):
        """Maneja datos de join (tabla de productos del server)"""
        results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_join_receiver.stop_consuming()

        while True:
            # Escuchar datos de join (productos)
            self.data_join_receiver.connection.call_later(TIMEOUT, stop)
            self.data_join_receiver.start_consuming(callback)

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
                    logging.error(f"action: error_parsing_join_data | type:{self.joiner_type} | error:{e}")
                except Exception as e:
                    logging.error(f"action: unexpected_error_join_data | type:{self.joiner_type} | error:{e}")

                results.remove(data)
                
    def handle_data(self):
        """Maneja datos del maximizer y END messages del exchange"""
        results = []
        end_results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()

        while True:
            # Escuchar datos del maximizer con timeout
            self.data_receiver.connection.call_later(TIMEOUT, stop)
            self.data_receiver.start_consuming(callback)

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
                                    self.apply_for_client(client_id)
                                    self.publish_results(client_id)
                                    self.clean_client_data(client_id)
                                    self.completed_clients.append(client_id)
                                    self._pending_end_messages.append(client_id)
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
                                # Aplica el join
                                self.apply_for_client(client_id)
                                # Publica los resultados al to_merge_data
                                self.publish_results(client_id)
                                # Limpiar datos del cliente después de procesar
                                self.clean_client_data(client_id)
                                # Mark this client as processed
                                self.completed_clients.append(client_id)
                                self._pending_end_messages.append(client_id)
                            else:
                                logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{client_id} | file_type:{chunk.table_type()}")
                    
                    # Enviar END messages para clientes recién completados
                    if hasattr(self, '_pending_end_messages'):
                        for client_id in self._pending_end_messages:
                            self.send_end_query_msg(client_id)
                        self._pending_end_messages.clear()
                    
                except ValueError as e:
                    logging.error(f"action: error_parsing_data | type:{self.joiner_type} | error:{e}")
                except Exception as e:
                    logging.error(f"action: unexpected_error | type:{self.joiner_type} | error:{e}")
 
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
    
    def apply(self, client_id) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        logging.info(f"action: joining_data | type:{self.joiner_type} | cli_id:{client_id} | type:{self.joiner_type}")
        # Hacer el join entre self.data y self.joiner_data
        rows = self.data.get(client_id, [])
        if not client_id in self.joiner_results:
            self.joiner_results[client_id] = []
        for row in rows:
            self.joiner_results[client_id].append(self.join_result(row, client_id))
        logging.info(f"action: joiner_result | type:{self.joiner_type} | results_out: Data: {self.data} - Joiner_data: {self.joiner_data}")
        return True
    
    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        # Iniciar hilos para manejar data y join_data (eliminamos end_message_handler_thread)
        data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")
        
        data_handler_thread.start() 
        join_data_handler_thread.start()
    
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
            
        for chunk in self.joiner_data_chunks[client_id]:
            for row in chunk.rows:
                joined_row = self.join_result(row, client_id)
                if client_id not in self.joiner_results:
                    self.joiner_results[client_id] = []
                self.joiner_results[client_id].append(joined_row)
        
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

class MenuItemsJoiner(Joiner):
    
    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.item_id] = row.name
    
    def join_result(self, row: TableProcessRow, client_id):
        # Extraer mes/año de created_at
        if hasattr(row.created_at, 'date'):
            date_obj = row.created_at.date
            month_year = f"{date_obj.month:02d}-{date_obj.year}"
        else:
            month_year = "UNKNOWN"
            
        result = {
            "item_id": row.item_id,
            "item_name": self.joiner_data[client_id].get(row.item_id, "UNKNOWN"),
            "quantity": row.quantity,
            "subtotal": row.subtotal,
            "month_year": month_year
        }
        return result
    
    def send_end_query_msg(self, client_id):
        end_query_msg_1 = MessageQueryEnd(client_id, ResultTableType.QUERY_2_1, 1)
        end_query_msg_2 = MessageQueryEnd(client_id, ResultTableType.QUERY_2_2, 1)

        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        client_queue.send(end_query_msg_1.encode())
        client_queue.send(end_query_msg_2.encode())

    def publish_results(self, client_id):
        sellings_results = []
        profit_results = []
        joiner_results = self.joiner_results.get(client_id, [])
        for row in joiner_results:
            # INCLUIR CLIENT_ID EN LOS RESULTADOS
            row["client_id"] = client_id
            item_id = row["item_id"]  # Agregar esta línea
            item_name = row["item_name"]
            year_month = row["month_year"]
            if row["quantity"] is not None:
                sellings_quantity = row["quantity"]
                max_selling = Query2_1ResultRow(item_id, item_name, sellings_quantity, year_month)
                sellings_results.append(max_selling)
            if row["subtotal"] is not None:
                profit_sum = row["subtotal"]
                max_profit = Query2_2ResultRow(item_id, item_name, profit_sum, year_month)
                profit_results.append(max_profit)
            
        if not sellings_results and not profit_results:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")
            return
        
        # Enviar a cola específica del cliente
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        
        if sellings_results:
            sellings_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_1)
            sellings_chunk = ResultChunk(sellings_header, sellings_results)
            client_queue.send(sellings_chunk.serialize())
            logging.info(f"action: sent_selling_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(sellings_results)}")

        if profit_results:
            profit_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_2)  # Corregir TableType
            profit_chunk = ResultChunk(profit_header, profit_results)
            client_queue.send(profit_chunk.serialize())
            logging.info(f"action: sent_profit_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(profit_results)}")
        
        client_queue.close()
        logging.info(f"action: sent_result_message | type:{self.joiner_type} | client_id:{client_id}")
    
class StoresTpvJoiner(Joiner):
    
    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_stores")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.store_id] = row.store_name
    
    def join_result(self, row: TableProcessRow, client_id):
        # Soporte para TPVProcessRow
        if hasattr(row, 'tpv') and hasattr(row, 'year_half'):
            # TPVProcessRow
            result = {
                "store_id": row.store_id,
                "store_name": self.joiner_data[client_id].get(row.store_id, "UNKNOWN"),
                "tpv": row.tpv,
                "year_half": row.year_half,
            }
        else:
            # Fallback para TransactionsProcessRow (compatibilidad)
            result = {
                "store_id": row.store_id,
                "store_name": self.joiner_data[client_id].get(row.store_id, "UNKNOWN"),
                "tpv": row.final_amount,
                "year_half": row.year_half_created_at,
            }
        return result
    
    def publish_results(self, client_id):
        joiner_results = self.joiner_results.get(client_id, [])
        query3_results = []
        for row in joiner_results:
            store_id = row["store_id"]
            store_name = row["store_name"]
            tpv = row["tpv"]
            year_half = row["year_half"]
            query3_result = Query3ResultRow(store_id, store_name, tpv, year_half)
            query3_results.append(query3_result)
        
        if query3_results:
            query3_header = ResultChunkHeader(client_id, ResultTableType.QUERY_3)
            query3_chunk = ResultChunk(query3_header, query3_results)
            self.data_sender.send(query3_chunk.serialize())
            logging.info(f"action: sent_result_message | type:{self.joiner_type}")
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")
    
    def send_end_query_msg(self, client_id):
        end_query_msg_3 = MessageQueryEnd(client_id, ResultTableType.QUERY_3, 1)
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        client_queue.send(end_query_msg_3.encode())
        client_queue.close()
        logging.info(f"action: sent_end_query_message | type:{self.joiner_type} | client_id:{client_id}")
            
class StoresTop3Joiner(Joiner):
    
    def define_queues(self):
        # Recibe del TOP3 absoluto
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_purchases_joiner")
        # Recibe datos de stores del servidor
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_stores")
        # Envía al UsersJoiner
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")

    def save_data_join_fields(self, row, client_id):
        # Guarda mapping store_id -> store_name
        if hasattr(row, 'store_id') and hasattr(row, 'store_name'):
            self.joiner_data[client_id][row.store_id] = row.store_name
            logging.debug(f"action: save_store_data | store_id:{row.store_id} | store_name:{row.store_name}")
    
    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de stores).
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        # Inicializar diccionario para este cliente si no existe
        if client_id not in self.joiner_data:
            self.joiner_data[client_id] = {}
            
        # Guardar mapping store_id → store_name
        for row in rows:
            if hasattr(row, 'store_id') and hasattr(row, 'store_name'):
                self.joiner_data[client_id][row.store_id] = row.store_name
                logging.debug(f"action: save_stores_join_data | type:{self.joiner_type} | store_id:{row.store_id} | store_name:{row.store_name}")
            else:
                logging.warning(f"action: invalid_stores_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_store_id:{hasattr(row, 'store_id')} | has_store_name:{hasattr(row, 'store_name')}")
            
        logging.info(f"action: saved_stores_join_data | type:{self.joiner_type} | client_id:{client_id} | stores_loaded:{len(self.joiner_data[client_id])}")
        return True
    
    def join_result(self, row: TableProcessRow, client_id):
        # Procesar PurchasesPerUserStoreRow del TOP3 absoluto
        if isinstance(row, PurchasesPerUserStoreRow):
            store_id = row.store_id
            store_name = self.joiner_data[client_id].get(store_id, f"UNKNOWN_STORE_{store_id}")
            
            # Crear nueva fila con store_name llenado
            joined_row = PurchasesPerUserStoreRow(
                store_id=store_id,
                store_name=store_name,  # ¡Ahora con el nombre real!
                user_id=row.user_id,
                user_birthdate=row.user_birthdate,  # Sigue siendo placeholder
                purchases_made=row.purchases_made
            )
            
            logging.debug(f"action: joined_store_data | store_id:{store_id} | store_name:{store_name} | user_id:{row.user_id} | purchases:{row.purchases_made}")
            return joined_row
        else:
            logging.warning(f"action: unexpected_row_type | expected:PurchasesPerUserStoreRow | got:{type(row)}")
            return row
    
    def send_end_query_msg(self, client_id):
        # Envía END message al UsersJoiner
        try:
            end_msg = MessageEnd(client_id, TableType.PURCHASES_PER_USER_STORE, 1)
            self.data_sender.send(end_msg.encode())
            logging.info(f"action: sent_end_to_users_joiner | client_id:{client_id}")
        except Exception as e:
            logging.error(f"action: error_sending_end_to_users_joiner | error:{e}")
    
    def publish_results(self, client_id):
        # Envía PurchasesPerUserStoreRow con store_name llenado al UsersJoiner
        joiner_results = self.joiner_results.get(client_id, [])
        
        if joiner_results:
            # Crear chunk con las filas que tienen store_name llenado
            from utils.file_utils.process_chunk import ProcessChunkHeader
            header = ProcessChunkHeader(client_id, TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, joiner_results)
            
            self.data_sender.send(chunk.serialize())
            logging.info(f"action: sent_to_users_joiner | type:{self.joiner_type} | client_id:{client_id} | rows:{len(joiner_results)}")
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")

class UsersJoiner(Joiner):
    
    def define_queues(self):
        # Recibe del StoresTop3Joiner
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")
        # Recibe datos de users del servidor
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_users")
        # Envía resultados finales
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        # Guarda mapping user_id -> birthdate
        if hasattr(row, 'user_id') and hasattr(row, 'birthdate'):
            self.joiner_data[client_id][row.user_id] = row.birthdate
            logging.debug(f"action: save_user_data | user_id:{row.user_id} | birthdate:{row.birthdate}")
    
    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de users).
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        
        # Inicializar diccionario para este cliente si no existe
        if client_id not in self.joiner_data:
            self.joiner_data[client_id] = {}
            
        # Guardar mapping user_id → birthdate
        for row in rows:
            if hasattr(row, 'user_id') and hasattr(row, 'birthdate'):
                self.joiner_data[client_id][row.user_id] = row.birthdate
                # logging.debug(f"action: save_user_join_data | type:{self.joiner_type} | user_id:{row.user_id} | birthdate:{row.birthdate}")
            else:
                logging.warning(f"action: invalid_users_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_user_id:{hasattr(row, 'user_id')} | has_birthdate:{hasattr(row, 'birthdate')}")
            
        logging.info(f"action: saved_users_join_data | type:{self.joiner_type} | client_id:{client_id} | users_loaded:{len(self.joiner_data[client_id])}")
        return True
    
    def send_end_query_msg(self, client_id):
        # Envía END message final para Query 4
        try:
            end_query_msg = MessageQueryEnd(client_id, ResultTableType.QUERY_4, 1)
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            client_queue.send(end_query_msg.encode())
            client_queue.close()
            logging.info(f"action: sent_end_query_4 | client_id:{client_id}")
        except Exception as e:
            logging.error(f"action: error_sending_end_query_4 | error:{e}")
    
    def join_result(self, row: TableProcessRow, client_id):
        # Procesar PurchasesPerUserStoreRow del StoresTop3Joiner
        if isinstance(row, PurchasesPerUserStoreRow):
            user_id = row.user_id
            birthdate = self.joiner_data[client_id].get(user_id, None)
            
            if birthdate is None:
                logging.warning(f"action: user_not_found | user_id:{user_id} | using_placeholder")
                birthdate = "UNKNOWN"
            
            # Crear resultado final para Query 4
            result = Query4ResultRow(
                store_id=row.store_id,
                store_name=row.store_name,
                user_id=user_id,
                birthdate=birthdate,
                purchase_quantity=row.purchases_made
            )
            
            logging.debug(f"action: joined_user_data | store_id:{row.store_id} | store_name:{row.store_name} | user_id:{user_id} | birthdate:{birthdate} | purchases:{row.purchases_made}")
            return result
        else:
            logging.warning(f"action: unexpected_row_type | expected:PurchasesPerUserStoreRow | got:{type(row)}")
            return None

    def publish_results(self, client_id):
        # Envía resultados finales de Query 4
        joiner_results = self.joiner_results.get(client_id, [])
        
        # Filtrar resultados None
        query4_results = [result for result in joiner_results if result is not None]
        
        if query4_results:
            # Enviar a cola específica del cliente
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            
            query4_header = ResultChunkHeader(client_id, ResultTableType.QUERY_4)
            query4_chunk = ResultChunk(query4_header, query4_results)
            
            client_queue.send(query4_chunk.serialize())
            client_queue.close()
            
            logging.info(f"action: sent_query4_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(query4_results)}")
        else:
            logging.info(f"action: no_query4_results_to_send | type:{self.joiner_type} | client_id:{client_id}")