from utils.file_utils.table_type import TableType, ResultTableType
from utils.file_utils.process_table import TransactionItemsProcessRow
from utils.file_utils.result_table import Query2_1ResultRow, Query2_2ResultRow, Query3ResultRow, Query4ResultRow
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.result_chunk import ResultChunk, ResultChunkHeader
from utils.file_utils.file_table import DateTime
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
        self.client_end_messages_received = set()  # Track which clients have sent end messages
        self.completed_clients = set()  # Track which clients have been processed
        self.lock = threading.Lock()
        self.ready_to_join = False # Variable compartida para indicar que se recibio el END de join data

        self.define_queues()
        
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        # El joiner NO debe reenviar END messages - solo recibirlos
        # self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
    
    def handle_join_data(self):
        
        results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()

        while True:
            # Max receive initialization
            self.data_receiver.connection.call_later(TIMEOUT, stop)
            self.data_receiver.start_consuming(callback)

            for data in results:
                chunk = ProcessBatchReader.from_bytes(data)
                logging.info(f"action: receive_join_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                self.save_data(chunk.rows, self.joiner_data)
                
                # if recibo el END:
                with self.lock:
                    # Indica que ya se puede joinear para los proximos datos que lleguen
                    self.ready_to_join = True
                    logging.info(f"action: ready_to_join | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}") 
                    # Modifica self.data -> Compartido con handle_data_thread
                    # Aplica el join sobre los datos guardados que no pudieron joinearse antes
                    self.apply()
                    self.publish_results()

                results.remove(data)
                
    def handle_data(self):
        
        results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_join_receiver.stop_consuming()

        while True:
            # Max receive initialization
            self.data_join_receiver.connection.call_later(TIMEOUT, stop)
            self.data_join_receiver.start_consuming(callback)

            for data in results:
                try:
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
                            # Mark this client as processed
                            self.completed_clients.add(client_id)
                        else:
                            logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{client_id} | file_type:{chunk.table_type()}")
                except ValueError as e:
                    if "Datos insuficientes para el header" in str(e):
                        logging.debug(f"action: received_non_batch_data | type:{self.joiner_type} | data_size:{len(data)} | skipping")
                        # Skip non-batch data (could be control messages)
                    else:
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
        Guarda los datos para la tabla base necesaria para el join.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        # Se guarda en memoria
        for row in rows:
                logging.debug(f"action: save_join_data | type:{self.joiner_type} | item_id:{row.item_id} | quantity:{row.quantity} | subtotal:{row.subtotal} | created_at:{row.created_at}")
                if client_id not in self.joiner_data:
                    self.joiner_data[client_id()] = {}
                    self.save_data_join_fields(row, client_id)
                else:
                    logging.warning(f"error: save_join_data | type:{self.joiner_type} | unexpected_row_type:{type(row)}")
            
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
        # Iniciar hilos para manejar data y join_data
        data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")
        end_message_handler_thread = threading.Thread(target=self.handle_end_messages, name="EndMessageHandler")
        
        data_handler_thread.start() 
        join_data_handler_thread.start()
        end_message_handler_thread.start()
    
    def handle_end_messages(self):
        """Handle end messages from the maximizer pipeline"""
        logging.info(f"action: start_end_message_handler | type:{self.joiner_type} | listening_on:SECOND_END_MESSAGE")
        
        def end_callback(msg):
            # Parse the client_id from the end message
            try:
                # Assume end message contains client_id in format "client_id:X"
                if msg and "client_id:" in msg.decode():
                    client_id = int(msg.decode().split("client_id:")[1])
                    with self.lock:
                        self.client_end_messages_received.add(client_id)
                        logging.info(f"action: received_end_message | type:{self.joiner_type} | client_id:{client_id}")
                else:
                    # Fallback - add all potential client_ids (this is not ideal but works for single client)
                    with self.lock:
                        # Add client_id 1 as default (should be improved to parse actual client_id)
                        self.client_end_messages_received.add(1)
                        logging.info(f"action: received_end_message_fallback | type:{self.joiner_type} | client_id:1")
            except Exception as e:
                logging.error(f"action: error_parsing_end_message | type:{self.joiner_type} | error:{e}")
                # Fallback
                with self.lock:
                    self.client_end_messages_received.add(1)
        
        # Listen for end messages without timeout - blocking until message arrives
        try:
            self.middleware_exchange_receiver.start_consuming(end_callback)
        except Exception as e:
            logging.error(f"action: end_message_handler_failed | type:{self.joiner_type} | error:{e}")
            # If listening fails, the joiner should probably exit or restart
            raise

    def define_queues(self):
        raise NotImplementedError("Subclasses must implement define_queues method")

    def save_data_join_fields(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement save_data_join_fields method")
    
    def join_result(self, row: TableProcessRow, client_id):
        raise NotImplementedError("Subclasses must implement join_result method")
    
    def publish_results(self):
        raise NotImplementedError("Subclasses must implement publish_results method")
    
    def is_ready_to_join_for_client(self, client_id):
        """Check if we have received all necessary data and end messages for this client"""
        # Check if we have data from maximizers for this client
        has_maximizer_data = client_id in self.joiner_data_chunks
        # Check if we have received end message for this client  
        has_end_message = client_id in self.client_end_messages_received
        # Check if we haven't already processed this client
        not_processed = client_id not in self.completed_clients
        
        if has_maximizer_data and has_end_message and not_processed:
            logging.info(f"action: ready_to_join | client_id:{client_id} | has_data:{has_maximizer_data} | has_end:{has_end_message}")
        
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
    
class MenuItemsJoiner(Joiner):
    
    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.item_id] = row.name
    
    def join_result(self, row: TableProcessRow, client_id):
        result = {
            "item_id": row.item_id,
            "item_name": self.joiner_data[client_id].get(row.item_id, "UNKNOWN"),
            "quantity": row.quantity,
            "subtotal": row.subtotal,
            "month_year": row.month_year_created_at  # Corregir nombre del campo
        }
        return result
    
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
        
        if sellings_results:
            sellings_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_1)
            sellings_chunk = ResultChunk(sellings_header, sellings_results)
            self.data_sender.send(sellings_chunk.serialize())

        if profit_results:
            profit_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_2)  # Corregir TableType
            profit_chunk = ResultChunk(profit_header, profit_results)
            self.data_sender.send(profit_chunk.serialize())
        
        logging.info(f"action: sent_result_message | type:{self.joiner_type}")
    
class StoresTpvJoiner(Joiner):
    
    def define_receivers(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_stores")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.store_id] = row.store_name
    
    def join_result(self, row: TableProcessRow, client_id):
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
            
class StoresTop3Joiner(Joiner):
    
    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_top3")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_stores")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")

    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.store_id] = row.store_name
    
    def join_result(self, row: TableProcessRow, client_id):
        # REPENSAR TIPO DE DATO PARA CANTIDAD DE COMPRAS -> AGGREGATOR
        result = {
            "store_id": row.store_id,
            "store_name": self.joiner_data[client_id].get(row.store_id, "UNKNOWN"),
            "user_id": row.user_id,
            "purchases_quantity": row.quantity,
        }
        return result
    
    def publish_results(self, client_id):
        # PENSAR NUEVO TIPO DE DATA PARA PROCESS TABLE
        joiner_results = self.joiner_results.get(client_id, [])
        query4_results = []
        for row in joiner_results:
            query4_result = Query4ResultRow(store_id=row["store_id"], store_name=row["store_name"], user_id=row["user_id"], purchases_quantity=row["purchases_quantity"])
            query4_results.append(query4_result)
        
        if query4_results:
            query4_header = ResultChunkHeader(client_id, ResultTableType.QUERY_4)
            query4_chunk = ResultChunk(query4_header, query4_results)
            self.data_sender.send(query4_chunk.serialize())
            logging.info(f"action: sent_result_message | type:{self.joiner_type}")
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")

class UsersJoiner(Joiner):
    
    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_users")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        
    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.user_id] = row.birthdate
    
    def join_result(self, row: TableProcessRow, client_id):
        result = {
            "store_id": row.store_id,
            "store_name": row.store_name,
            "user_id": row.user_id,
            "birthdate": self.joiner_data[client_id].get(row.user_id, "UNKNOWN"),
            "purchases_quatity": row.quantity,
        }
        return result

    def publish_results(self, client_id):
        joiner_results = self.joiner_results.get(client_id, [])
        query4_partial_results = []
        for row in joiner_results:
            store_id = row["store_id"]
            store_name = row["store_name"]
            user_id = row["user_id"]
            birthdate = row["birthdate"]
            purchase_quantity = row["purchases_quatity"]
            query4_result = Query4ResultRow(store_id, store_name, user_id, birthdate, purchase_quantity)
            query4_partial_results.append(query4_result)
        
        if query4_partial_results:
            query4_header = ResultChunkHeader(client_id, ResultTableType.QUERY_4)
            query4_chunk = ResultChunk(query4_header, query4_partial_results)
            self.data_sender.send(query4_chunk.serialize())
            logging.info(f"action: sent_result_message | type:{self.joiner_type}")
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")