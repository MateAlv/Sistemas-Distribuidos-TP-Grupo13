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
        self.lock = threading.Lock()
        self.ready_to_join = False # Variable compartida para indicar que se recibio el END de join data

        self.define_queues()
        
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
        self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
        self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
    
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
                chunk = ProcessBatchReader.from_bytes(data)
                logging.info(f"action: receive_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                                 
                with self.lock:
                    self.save_data(chunk)
                    # wait for END message of Join Table
                    if self.ready_to_join:
                        # Aplica el join
                        self.apply()
                        # Publica los resultados al to_merge_data
                        self.publish_results()
                    else:
                        logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
 
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
            "month_year": row.month_year
        }
        return result
    
    def publish_results(self, client_id):
        sellings_results = []
        profit_results = []
        joiner_results = self.joiner_results.get(client_id, [])
        for row in joiner_results:
            # INCLUIR CLIENT_ID EN LOS RESULTADOS
            row["client_id"] = client_id
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
        
        sellings_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_1)
        sellings_chunk = ResultChunk(sellings_header, sellings_results)
        self.data_sender.send(sellings_chunk.serialize())

        profit_header = ResultChunkHeader(client_id, TableType.QUERY_2_2)
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
        #query4_results = []
        for row in joiner_results:
            #query4_result = Query4ResultRow(store_id=row["store_id"], store_name=row["store_name"], tpv=row["tpv"], year_half=row["year_half"])
            #query4_results.append(query4_result)
        
        if query4_partial_results:
            #query4_header = ResultChunkHeader(client_id, ResultTableType.QUERY_4)
            #query4_chunk = ResultChunk(query4_header, query4_results)
            #self.data_sender.send(query4_chunk.serialize())
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