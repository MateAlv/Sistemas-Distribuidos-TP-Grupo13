from utils.file_utils.table_type import TableType
from functools import partial
from logging import log
from utils.file_utils.process_table import TransactionItemsProcessRow
import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from collections import defaultdict
import datetime
import threading

TIMEOUT = 3

class Joiner:
    def __init__(self, join_type: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)

        self.joiner_type = join_type
        self.data = {}
        self.joiner_data = {}
        self.joiner_results = []
        self.lock = threading.Lock()
        self.ready_to_join = False # Variable compartida para indicar que se recibio el END de join data
        self.received_end_message = False
        
        if self.joiner_type == "ITEMS":
            self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
            self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
            self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
            
        self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
        self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")

    def is_absolute_max(self):
        return self.maximizer_range == "0"
    
    def handle_join_data(self):
        
        results = []
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()

        while True:
            # Max receive initialization
            self.data_receiver.connection.call_later(TIMEOUT, stop_max)
            self.data_receiver.start_consuming(callback_max)

            for data in results:
                chunk = ProcessBatchReader.from_bytes(data)
                logging.info(f"action: receive_join_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                self.save_data(chunk.rows, self.joiner_data)
                
                # if recibo el END:
                with self.lock:
                    self.ready_to_join = True
                    # Modifica self.data -> Compartido con handle_data_thread
                    self.apply()
                    logging.info(f"action: ready_to_join | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}") 

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
                # Data es END message
                # if es END:
                    # self.received_end_message = True
                #else:
                chunk = ProcessBatchReader.from_bytes(data)
                logging.info(f"action: receive_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                                        
                with self.lock:
                    self.save_data(chunk)
                    # wait for END message of Join Table
                    if self.ready_to_join:
                        logging.info(f"action: joining_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
                        self.apply()
                        if self.received_end_message:
                            self.publish_results()
                            logging.info(f"action: sent_end_message | type:{self.joiner_type}")
                    else:
                        logging.debug(f"action: waiting_join_data | type:{self.joiner_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
 
                results.remove(data)
    
    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        # Iniciar hilos para manejar data y join_data
        data_handler_thread = threading.Thread(target=self.handle_data, name="DataHandler")
        join_data_handler_thread = threading.Thread(target=self.handle_join_data, name="JoinDataHandler")
        data_handler_thread.start() 
        join_data_handler_thread.start()
        
        
    def save_data(self, chunk) -> bool:
        """
        Guarda los datos en la lista correspondiente.
        """
        # Se guarda en memoria
        if self.joiner_type == "ITEMS":
            if chunk.table_type() == TableType.TRANSACTION_ITEMS:
                logging.debug(f"action: save_data | type:{self.joiner_type} | rows:{len(chunk.rows)}")
                if chunk.client_id() not in self.data:
                    self.data[chunk.client_id()] = {"header": chunk.header, "rows": []}
                self.data[chunk.client_id()]["rows"].extend(chunk.rows)
            else:
                logging.warning(f"error: save_data | type:{self.joiner_type} | unexpected_table_type:{chunk.table_type()}")
        return True
    
    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos de join en la lista correspondiente.
        """
        # Se guarda en memoria
        if self.joiner_type == "ITEMS":
            for row in rows:
                if isinstance(row, MenuItemsProcessRow):
                    logging.debug(f"action: save_join_data | type:{self.joiner_type} | item_id:{row.item_id} | quantity:{row.quantity} | subtotal:{row.subtotal} | created_at:{row.created_at}")
                    self.joiner_data[chunk.client_id()]["header"] = chunk.header
                    if chunk.client_id() not in self.joiner_data:
                        self.joiner_data[chunk.client_id()] = {"header": chunk.header, "rows": {}}
                    self.joiner_data[chunk.client_id()]["rows"][row.item_id] = row.name
                else:
                    logging.warning(f"error: save_join_data | type:{self.joiner_type} | unexpected_row_type:{type(row)}")
            
        return True

    def apply(self) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        if self.joiner_type == "ITEMS":
            # Hacer el join entre self.data y self.joiner_data
            for row in self.data:
                self.joiner_results.append({
                    "item_id": row.item_id,
                    "item_name": self.joiner_data.get(row.item_id, "UNKNOWN"),
                    "quantity": row.quantity,
                    "subtotal": row.subtotal,
                    "month_year": row.month_year
                })
            logging.info(f"action: joiner_result | type:{self.joiner_type} | results_out: Data: {self.data} - Joiner_data: {self.joiner_data}")
            return True
    
    def publish_results(self):
        if self.joiner_type == "ITEMS":
            sellings_results = []
            profit_results = []
            for result in self.joiner_results:
                if result["quantity"] is not None:
                    max_selling = {
                        "item_name": result["name"],
                        "quantity": result["quantity"],
                        "month_year": result["month_year"]
                    }
                    sellings_results.append(max_selling)
                if result["subtotal"] is not None:
                    max_profit = {
                        "item_name": result["name"],
                        "subtotal": result["subtotal"],
                        "month_year": result["month_year"]
                    }
                    profit_results.append(max_profit)
            
            # sellings_header = ResultChunkHeader(chunk.header, TableType.MAX_SELLINGS)
            # profit_header = ResultChunkHeader(chunk.header, TableType.MAX_PROFITS)
            # sellings_chunk = ResultChunk(sellings_header, sellings_results)
            # profit_chunk = ResultChunk(profit_header, profit_results)
            # self.data_sender.send(sellings_chunk.serialize())
            # self.data_sender.send(profit_chunk.serialize())