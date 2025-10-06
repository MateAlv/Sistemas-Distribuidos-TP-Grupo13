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

TIMEOUT = 3

class Maximizer:
    def __init__(self, max_type: str, max_range: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.maximizer_type = max_type
        self.maximizer_range = max_range
        
        if self.maximizer_type == "MAX":
            self.sellings_max = dict()  # Almacena los máximos actuales
            self.profit_max = dict()    # Almacena los máximos actuales

            if self.is_absolute_max():
                # Maximo absoluto
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
            else:
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                if self.maximizer_range == "1":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
                elif self.maximizer_range == "2":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
                elif self.maximizer_range == "3":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
                else:
                    raise ValueError(f"Rango de maximizer inválido: {self.maximizer_range}")
            

        self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
        self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")

    def is_absolute_max(self):
        return self.maximizer_range == "0"
    
    def run(self):
        logging.info(f"Maximizer iniciado. Tipo: {self.maximizer_type}")
        
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
                logging.info(f"action: maximize | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                self.apply(chunk)
                
                if self.is_absolute_max():
                    # wait for END message
                    logging.info(f"action: waiting_end_message | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
                else:
                    self.publish_results(chunk)
                
                results.remove(data)

    def update_max(self, rows: list[TableProcessRow]) -> bool:
        """
        Actualiza los máximos relativos o absolutos.
        """
        for row in rows:
            key = (row.item_id, row.month_year_created_at)  # month_year es un objeto MonthYear
            if key not in self.sellings_max or row.quantity > self.sellings_max[key][0]:
                self.sellings_max[key] = row.quantity
            if key not in self.profit_max or row.subtotal > self.profit_max[key][0]:
                self.profit_max[key] = row.subtotal
    
    def apply(self, chunk) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        if self.maximizer_type == "MAX":
            self.update_max(chunk.rows)
            if self.is_absolute_max():
                logging.info(f"action: maximizer_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | results_out: Sellings_max: {self.sellings_max} - Profit_max: {self.profit_max}")
            else:
                logging.info(f"action: maximizer_partial_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | results_out: Sellings_max_partial: {self.sellings_max} - Profit_max_partial: {self.profit_max}")
            return True
        else:
            logging.error(f"Maximizador desconocido: {self.maximizer_type}")
            return False
    
    def publish_results(self, chunk):
        
        accumulated_results = []
        
        if self.maximizer_type == "MAX":
            for result in self.sellings_max.items():
                (item_id, month_year), max_quantity = result
                new_row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=max_quantity,
                    subtotal=None,
                    created_at=DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0)) if month_year is not None else None
                )
                accummulated_results.append(new_row)
            for result in self.profit_max.items():
                item_id, (max_profit, month_year) = result
                new_row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=None,
                    subtotal=max_profit,
                    created_at=DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0)) if month_year is not None else None
                )
                accummulated_results.append(new_row)
        
        self.data_sender.send(ProcessChunk(chunk.header, accummulated_results).serialize())