from functools import partial
from logging import log
from utils.file_utils.process_table import TransactionItemsProcessRow, TransactionsProcessRow
import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from collections import defaultdict
import datetime
import heapq

TIMEOUT = 3

class Maximizer:
    def __init__(self, max_type: str, max_range: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.maximizer_type = max_type
        self.maximizer_range = max_range
        self.client_id = None  # Track client ID for publishing results
        
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
                    
        elif self.maximizer_type == "TOP3":
            # Para TOP3 clientes por store (Query 4)
            self.top3_by_store = defaultdict(list)  # store_id -> [(count, user_id), ...]
            self.data_receiver = MessageMiddlewareQueue("rabbitmq", "transactions_sum_by_client")
            self.data_sender = MessageMiddlewareQueue("rabbitmq", "top3_clients_by_store")
        else:
            raise ValueError(f"Tipo de maximizer inválido: {self.maximizer_type}")

        self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
        self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")

    def is_absolute_max(self):
        return self.maximizer_range == "0"
    
    def run(self):
        logging.info(f"Maximizer iniciado. Tipo: {self.maximizer_type}")
        
        results = []
        end_received = False
        
        def callback(msg): results.append(msg)
        def stop():
            self.data_receiver.stop_consuming()
        def end_callback(msg): 
            nonlocal end_received
            end_received = True
            self.data_receiver.stop_consuming()

        while not end_received:
            # Max receive initialization
            self.data_receiver.connection.call_later(TIMEOUT, stop)
            self.data_receiver.start_consuming(callback)
            
            # Escuchar end message en paralelo solo para absolute max
            if self.maximizer_type == "MAX" and self.is_absolute_max():
                try:
                    self.middleware_exchange_receiver.start_consuming(end_callback, timeout=0.1)
                except:
                    pass  # Timeout es normal

            for data in results:
                chunk = ProcessBatchReader.from_bytes(data)
                logging.info(f"action: maximize | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                # Track client_id for publishing results
                if self.client_id is None:
                    self.client_id = chunk.client_id()
                
                self.apply(chunk)
                
                if self.maximizer_type == "MAX" and self.is_absolute_max():
                    # Solo acumular, no enviar hasta el END
                    logging.debug(f"action: accumulating_for_absolute_max | type:{self.maximizer_type}")
                elif self.maximizer_type == "TOP3":
                    # Para TOP3, procesar chunk y enviar resultado incremental
                    top3_chunk = self.apply_top3_chunk(chunk)
                    if top3_chunk:
                        self.publish_top3_chunk(top3_chunk)
                else:
                    self.publish_results(chunk)
                
                results.remove(data)
                
        # Si recibimos END message y somos absolute max, enviar resultados finales
        if self.maximizer_type == "MAX" and self.is_absolute_max():
            self.publish_absolute_max_results()
            # Enviar end message con client_id
            try:
                end_msg = f"client_id:{self.client_id or 1}"
                self.middleware_exchange_sender.send(end_msg)
                self.middleware_exchange_sender.close()
                logging.info(f"action: sent_end_message | client_id:{self.client_id or 1}")
            except Exception as e:
                logging.error(f"action: error_sending_end_message | error:{e}")
                pass

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

    def update_top3(self, rows: list[TableProcessRow]):
        """
        Actualiza los top 3 clientes por store basado en el número de compras.
        """
        for row in rows:
            if hasattr(row, 'store_id') and hasattr(row, 'user_id') and hasattr(row, 'final_amount'):
                store_id = int(row.store_id)
                user_id = int(row.user_id)
                purchase_count = int(row.final_amount)  # El count está en final_amount
                
                # Mantener solo los top 3 usando un heap
                if len(self.top3_by_store[store_id]) < 3:
                    heapq.heappush(self.top3_by_store[store_id], (purchase_count, user_id))
                else:
                    # Si el nuevo count es mayor que el mínimo en el heap
                    if purchase_count > self.top3_by_store[store_id][0][0]:
                        heapq.heapreplace(self.top3_by_store[store_id], (purchase_count, user_id))
                
                logging.debug(f"action: update_top3 | store_id:{store_id} | user_id:{user_id} | count:{purchase_count}")

    def apply_top3_chunk(self, chunk):
        """
        Procesa un chunk para encontrar candidatos a top 3 por store.
        En lugar de mantener estado global, retorna el top 3 del chunk actual.
        """
        chunk_top3_by_store = defaultdict(list)
        
        for row in chunk.rows:
            if hasattr(row, 'store_id') and hasattr(row, 'user_id') and hasattr(row, 'final_amount'):
                store_id = int(row.store_id)
                user_id = int(row.user_id)
                purchase_count = int(row.final_amount)
                
                # Mantener solo los top 3 del chunk usando un heap
                if len(chunk_top3_by_store[store_id]) < 3:
                    heapq.heappush(chunk_top3_by_store[store_id], (purchase_count, user_id))
                else:
                    # Si el nuevo count es mayor que el mínimo en el heap
                    if purchase_count > chunk_top3_by_store[store_id][0][0]:
                        heapq.heapreplace(chunk_top3_by_store[store_id], (purchase_count, user_id))
                
                logging.debug(f"action: chunk_top3 | store_id:{store_id} | user_id:{user_id} | count:{purchase_count}")

        # Crear chunk de salida con los candidatos top 3 de este chunk
        if not chunk_top3_by_store:
            return None
            
        rows = []
        marker_date = datetime.date(2024, 1, 1)
        
        for store_id, top3_heap in chunk_top3_by_store.items():
            # Convertir heap a lista ordenada (mayor a menor)
            top3_list = sorted(top3_heap, key=lambda x: x[0], reverse=True)
            
            for rank, (purchase_count, user_id) in enumerate(top3_list, 1):
                row = TransactionsProcessRow(
                    transaction_id=f"candidate_rank_{rank}",
                    store_id=store_id,
                    user_id=user_id,
                    final_amount=float(purchase_count),  # Mantener el count original, no el rank
                    created_at=marker_date,
                )
                rows.append(row)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TRANSACTIONS)
        return ProcessChunk(header, rows)
    
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
        elif self.maximizer_type == "TOP3":
            self.update_top3(chunk.rows)
            logging.info(f"action: top3_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | stores_processed:{len(self.top3_by_store)}")
            return True
        else:
            logging.error(f"Maximizador desconocido: {self.maximizer_type}")
            return False
    
    def publish_results(self, chunk):
        
        accumulated_results = []
        
        if self.maximizer_type == "MAX":
            for key, value in self.sellings_max.items():
                item_id, month_year = key
                max_quantity = value
                new_row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=max_quantity,
                    subtotal=None,
                    created_at=DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0)) if month_year is not None else None
                )
                accumulated_results.append(new_row)
            for key, value in self.profit_max.items():
                item_id, month_year = key
                max_profit = value
                new_row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=None,
                    subtotal=max_profit,
                    created_at=DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0)) if month_year is not None else None
                )
                accumulated_results.append(new_row)

        self.data_sender.send(ProcessChunk(chunk.header, accumulated_results).serialize())

    def publish_absolute_max_results(self):
        """
        Publica los resultados de máximos absolutos por mes.
        Solo los máximos de cada mes para Q2.
        """
        accumulated_results = []
        
        # Para cada mes, encontrar el producto con más ventas y más ganancias
        monthly_max_selling = defaultdict(lambda: (0, 0))  # (item_id, max_quantity)
        monthly_max_profit = defaultdict(lambda: (0, 0.0))  # (item_id, max_profit)
        
        # Encontrar máximos por mes para ventas
        for (item_id, month_year), quantity in self.sellings_max.items():
            month_key = (month_year.year, month_year.month)
            if quantity > monthly_max_selling[month_key][1]:
                monthly_max_selling[month_key] = (item_id, quantity)
        
        # Encontrar máximos por mes para ganancias
        for (item_id, month_year), profit in self.profit_max.items():
            month_key = (month_year.year, month_year.month)
            if profit > monthly_max_profit[month_key][1]:
                monthly_max_profit[month_key] = (item_id, profit)
        
        # Crear filas de resultado para ventas
        for (year, month), (item_id, max_quantity) in monthly_max_selling.items():
            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id="max_selling",
                item_id=item_id,
                quantity=max_quantity,
                subtotal=None,  # Solo cantidad para ventas
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.info(f"action: max_selling_per_month | year:{year} | month:{month} | item_id:{item_id} | quantity:{max_quantity}")
        
        # Crear filas de resultado para ganancias  
        for (year, month), (item_id, max_profit) in monthly_max_profit.items():
            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id="max_profit",
                item_id=item_id,
                quantity=None,  # Solo ganancia para profits
                subtotal=max_profit,
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.info(f"action: max_profit_per_month | year:{year} | month:{month} | item_id:{item_id} | profit:{max_profit}")
        
        # Enviar resultados al joiner
        if accumulated_results:
            from utils.file_utils.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=self.client_id or 1, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            self.data_sender.close()
            
            logging.info(f"action: publish_absolute_max_results | result: success | months_selling:{len(monthly_max_selling)} | months_profit:{len(monthly_max_profit)}")

    def publish_top3_chunk(self, top3_chunk):
        """
        Publica un chunk con candidatos a top 3 clientes por store.
        """
        import base64
        
        payload_b64 = base64.b64encode(top3_chunk.serialize()).decode("utf-8")
        self.data_sender.send(payload_b64)
        self.data_sender.close()
        
        logging.info(f"action: publish_top3_chunk | result: success | candidates:{len(top3_chunk.rows)}")

    def publish_top3_results(self, chunk):
        """
        Publica los resultados de top 3 clientes por store.
        Envía el user_id de los top 3 clientes para que el joiner los use para obtener birthdates.
        """
        accumulated_results = []
        marker_date = datetime.date(2024, 1, 1)  # Fecha marca
        
        for store_id, top3_heap in self.top3_by_store.items():
            # Convertir heap a lista ordenada (mayor a menor)
            top3_list = sorted(top3_heap, key=lambda x: x[0], reverse=True)
            
            for rank, (purchase_count, user_id) in enumerate(top3_list, 1):
                # Usar TransactionsProcessRow para enviar los datos
                # store_id = store, user_id = cliente, final_amount = rank (1, 2, 3)
                row = TransactionsProcessRow(
                    transaction_id=f"top3_rank_{rank}",
                    store_id=store_id,
                    user_id=user_id,
                    final_amount=float(rank),  # Ranking (1, 2, 3)
                    created_at=marker_date,
                )
                accumulated_results.append(row)
                
                logging.debug(f"action: top3_client | store_id:{store_id} | user_id:{user_id} | rank:{rank} | purchases:{purchase_count}")
        
        self.data_sender.send(ProcessChunk(chunk.header, accumulated_results).serialize())
        self.data_sender.close()
        
        logging.info(f"action: publish_top3_results | result: success | stores:{len(self.top3_by_store)} | total_clients:{len(accumulated_results)}")