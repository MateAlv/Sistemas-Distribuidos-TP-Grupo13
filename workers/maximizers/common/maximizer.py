from functools import partial
from logging import log
from utils.file_utils.process_table import TransactionItemsProcessRow, TransactionsProcessRow
import logging
import threading
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.file_utils.end_messages import MessageEnd
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from collections import defaultdict
import datetime
import heapq
import base64

TIMEOUT = 3

class Maximizer:
    def __init__(self, max_type: str, max_range: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.maximizer_type = max_type
        self.maximizer_range = max_range
        self.client_id = None  # Track client ID for publishing results
        self.end_received = False  # Track si se recibió END message
        self.received_ranges = set()  # Para el absolute max: trackear qué rangos han enviado datos
        
        if self.maximizer_type == "MAX":
            self.sellings_max = dict()  # Almacena los máximos actuales
            self.profit_max = dict()    # Almacena los máximos actuales
            self.partial_maximizers_finished = 0  # Para el absolute max: contar cuántos parciales terminaron

            if self.is_absolute_max():
                # Maximo absoluto - recibe de maximizers parciales
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                # Envía END message al joiner cuando termine
                self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_PRODUCTS", [""], exchange_type="fanout")
                self.expected_partial_maximizers = 3  # Sabemos que son 3: rango 1, 4, 7
            else:
                # Maximizers parciales - envían al absolute max
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                if self.maximizer_range == "1":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
                elif self.maximizer_range == "4":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
                elif self.maximizer_range == "7":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
                else:
                    raise ValueError(f"Rango de maximizer inválido: {self.maximizer_range}")
                # Escucha END de los aggregators - usando convención consistente
                self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "end_exchange_aggregator_PRODUCTS", [""], exchange_type="fanout")
                    
        elif self.maximizer_type == "TOP3":
            # Para TOP3 clientes por store (Query 4)
            self.top3_by_store = defaultdict(list)  # store_id -> [(count, user_id), ...]
            self.data_receiver = MessageMiddlewareQueue("rabbitmq", "transactions_sum_by_client")
            self.data_sender = MessageMiddlewareQueue("rabbitmq", "top3_clients_by_store")
            # Escucha END del aggregator de PURCHASES - usando convención consistente
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "end_exchange_aggregator_PURCHASES", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_TOP3", [""], exchange_type="fanout")
        else:
            raise ValueError(f"Tipo de maximizer inválido: {self.maximizer_type}")

    def is_absolute_max(self):
        return self.maximizer_range == "0"
    
    def run(self):
        logging.info(f"Maximizer iniciado. Tipo: {self.maximizer_type}, Rango: {self.maximizer_range}, Reciever: {self.data_receiver.queue_name}")
        
        results = []
        end_messages = []
        
        def callback(msg): 
            results.append(msg)
            logging.info(f"action: data_received | type:{self.maximizer_type} | range:{self.maximizer_range} | size:{len(msg)}")
            
        def stop():
            self.data_receiver.stop_consuming()
            
        def end_callback(msg): 
            end_messages.append(msg)
            logging.info(f"action: end_message_received | type:{self.maximizer_type} | range:{self.maximizer_range} | msg_size:{len(msg)} | msg_preview:{msg[:100] if len(msg) > 100 else msg}")
            
        def end_stop():
            self.middleware_exchange_receiver.stop_consuming()

        # Iniciar el consumer de END messages en background
        import threading
        def check_end_messages():
            if not self.is_absolute_max():  # Solo los maximizers parciales escuchan END_MESSAGE
                logging.info(f"action: starting_end_message_listener | type:{self.maximizer_type} | range:{self.maximizer_range} | exchange:{self.middleware_exchange_receiver.exchange_name}")
                while not self.end_received:
                    try:
                        self.middleware_exchange_receiver.connection.call_later(TIMEOUT, end_stop)
                        self.middleware_exchange_receiver.start_consuming(end_callback)
                    except Exception as e:
                        logging.debug(f"action: end_message_timeout | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")
                    
                    # Procesar END messages recibidos
                    for end_msg in end_messages:
                        logging.info(f"action: processing_end_message | type:{self.maximizer_type} | range:{self.maximizer_range} | setting_end_received_true")
                        self.end_received = True
                        end_messages.remove(end_msg)
                        break
        
        if not self.is_absolute_max():
            end_thread = threading.Thread(target=check_end_messages)
            end_thread.daemon = True
            end_thread.start()
            logging.info(f"action: end_message_listener_started | type:{self.maximizer_type} | range:{self.maximizer_range}")

        # Loop principal para procesar datos
        while not self.end_received:
            # Escuchar datos con timeout
            try:
                self.data_receiver.connection.call_later(TIMEOUT, stop)
                self.data_receiver.start_consuming(callback)
            except:
                pass  # Timeout es normal

            # Procesar datos recibidos
            for data in results:
                try:
                    chunk = ProcessBatchReader.from_bytes(data)
                    logging.info(f"action: maximize | type:{self.maximizer_type} | range:{self.maximizer_range} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                    
                    # Track client_id for publishing results
                    if self.client_id is None:
                        self.client_id = chunk.client_id()
                    
                    self.apply(chunk)
                    
                    # Para absolute max: detectar cuándo termina cada maximizer parcial
                    if self.is_absolute_max():
                        # Contar maximizers parciales únicos que han enviado datos
                        for row in chunk.rows:
                            if hasattr(row, 'transaction_id') and row.transaction_id:
                                if 'range_1' in row.transaction_id:
                                    self.received_ranges.add('1')
                                elif 'range_4' in row.transaction_id:
                                    self.received_ranges.add('4')
                                elif 'range_7' in row.transaction_id:
                                    self.received_ranges.add('7')
                        
                        self.partial_maximizers_finished = len(self.received_ranges)
                        logging.info(f"action: absolute_max_tracking | partial_maximizers_seen:{self.partial_maximizers_finished}/{self.expected_partial_maximizers} | received_ranges:{sorted(self.received_ranges)}")
                        
                        # Si recibimos datos de todos los maximizers parciales, podemos terminar
                        if self.partial_maximizers_finished >= self.expected_partial_maximizers:
                            self.end_received = True
                            logging.info(f"action: absolute_max_ready | all_partial_maximizers_received:{self.partial_maximizers_finished}")
                    
                    # Los maximizers parciales NO envían resultados hasta recibir END
                    # Solo el absolute max envía resultados finales
                    if self.maximizer_type == "TOP3":
                        # Para TOP3, procesar chunk y enviar resultado incremental
                        top3_chunk = self.apply_top3_chunk(chunk)
                        if top3_chunk:
                            self.publish_top3_chunk(top3_chunk)
                            
                except Exception as e:
                    logging.error(f"action: error_processing_data | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")
                
                results.remove(data)
                
        # Procesar resultados finales después de END message
        logging.info(f"action: end_received_processing_final | type:{self.maximizer_type} | range:{self.maximizer_range}")
        
        if self.maximizer_type == "MAX":
            if self.is_absolute_max():
                # Absolute max: enviar resultados finales al joiner
                self.publish_absolute_max_results()
                # Enviar END message al joiner
                try:
                    logging.info(f"action: sending_end_message_to_joiner | client_id:{self.client_id or 1} | exchange:{self.middleware_exchange_sender.exchange_name}")
                    end_msg = MessageEnd(self.client_id or 1, TableType.TRANSACTION_ITEMS, 1)
                    self.middleware_exchange_sender.send(end_msg.encode())
                    logging.info(f"action: sent_end_message_to_joiner | client_id:{self.client_id or 1} | format:END;{self.client_id or 1};{TableType.TRANSACTION_ITEMS.value};1")
                except Exception as e:
                    logging.error(f"action: error_sending_end_to_joiner | error:{e}")
            else:
                # Maximizers parciales: enviar máximos al absolute max
                self.publish_partial_max_results()
                # NO necesitan enviar END message ya que el absolute max detecta automáticamente
                logging.info(f"action: partial_maximizer_finished | range:{self.maximizer_range} | client_id:{self.client_id or 1}")
        elif self.maximizer_type == "TOP3":
            # Enviar resultados finales de TOP3
            self.publish_top3_final_results()
            
        logging.info(f"action: maximizer_finished | type:{self.maximizer_type} | range:{self.maximizer_range}")

    def update_max(self, rows: list[TableProcessRow]) -> bool:
        """
        Actualiza los máximos relativos o absolutos.
        """
        updated_count = 0
        for row in rows:
            if hasattr(row, 'item_id') and hasattr(row, 'created_at'):
                # Extraer año/mes de created_at
                if hasattr(row.created_at, 'date'):
                    date_obj = row.created_at.date
                    month_year = type('MonthYear', (), {'year': date_obj.year, 'month': date_obj.month})()
                else:
                    logging.warning(f"action: invalid_date | item_id:{row.item_id} | created_at:{row.created_at}")
                    continue
                
                key = (row.item_id, month_year)
                
                # Actualizar máximo de ventas si existe quantity
                if hasattr(row, 'quantity') and row.quantity is not None:
                    if key not in self.sellings_max or row.quantity > self.sellings_max[key]:
                        old_value = self.sellings_max.get(key, 0)
                        self.sellings_max[key] = row.quantity
                        updated_count += 1
                        logging.debug(f"action: update_selling_max | type:{self.maximizer_type} | range:{self.maximizer_range} | item_id:{row.item_id} | month:{month_year.month}/{month_year.year} | old:{old_value} | new:{row.quantity}")
                
                # Actualizar máximo de ganancias si existe subtotal
                if hasattr(row, 'subtotal') and row.subtotal is not None:
                    if key not in self.profit_max or row.subtotal > self.profit_max[key]:
                        old_value = self.profit_max.get(key, 0.0)
                        self.profit_max[key] = row.subtotal
                        updated_count += 1
                        logging.debug(f"action: update_profit_max | type:{self.maximizer_type} | range:{self.maximizer_range} | item_id:{row.item_id} | month:{month_year.month}/{month_year.year} | old:{old_value} | new:{row.subtotal}")
        
        if updated_count > 0:
            logging.info(f"action: maximizer_update | type:{self.maximizer_type} | range:{self.maximizer_range} | updates:{updated_count} | total_selling_keys:{len(self.sellings_max)} | total_profit_keys:{len(self.profit_max)}")
        
        return updated_count > 0

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
                # Formatear los diccionarios de máximos para logging legible
                selling_summary = {f"item_{item_id}_({month.month}/{month.year})": qty for (item_id, month), qty in self.sellings_max.items()}
                profit_summary = {f"item_{item_id}_({month.month}/{month.year})": profit for (item_id, month), profit in self.profit_max.items()}
                logging.info(f"action: maximizer_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | sellings_max: {selling_summary} | profit_max: {profit_summary}")
            else:
                # Formatear los diccionarios de máximos para logging legible
                selling_summary = {f"item_{item_id}_({month.month}/{month.year})": qty for (item_id, month), qty in self.sellings_max.items()}
                profit_summary = {f"item_{item_id}_({month.month}/{month.year})": profit for (item_id, month), profit in self.profit_max.items()}
                logging.info(f"action: maximizer_partial_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | sellings_max_partial: {selling_summary} | profit_max_partial: {profit_summary}")
            return True
        elif self.maximizer_type == "TOP3":
            self.update_top3(chunk.rows)
            logging.info(f"action: top3_result | type:{self.maximizer_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | stores_processed:{len(self.top3_by_store)}")
            return True
        else:
            logging.error(f"Maximizador desconocido: {self.maximizer_type}")
            return False
    
    def publish_partial_max_results(self):
        """
        Los maximizers parciales envían sus máximos locales al absolute max.
        """
        accumulated_results = []
        
        # Enviar máximos de ventas
        for (item_id, month_year), quantity in self.sellings_max.items():
            created_at = DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id=f"partial_max_selling_range_{self.maximizer_range}",
                item_id=item_id,
                quantity=quantity,
                subtotal=None,  # Solo cantidad para ventas
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.debug(f"action: send_partial_selling_max | range:{self.maximizer_range} | item_id:{item_id} | month:{month_year.month}/{month_year.year} | quantity:{quantity}")
        
        # Enviar máximos de ganancias  
        for (item_id, month_year), profit in self.profit_max.items():
            created_at = DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id=f"partial_max_profit_range_{self.maximizer_range}",
                item_id=item_id,
                quantity=None,  # Solo ganancia para profits
                subtotal=profit,
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.debug(f"action: send_partial_profit_max | range:{self.maximizer_range} | item_id:{item_id} | month:{month_year.month}/{month_year.year} | profit:{profit}")
        
        # Enviar resultados al absolute max
        if accumulated_results:
            from utils.file_utils.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=self.client_id or 1, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            
            chunk_data = chunk.serialize()
            self.data_sender.send(chunk_data)
            
            logging.info(f"action: publish_partial_max_results | range:{self.maximizer_range} | client_id:{self.client_id or 1} | rows_sent:{len(accumulated_results)} | selling_entries:{len(self.sellings_max)} | profit_entries:{len(self.profit_max)} | bytes_sent:{len(chunk_data)} | queue:{self.data_sender.queue_name}")
        else:
            logging.warning(f"action: no_partial_results_to_send | range:{self.maximizer_range} | client_id:{self.client_id or 1}")
        
        # NO cerrar la conexión aquí para permitir que otros maximizers usen la misma queue

    def publish_top3_final_results(self):
        """
        Publica los resultados finales de TOP3 después del END message.
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
                    transaction_id=f"top3_final_rank_{rank}",
                    store_id=store_id,
                    user_id=user_id,
                    final_amount=float(rank),  # Ranking (1, 2, 3)
                    created_at=marker_date,
                )
                accumulated_results.append(row)
                
                logging.debug(f"action: top3_final_client | store_id:{store_id} | user_id:{user_id} | rank:{rank} | purchases:{purchase_count}")
        
        if accumulated_results:
            from utils.file_utils.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=self.client_id or 1, table_type=TableType.TRANSACTIONS)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            self.data_sender.close()
            
            logging.info(f"action: publish_top3_final_results | result: success | stores:{len(self.top3_by_store)} | total_clients:{len(accumulated_results)}")
            
            # Enviar END message
            try:
                end_msg = MessageEnd(self.client_id or 1, TableType.TRANSACTIONS, 1)
                self.middleware_exchange_sender.send(end_msg.encode())
                logging.info(f"action: sent_top3_end_message | client_id:{self.client_id or 1} | format:END;{self.client_id or 1};{TableType.TRANSACTIONS.value};1")
            except Exception as e:
                logging.error(f"action: error_sending_top3_end_message | error:{e}")
        else:
            logging.warning(f"action: no_top3_results_to_send | client_id:{self.client_id or 1}")

    def publish_results(self, chunk):
        """
        DEPRECATED - Solo para compatibilidad. Los maximizers parciales NO deben usar esto.
        """
        logging.warning(f"action: deprecated_publish_results_called | type:{self.maximizer_type} | range:{self.maximizer_range}")
        return

    def publish_absolute_max_results(self):
        """
        Publica los resultados de máximos absolutos por mes.
        Solo los máximos de cada mes para Q2.
        """
        logging.info(f"action: calculating_absolute_max | selling_entries:{len(self.sellings_max)} | profit_entries:{len(self.profit_max)}")
        
        if not self.sellings_max and not self.profit_max:
            logging.warning(f"action: no_data_for_absolute_max | client_id:{self.client_id or 1}")
            return
            
        accumulated_results = []
        
        # Para cada mes, encontrar el producto con más ventas y más ganancias
        monthly_max_selling = defaultdict(lambda: (0, 0))  # (item_id, max_quantity)
        monthly_max_profit = defaultdict(lambda: (0, 0.0))  # (item_id, max_profit)
        
        # Encontrar máximos por mes para ventas
        for (item_id, month_year), quantity in self.sellings_max.items():
            month_key = (month_year.year, month_year.month)
            if quantity > monthly_max_selling[month_key][1]:
                old_item, old_qty = monthly_max_selling[month_key]
                monthly_max_selling[month_key] = (item_id, quantity)
                logging.debug(f"action: new_monthly_selling_max | month:{month_year.month}/{month_year.year} | old_item:{old_item} | old_qty:{old_qty} | new_item:{item_id} | new_qty:{quantity}")
        
        # Encontrar máximos por mes para ganancias
        for (item_id, month_year), profit in self.profit_max.items():
            month_key = (month_year.year, month_year.month)
            if profit > monthly_max_profit[month_key][1]:
                old_item, old_profit = monthly_max_profit[month_key]
                monthly_max_profit[month_key] = (item_id, profit)
                logging.debug(f"action: new_monthly_profit_max | month:{month_year.month}/{month_year.year} | old_item:{old_item} | old_profit:{old_profit} | new_item:{item_id} | new_profit:{profit}")
        
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
            
            logging.info(f"action: publish_absolute_max_results | result: success | client_id:{self.client_id or 1} | months_selling:{len(monthly_max_selling)} | months_profit:{len(monthly_max_profit)} | total_rows:{len(accumulated_results)}")
        else:
            logging.warning(f"action: no_absolute_max_results | client_id:{self.client_id or 1}")

    def publish_top3_chunk(self, top3_chunk):
        """
        Publica un chunk con candidatos a top 3 clientes por store.
        """
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