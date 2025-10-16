from utils.file_utils.process_table import TransactionItemsProcessRow, PurchasesPerUserStoreRow
import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.file_utils.end_messages import MessageEnd
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from collections import defaultdict, deque
from typing import Optional
import datetime
import heapq

TIMEOUT = 3

class Maximizer:
    def __init__(self, max_type: str, max_range: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)

        self.__running = True

        self.maximizer_type = max_type
        self.maximizer_range = max_range
        self.clients_end_processed = set()
        
        if self.maximizer_type == "MAX":
            self.sellings_max = defaultdict(dict)   # client_id -> {(item_id, month): quantity}
            self.profit_max = defaultdict(dict)     # client_id -> {(item_id, month): subtotal}

            if self.is_absolute_max():
                # Máximo absoluto - recibe de maximizers parciales
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_PRODUCTS", [""], exchange_type="fanout")
                self.middleware_exchange_receiver = None 
                self.expected_partial_maximizers = 3 
                self.partial_ranges_seen = defaultdict(set)  # client_id -> {range_id}
                self.partial_end_counts = defaultdict(int)   # client_id -> cantidad de END recibidos
            else:
                # Maximizers parciales - envían al absolute max
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                self.middleware_exchange_sender = None  # No envía END directamente al joiner
                
                if self.maximizer_range == "1":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
                elif self.maximizer_range == "4":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
                elif self.maximizer_range == "7":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
                else:
                    raise ValueError(f"Rango de maximizer inválido: {self.maximizer_range}")
                self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "end_exchange_aggregator_PRODUCTS", [""], exchange_type="fanout")
                    
        elif self.maximizer_type == "TOP3":
            # Para TOP3 clientes por store (Query 4)
            self.top3_by_store = defaultdict(lambda: defaultdict(list))  # client_id -> store_id -> heap[(count, user_id)]
            
            if self.is_absolute_top3():
                # TOP3 absoluto - recibe de TOP3 parciales
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_purchases_joiner")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_top3_absolute")
                self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_TOP3", [""], exchange_type="fanout")
                self.middleware_exchange_receiver = None  # No necesita escuchar END de nadie
                self.expected_partial_top3 = 3  # Sabemos que son 3: rango 1, 4, 7
                self.partial_top3_finished = defaultdict(int)  # client_id -> cantidad de END recibidos
            else:
                # TOP3 parciales - envían al absoluto
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_top3_absolute")
                self.middleware_exchange_sender = None
                self.middleware_exchange_receiver = None
                
                if self.maximizer_range == "1":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_top_1_3")
                elif self.maximizer_range == "4":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_top_4_6")
                elif self.maximizer_range == "7":
                    self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_top_7_10")
                else:
                    raise ValueError(f"Rango de maximizer TOP3 inválido: {self.maximizer_range}")
        else:
            raise ValueError(f"Tipo de maximizer inválido: {self.maximizer_type}")

    def is_absolute_max(self):
        return self.maximizer_range == "0"
    
    def is_absolute_top3(self):
        return self.maximizer_range == "0"
    
    def delete_client_data(self, client_id: int):
        """Elimina la información almacenada de un cliente"""
        if self.maximizer_type == "MAX":
            if client_id in self.sellings_max:
                del self.sellings_max[client_id]
            if client_id in self.profit_max:
                del self.profit_max[client_id]
            if self.is_absolute_max():
                self.partial_ranges_seen.pop(client_id, None)
                self.partial_end_counts.pop(client_id, None)
        elif self.maximizer_type == "TOP3":
            self.top3_by_store.pop(client_id, None)
            if self.is_absolute_top3():
                self.partial_top3_finished.pop(client_id, None)
        
        logging.info(f"action: delete_client_data | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
    
    def process_client_end(self, client_id: int, table_type: TableType):
        """Procesa el final de un cliente y envía resultados"""
        if client_id in self.clients_end_processed:
            logging.debug(f"action: end_already_processed | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
            return

        if not self._is_valid_table_type(table_type, for_end=True):
            expected_table = self._expected_table_type()
            logging.debug(f"action: ignoring_end_wrong_table | expected:{expected_table} | received:{table_type} | client_id:{client_id}")
            return

        self.clients_end_processed.add(client_id)
        logging.info(f"action: end_received_processing_final | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")

        if self.maximizer_type == "MAX":
            if self.is_absolute_max():
                self.publish_absolute_max_results(client_id)
                self._send_end_message(client_id, TableType.TRANSACTION_ITEMS, "joiner")
            else:
                self.publish_partial_max_results(client_id)
                self._send_end_message(client_id, TableType.TRANSACTION_ITEMS, "absolute")
                logging.info(f"action: partial_maximizer_finished | range:{self.maximizer_range} | client_id:{client_id}")
        elif self.maximizer_type == "TOP3":
            if self.is_absolute_top3():
                self.publish_absolute_top3_results(client_id)
                self._send_end_message(client_id, TableType.PURCHASES_PER_USER_STORE, "joiner")
            else:
                self.publish_partial_top3_results(client_id)
                self._send_end_message(client_id, TableType.PURCHASES_PER_USER_STORE, "absolute")
                logging.info(f"action: partial_top3_finished | range:{self.maximizer_range} | client_id:{client_id}")

        logging.info(f"action: maximizer_finished | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
        self.delete_client_data(client_id)
    
    def run(self):
        logging.info(f"Maximizer iniciado. Tipo: {self.maximizer_type}, Rango: {self.maximizer_range}, Receiver: {getattr(self.data_receiver, 'queue_name', 'unknown')}")
        
        messages = deque()
        
        def callback(msg):
            messages.append(msg)
            logging.info(f"action: data_received | type:{self.maximizer_type} | range:{self.maximizer_range} | size:{len(msg)}")
            
        def stop():
            try:
                self.data_receiver.stop_consuming()
            except Exception as e:
                logging.debug(f"action: stop_consuming_warning | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")

        while self.__running:
            try:
                if hasattr(self.data_receiver, "connection"):
                    self.data_receiver.connection.call_later(TIMEOUT, stop)
                self.data_receiver.start_consuming(callback)
            except Exception as e:
                logging.error(f"action: error_during_consumption | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")

            while messages:
                data = messages.popleft()
                try:
                    if data.startswith(b"END;"):
                        self._handle_end_message(data)
                    else:
                        self._handle_data_chunk(data)
                except Exception as e:
                    logging.error(f"action: error_processing_message | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")

    def _handle_end_message(self, raw_message: bytes):
        try:
            end_message = MessageEnd.decode(raw_message)
        except Exception as e:
            logging.error(f"action: error_decoding_end_message | error:{e}")
            return

        client_id = end_message.client_id()
        table_type = end_message.table_type()
        expected_table = self._expected_table_type()

        if not self._is_valid_table_type(table_type, for_end=True):
            expected_table = self._expected_table_type()
            logging.debug(f"action: ignoring_end_wrong_table | expected:{expected_table} | received:{table_type} | client_id:{client_id}")
            return

        if self.maximizer_type == "MAX" and self.is_absolute_max():
            if client_id in self.clients_end_processed:
                logging.debug(f"action: ignoring_end_already_processed | client_id:{client_id}")
                return

            self.partial_end_counts[client_id] += 1
            current = self.partial_end_counts[client_id]
            logging.info(f"action: end_received_from_partial | type:{self.maximizer_type} | client_id:{client_id} | received:{current}/{self.expected_partial_maximizers}")

            if current >= self.expected_partial_maximizers:
                logging.info(f"action: absolute_max_all_partials_finished | client_id:{client_id}")
                self.process_client_end(client_id, table_type)
            else:
                logging.info(f"action: waiting_for_more_partials | client_id:{client_id} | received:{current} | expected:{self.expected_partial_maximizers}")
            return

        if self.maximizer_type == "TOP3" and self.is_absolute_top3():
            if client_id in self.clients_end_processed:
                logging.debug(f"action: ignoring_end_already_processed | client_id:{client_id}")
                return

            self.partial_top3_finished[client_id] += 1
            current = self.partial_top3_finished[client_id]
            logging.info(f"action: end_received_from_partial | type:{self.maximizer_type} | client_id:{client_id} | received:{current}/{self.expected_partial_top3}")

            if current >= self.expected_partial_top3:
                logging.info(f"action: absolute_top3_all_partials_finished | client_id:{client_id}")
                self.process_client_end(client_id, table_type)
            else:
                logging.info(f"action: waiting_for_more_partials | client_id:{client_id} | received:{current} | expected:{self.expected_partial_top3}")
            return

        self.process_client_end(client_id, table_type)

    def _handle_data_chunk(self, data: bytes):
        chunk = ProcessBatchReader.from_bytes(data)
        client_id = chunk.client_id()
        table_type = chunk.table_type()
        expected_table = self._expected_table_type()

        if not self._is_valid_table_type(table_type, for_end=False):
            logging.debug(f"action: ignoring_chunk_wrong_table | expected:{expected_table} | received:{table_type} | client_id:{client_id}")
            return

        # Si llega un nuevo chunk para un cliente ya procesado, reabrir su ciclo
        if self.maximizer_type == "MAX":
            if client_id in self.clients_end_processed and client_id not in self.sellings_max:
                self.clients_end_processed.discard(client_id)
        elif self.maximizer_type == "TOP3":
            if client_id in self.clients_end_processed and client_id not in self.top3_by_store:
                self.clients_end_processed.discard(client_id)

        if client_id in self.clients_end_processed:
            logging.debug(f"action: ignoring_data_after_end_processed | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
            return

        logging.info(f"action: maximize | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id} | file_type:{table_type} | rows_in:{len(chunk.rows)}")
        self.apply(client_id, chunk)

        if self.maximizer_type == "MAX" and self.is_absolute_max():
            ranges_seen = self.partial_ranges_seen[client_id]
            for row in chunk.rows:
                range_id = self._extract_range_from_row(row)
                if range_id:
                    ranges_seen.add(range_id)

            logging.info(f"action: absolute_max_tracking | client_id:{client_id} | partial_maximizers_seen:{len(ranges_seen)}/{self.expected_partial_maximizers} | received_ranges:{sorted(ranges_seen)}")

            if len(ranges_seen) >= self.expected_partial_maximizers:
                logging.info(f"action: absolute_max_ready | client_id:{client_id} | all_partial_data_received")
                self.process_client_end(client_id, table_type)

    def _send_end_message(self, client_id: int, table_type: TableType, target: str):
        try:
            logging.info(f"action: sending_end_message_to_{target} | client_id:{client_id}")
            end_msg = MessageEnd(client_id, table_type, 1)
            self.data_sender.send(end_msg.encode())
            logging.info(f"action: sent_end_message_to_{target} | client_id:{client_id} | format:END;{client_id};{table_type.value};1")
        except Exception as e:
            logging.error(f"action: error_sending_end_to_{target} | client_id:{client_id} | error:{e}")

    def _expected_table_type(self) -> TableType:
        if self.maximizer_type == "MAX":
            return TableType.TRANSACTION_ITEMS
        if self.maximizer_type == "TOP3":
            return TableType.PURCHASES_PER_USER_STORE
        raise ValueError(f"Tipo de maximizer inválido: {self.maximizer_type}")

    def _is_valid_table_type(self, table_type: TableType, *, for_end: bool) -> bool:
        expected = self._expected_table_type()
        if table_type == expected:
            return True

        # Los maximizers TOP3 parciales reciben END con TableType.TRANSACTIONS desde el aggregator
        if self.maximizer_type == "TOP3" and not self.is_absolute_top3():
            if for_end and table_type == TableType.TRANSACTIONS:
                return True

        return False

    def _extract_range_from_row(self, row) -> Optional[str]:
        if hasattr(row, "transaction_id") and row.transaction_id:
            if "range_1" in row.transaction_id:
                return "1"
            if "range_4" in row.transaction_id:
                return "4"
            if "range_7" in row.transaction_id:
                return "7"
        return None
    
    def shutdown(self, signum=None, frame=None):
        logging.info(f"SIGTERM recibido: cerrando maximizer {self.maximizer_type} (rango {self.maximizer_range})")

        # Detener consumos de las colas
        try:
            self.data_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_sender.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_exchange_sender.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_exchange_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Cerrar conexiones
        try:
            self.data_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.data_sender.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_exchange_sender.close()
        except (OSError, RuntimeError, AttributeError):
            pass
        try:
            self.middleware_exchange_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Detener el loop principal
        self.__running = False

        # Liberar estructuras
        for attr in [
            "sellings_max",
            "profit_max",
            "partial_ranges_seen",
            "partial_end_counts",
            "top3_by_store",
            "partial_top3_finished",
            "clients_end_processed"
        ]:
            try:
                obj = getattr(self, attr, None)
                if isinstance(obj, (dict, list, set)) and hasattr(obj, "clear"):
                    obj.clear()
            except (OSError, RuntimeError, AttributeError):
                pass

        logging.info(f"Maximizer {self.maximizer_type} rango {self.maximizer_range} apagado correctamente.")

    def update_max(self, client_id: int, rows: list[TableProcessRow]) -> bool:
        """
        Actualiza los máximos relativos o absolutos.
        """
        updated_count = 0
        client_sellings = self.sellings_max[client_id]
        client_profit = self.profit_max[client_id]

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
                    if key not in client_sellings or row.quantity > client_sellings[key]:
                        old_value = client_sellings.get(key, 0)
                        client_sellings[key] = row.quantity
                        updated_count += 1
                        logging.debug(f"action: update_selling_max | type:{self.maximizer_type} | range:{self.maximizer_range} | item_id:{row.item_id} | month:{month_year.month}/{month_year.year} | old:{old_value} | new:{row.quantity}")
                
                # Actualizar máximo de ganancias si existe subtotal
                if hasattr(row, 'subtotal') and row.subtotal is not None:
                    if key not in client_profit or row.subtotal > client_profit[key]:
                        old_value = client_profit.get(key, 0.0)
                        client_profit[key] = row.subtotal
                        updated_count += 1
                        logging.debug(f"action: update_profit_max | type:{self.maximizer_type} | range:{self.maximizer_range} | item_id:{row.item_id} | month:{month_year.month}/{month_year.year} | old:{old_value} | new:{row.subtotal}")
        
        if updated_count > 0:
            logging.info(f"action: maximizer_update | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id} | updates:{updated_count} | total_selling_keys:{len(client_sellings)} | total_profit_keys:{len(client_profit)}")
        
        return updated_count > 0

    def update_top3(self, client_id: int, rows: list[TableProcessRow]):
        """
        Actualiza los top 3 clientes por store basado en el número de compras.
        """
        client_top3 = self.top3_by_store[client_id]

        for row in rows:
            if isinstance(row, PurchasesPerUserStoreRow):
                store_id = int(row.store_id)
                user_id = int(row.user_id)
                purchase_count = int(row.purchases_made)
                
                # Mantener solo los top 3 usando un heap
                if len(client_top3[store_id]) < 3:
                    heapq.heappush(client_top3[store_id], (purchase_count, user_id))
                else:
                    # Si el nuevo count es mayor que el mínimo en el heap
                    if purchase_count > client_top3[store_id][0][0]:
                        heapq.heapreplace(client_top3[store_id], (purchase_count, user_id))
                        logging.debug(f"action: update_top3 | store_id:{store_id} | user_id:{user_id} | count:{purchase_count}")

    def apply(self, client_id: int, chunk) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        if self.maximizer_type == "MAX":
            self.update_max(client_id, chunk.rows)
            client_sellings = self.sellings_max[client_id]
            client_profit = self.profit_max[client_id]

            if self.is_absolute_max():
                # Formatear los diccionarios de máximos para logging legible
                selling_summary = {f"item_{item_id}_({month.month}/{month.year})": qty for (item_id, month), qty in client_sellings.items()}
                profit_summary = {f"item_{item_id}_({month.month}/{month.year})": profit for (item_id, month), profit in client_profit.items()}
                logging.info(f"action: maximizer_result | type:{self.maximizer_type} | client_id:{client_id} | file_type:{chunk.table_type()} | sellings_max:{selling_summary} | profit_max:{profit_summary}")
            else:
                # Formatear los diccionarios de máximos para logging legible
                selling_summary = {f"item_{item_id}_({month.month}/{month.year})": qty for (item_id, month), qty in client_sellings.items()}
                profit_summary = {f"item_{item_id}_({month.month}/{month.year})": profit for (item_id, month), profit in client_profit.items()}
                logging.info(f"action: maximizer_partial_result | type:{self.maximizer_type} | client_id:{client_id} | file_type:{chunk.table_type()} | sellings_max_partial:{selling_summary} | profit_max_partial:{profit_summary}")
            return True
        elif self.maximizer_type == "TOP3":
            if self.is_absolute_top3():
                client_top3 = self.top3_by_store[client_id]
                logging.debug(f"action: processing_absolute_top3_chunk | client_id:{client_id} | rows_count:{len(chunk.rows)} | stores_before:{list(client_top3.keys())}")
                
                for row in chunk.rows:
                    if isinstance(row, PurchasesPerUserStoreRow):
                        store_id = int(row.store_id)
                        user_id = int(row.user_id)
                        purchase_count = int(row.purchases_made)
                        
                        logging.debug(f"action: processing_absolute_row | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | purchase_count:{purchase_count} | heap_size_before:{len(client_top3[store_id])}")
                        
                        # Acumular directamente en top3_by_store
                        if len(client_top3[store_id]) < 3:
                            heapq.heappush(client_top3[store_id], (purchase_count, user_id))
                            logging.debug(f"action: pushed_to_heap | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | heap_size_after:{len(client_top3[store_id])}")
                        else:
                            # Si el nuevo count es mayor que el mínimo en el heap
                            if purchase_count > client_top3[store_id][0][0]:
                                old_min = client_top3[store_id][0]
                                heapq.heapreplace(client_top3[store_id], (purchase_count, user_id))
                                logging.debug(f"action: replaced_in_heap | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | old_min:{old_min}")
                            else:
                                logging.debug(f"action: skipped_lower_count | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | count:{purchase_count} | min_heap:{client_top3[store_id][0][0]}")
                        
                        logging.debug(f"action: absolute_top3_accumulate | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | count:{purchase_count}")
                
                logging.info(f"action: absolute_top3_result | type:{self.maximizer_type} | client_id:{client_id} | file_type:{chunk.table_type()} | stores_processed:{len(client_top3)} | stores_after:{list(client_top3.keys())}")
                
                # Log de estado completo
                for store_id, heap in client_top3.items():
                    logging.debug(f"action: store_heap_state | client_id:{client_id} | store_id:{store_id} | heap:{list(heap)}")
            else:
                # Los maximizers parciales usan la lógica normal
                self.update_top3(client_id, chunk.rows)
                logging.info(f"action: top3_result | type:{self.maximizer_type} | client_id:{client_id} | file_type:{chunk.table_type()} | stores_processed:{len(self.top3_by_store[client_id])}")
            return True
        else:
            logging.error(f"Maximizador desconocido: {self.maximizer_type}")
            return False
    
    def publish_partial_max_results(self, client_id: int):
        """
        Los maximizers parciales envían sus máximos locales al absolute max.
        """
        accumulated_results = []
        client_sellings = self.sellings_max.get(client_id, {})
        client_profit = self.profit_max.get(client_id, {})
        
        # Enviar máximos de ventas
        for (item_id, month_year), quantity in client_sellings.items():
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
        for (item_id, month_year), profit in client_profit.items():
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
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            
            chunk_data = chunk.serialize()
            self.data_sender.send(chunk_data)
            
            logging.info(f"action: publish_partial_max_results | range:{self.maximizer_range} | client_id:{client_id} | rows_sent:{len(accumulated_results)} | selling_entries:{len(client_sellings)} | profit_entries:{len(client_profit)} | bytes_sent:{len(chunk_data)} | queue:{self.data_sender.queue_name}")
        else:
            logging.warning(f"action: no_partial_results_to_send | range:{self.maximizer_range} | client_id:{client_id}")
        

    def publish_partial_top3_results(self, client_id: int):
        """
        Publica los resultados parciales de TOP3 al TOP3 absoluto después del END message.
        """
        accumulated_results = []
        marker_date = datetime.date(2024, 1, 1)  # Fecha marca
        client_top3 = self.top3_by_store.get(client_id, {})
        
        for store_id, top3_heap in client_top3.items():
            # Convertir heap a lista ordenada (mayor a menor)
            top3_list = sorted(top3_heap, key=lambda x: x[0], reverse=True)
            
            for rank, (purchase_count, user_id) in enumerate(top3_list, 1):
                # Usar PurchasesPerUserStoreRow para enviar los datos al TOP3 absoluto
                row = PurchasesPerUserStoreRow(
                    store_id=store_id,
                    store_name="",  # Placeholder - lo llena el joiner
                    user_id=user_id,
                    user_birthdate=marker_date,  # Placeholder - lo llena el joiner
                    purchases_made=purchase_count,  # Cantidad real de compras, no el rank
                )
                accumulated_results.append(row)
                
                logging.debug(f"action: partial_top3_client | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | rank:{rank} | purchases:{purchase_count}")
        
        if accumulated_results:
            from utils.file_utils.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            # NO cerrar la conexión aquí - se cerrará después de enviar el END message
            
            logging.info(f"action: publish_partial_top3_results | result: success | client_id:{client_id} | stores:{len(client_top3)} | total_clients:{len(accumulated_results)}")
        else:
            logging.warning(f"action: no_partial_top3_results_to_send | client_id:{client_id}")

    def publish_absolute_top3_results(self, client_id: int):
        """
        Publica los resultados finales del TOP3 absoluto.
        Los maximizers parciales ya enviaron su TOP3 calculado, solo necesitamos reenviarlos.
        """
        client_top3 = self.top3_by_store.get(client_id, {})
        logging.info(f"action: forwarding_top3_results | client_id:{client_id} | stores_processed:{len(client_top3)} | stores_list:{list(client_top3.keys())}")
        
        # Log detailed state before processing
        for store_id, heap in client_top3.items():
            logging.debug(f"action: store_heap_before_forward | client_id:{client_id} | store_id:{store_id} | heap_content:{list(heap)} | heap_size:{len(heap)}")
        
        accumulated_results = []
        marker_date = datetime.date(2024, 1, 1)
        
        # Los datos ya vienen procesados de los maximizers parciales
        # Solo necesitamos reenviarlos al joiner
        for store_id, top3_heap in client_top3.items():
            logging.debug(f"action: processing_store_for_forward | client_id:{client_id} | store_id:{store_id} | heap_size:{len(top3_heap)}")
            
            # Los datos ya están como TOP3 por store
            for purchase_count, user_id in top3_heap:
                row = PurchasesPerUserStoreRow(
                    store_id=store_id,
                    store_name="",  # Placeholder - lo llena el joiner
                    user_id=user_id,
                    user_birthdate=marker_date,  # Placeholder - lo llena el joiner
                    purchases_made=purchase_count,
                )
                accumulated_results.append(row)
                logging.debug(f"action: forward_top3_result | client_id:{client_id} | store_id:{store_id} | user_id:{user_id} | purchases:{purchase_count}")
        
        if accumulated_results:
            from utils.file_utils.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            
            logging.info(f"action: publish_absolute_top3_results | result: success | client_id:{client_id} | stores_processed:{len(client_top3)} | total_results_forwarded:{len(accumulated_results)}")
        else:
            logging.warning(f"action: no_absolute_top3_results | client_id:{client_id}")

    def publish_absolute_max_results(self, client_id: int):
        """
        Publica los resultados de máximos absolutos por mes.
        Solo los máximos de cada mes para Q2.
        """
        client_sellings = self.sellings_max.get(client_id, {})
        client_profit = self.profit_max.get(client_id, {})
        logging.info(f"action: calculating_absolute_max | client_id:{client_id} | selling_entries:{len(client_sellings)} | profit_entries:{len(client_profit)}")
        
        if not client_sellings and not client_profit:
            logging.warning(f"action: no_data_for_absolute_max | client_id:{client_id}")
            return
            
        accumulated_results = []
        
        # Para cada mes, encontrar el producto con más ventas y más ganancias
        monthly_max_selling = defaultdict(lambda: (0, 0))  # (item_id, max_quantity)
        monthly_max_profit = defaultdict(lambda: (0, 0.0))  # (item_id, max_profit)
        
        # Encontrar máximos por mes para ventas
        for (item_id, month_year), quantity in client_sellings.items():
            month_key = (month_year.year, month_year.month)
            if quantity > monthly_max_selling[month_key][1]:
                old_item, old_qty = monthly_max_selling[month_key]
                monthly_max_selling[month_key] = (item_id, quantity)
                logging.debug(f"action: new_monthly_selling_max | month:{month_year.month}/{month_year.year} | old_item:{old_item} | old_qty:{old_qty} | new_item:{item_id} | new_qty:{quantity}")
        
        # Encontrar máximos por mes para ganancias
        for (item_id, month_year), profit in client_profit.items():
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
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            
            logging.info(f"action: publish_absolute_max_results | result: success | client_id:{client_id} | months_selling:{len(monthly_max_selling)} | months_profit:{len(monthly_max_profit)} | total_rows:{len(accumulated_results)}")
        else:
            logging.warning(f"action: no_absolute_max_results | client_id:{client_id}")
