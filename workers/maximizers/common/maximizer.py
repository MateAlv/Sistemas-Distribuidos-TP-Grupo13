from utils.processing.process_table import TransactionItemsProcessRow, PurchasesPerUserStoreRow
import logging
from utils.processing.process_table import TableProcessRow
from utils.processing.process_chunk import ProcessChunk
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.eof_protocol.end_messages import MessageEnd, MessageForceEnd
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from collections import defaultdict, deque
from typing import Optional
import datetime
import heapq
import re
import sys
import os
import uuid
import pickle
from utils.tolerance.persistence_service import PersistenceService
from .maximizer_working_state import MaximizerWorkingState
from utils.common.processing_types import MonthYear

from workers.common.sharding import queue_name_for, slugify_shard_id

TIMEOUT = 3


def default_top3_value():
    return defaultdict(list)

class Maximizer:
    def __init__(self, max_type: str, role: str, shard_id: Optional[str], partial_shards: list[str], monitor=None):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        self.monitor = monitor

        self.__running = True

        self.maximizer_type = max_type
        if role not in {"partial", "absolute"}:
            raise ValueError(f"Rol de maximizer inválido: {role}")
        self.role = role
        self.shard_id = shard_id
        self.maximizer_range = shard_id if shard_id else "absolute"
        self.partial_shards = partial_shards or []
        self.shard_slug = slugify_shard_id(shard_id) if shard_id else None
        self.partial_shards_lookup = {slugify_shard_id(s): s for s in self.partial_shards}
        self.expected_shard_slugs = set(self.partial_shards_lookup.keys())
        self.expected_partial_maximizers = len(self.expected_shard_slugs) if role == "absolute" and max_type == "MAX" else 0
        self.expected_partial_top3 = len(self.expected_shard_slugs) if role == "absolute" and max_type == "TOP3" else 0

        if self.role == "partial" and not self.shard_id:
            raise ValueError("Los maximizers parciales requieren un identificador de shard.")
        if self.role == "absolute" and self.maximizer_type in {"MAX", "TOP3"} and not self.partial_shards:
            raise ValueError("Los maximizers absolutos requieren la lista de shards parciales configurados.")

        self.persistence = PersistenceService(f"/data/persistence/maximizer_{self.maximizer_type}_{self.maximizer_range}")
        self.working_state = MaximizerWorkingState()
        
        if self.maximizer_type == "MAX":
            if self.is_absolute_max():
                # Máximo absoluto - recibe de maximizers parciales
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_PRODUCTS", [""], exchange_type="fanout")
                self.middleware_exchange_receiver = None 
            else:
                # Maximizers parciales - envían al absolute max
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")
                self.middleware_exchange_sender = None  # No envía END directamente al joiner
                
                queue_name = queue_name_for("MAX", self.shard_id)
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", queue_name)
                self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "end_exchange_aggregator_PRODUCTS", [""], exchange_type="fanout")
                    
        elif self.maximizer_type == "TOP3":
            # Para TOP3 clientes por store (Query 4)
            
            if self.is_absolute_top3():
                # TOP3 absoluto - recibe de TOP3 parciales
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_purchases_joiner")
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_top3_absolute")
                self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "end_exchange_maximizer_TOP3", [""], exchange_type="fanout")
                self.middleware_exchange_receiver = None  # No necesita escuchar END de nadie
            else:
                # TOP3 parciales - envían al absoluto
                self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_top3_absolute")
                self.middleware_exchange_sender = None
                self.middleware_exchange_receiver = None
                
                queue_name = queue_name_for("TOP3", self.shard_id)
                self.data_receiver = MessageMiddlewareQueue("rabbitmq", queue_name)
        else:
            raise ValueError(f"Tipo de maximizer inválido: {self.maximizer_type}")

        self._recover_state()

    def is_absolute_max(self):
        return self.maximizer_type == "MAX" and self.role == "absolute"
    
    def is_absolute_top3(self):
        return self.maximizer_type == "TOP3" and self.role == "absolute"
    
    def delete_client_data(self, client_id: int):
        """Elimina la información almacenada de un cliente"""
        if client_id == 0:
            self.working_state.delete_all_data(self.maximizer_type, self.role == "absolute")
        else:
            self.working_state.delete_client_data(client_id, self.maximizer_type, self.role == "absolute")
        
        logging.info(f"action: delete_client_data | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
        self._save_state(uuid.uuid4())
    
    def delete_all_data(self):
        """Elimina toda la información almacenada"""
        self.working_state.delete_all_data(self.maximizer_type, self.role == "absolute")
        logging.info(f"action: delete_all_data | type:{self.maximizer_type} | range:{self.maximizer_range}")
    
    def process_client_end(self, client_id: int, table_type: TableType):
        """Procesa el final de un cliente y envía resultados"""
        if self.working_state.is_client_end_processed(client_id):
            logging.debug(f"action: end_already_processed | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
            return

        if not self._is_valid_table_type(table_type, for_end=True):
            expected_table = self._expected_table_type()
            logging.debug(f"action: ignoring_end_wrong_table | expected:{expected_table} | received:{table_type} | client_id:{client_id}")
            return

        self.working_state.mark_client_end_processed(client_id)
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

        # Recover last processing chunk if exists
        last_chunk = self.persistence.recover_last_processing_chunk()
        if last_chunk:
            logging.info("Recovering last processing chunk...")
            try:
                self._handle_data_chunk(last_chunk.serialize())
            except Exception as e:
                logging.error(f"Error recovering last chunk: {e}")

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
                    if data.startswith(b"FORCE_END;"):
                        self._handle_force_end_message(data)
                    if data.startswith(b"END;"):
                        self._handle_end_message(data)
                    else:
                        self._handle_data_chunk(data)
                except Exception as e:
                    logging.error(f"action: error_processing_message | type:{self.maximizer_type} | range:{self.maximizer_range} | error:{e}")

    def _handle_force_end_message(self, raw_message: bytes):
        try:
            force_end = MessageForceEnd.decode(raw_message)
        except Exception as e:
            logging.error(f"action: error_decoding_force_end_message | error:{e}")
            return

        client_id = force_end.client_id()
        logging.info(f"action: force_end_received | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
        self.data_sender.send(force_end.encode())
        self.delete_client_data(client_id)

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
            if self.working_state.is_client_end_processed(client_id):
                logging.debug(f"action: ignoring_end_already_processed | client_id:{client_id}")
                return

            current = self.working_state.increment_partial_end_count(client_id)
            logging.info(f"action: end_received_from_partial | type:{self.maximizer_type} | client_id:{client_id} | received:{current}/{self.expected_partial_maximizers}")

            if current >= self.expected_partial_maximizers:
                logging.info(f"action: absolute_max_all_partials_finished | client_id:{client_id}")
                self.process_client_end(client_id, table_type)
            else:
                logging.info(f"action: waiting_for_more_partials | client_id:{client_id} | received:{current} | expected:{self.expected_partial_maximizers}")
            return

        if self.maximizer_type == "TOP3" and self.is_absolute_top3():
            if self.working_state.is_client_end_processed(client_id):
                logging.debug(f"action: ignoring_end_already_processed | client_id:{client_id}")
                return

            current = self.working_state.increment_partial_top3_finished(client_id)
            logging.info(f"action: end_received_from_partial | type:{self.maximizer_type} | client_id:{client_id} | received:{current}/{self.expected_partial_top3}")

            if current >= self.expected_partial_top3:
                logging.info(f"action: absolute_top3_all_partials_finished | client_id:{client_id}")
                self.process_client_end(client_id, table_type)
            else:
                logging.info(f"action: waiting_for_more_partials | client_id:{client_id} | received:{current} | expected:{self.expected_partial_top3}")
            return

        self.process_client_end(client_id, table_type)

    def _handle_data_chunk(self, data: bytes):
        self._check_crash_point("CRASH_BEFORE_PROCESS")
        chunk = ProcessBatchReader.from_bytes(data)
        client_id = chunk.client_id()
        table_type = chunk.table_type()
        expected_table = self._expected_table_type()

        if self.working_state.is_processed(chunk.message_id()):
            logging.info(f"action: duplicate_chunk_ignored | message_id:{chunk.message_id()}")
            return

        self.persistence.commit_processing_chunk(chunk)

        if not self._is_valid_table_type(table_type, for_end=False):
            logging.debug(f"action: ignoring_chunk_wrong_table | expected:{expected_table} | received:{table_type} | client_id:{client_id}")
            return

        # Si llega un nuevo chunk para un cliente ya procesado, reabrir su ciclo
        if self.working_state.is_client_end_processed(client_id):
            # Check if we really need to reopen or if it's just late data
            # For now, following original logic but using working_state
            if self.maximizer_type == "MAX":
                if client_id not in self.working_state.sellings_max:
                    self.working_state.unmark_client_end_processed(client_id)
            elif self.maximizer_type == "TOP3":
                if client_id not in self.working_state.top3_by_store:
                    self.working_state.unmark_client_end_processed(client_id)

        if self.working_state.is_client_end_processed(client_id):
            logging.debug(f"action: ignoring_data_after_end_processed | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id}")
            return

        if self.maximizer_type == "TOP3" and not self.is_absolute_top3():
            logging.info(
                f"action: top3_chunk_processing_started | range:{self.maximizer_range} | client_id:{client_id} "
                f"| rows:{len(chunk.rows)}"
            )

        logging.info(f"action: maximize | type:{self.maximizer_type} | range:{self.maximizer_range} | client_id:{client_id} | file_type:{table_type} | rows_in:{len(chunk.rows)}")
        self.apply(client_id, chunk)

        self._check_crash_point("CRASH_AFTER_PROCESS_BEFORE_COMMIT")

        if self.maximizer_type == "MAX" and self.is_absolute_max():
            ranges_seen = self.working_state.get_partial_ranges_seen(client_id)
            for row in chunk.rows:
                shard_slug = self._extract_shard_from_row(row)
                if shard_slug:
                    self.working_state.add_partial_range_seen(client_id, shard_slug)

            received_labels = sorted(self.partial_shards_lookup.get(slug, slug) for slug in ranges_seen)
            logging.info(
                f"action: absolute_max_tracking | client_id:{client_id} | partial_maximizers_seen:{len(ranges_seen)}/{self.expected_partial_maximizers} | received_shards:{received_labels}"
            )

            ready = False
            if self.expected_shard_slugs:
                ready = self.expected_shard_slugs.issubset(ranges_seen)
            elif self.expected_partial_maximizers > 0:
                ready = len(ranges_seen) >= self.expected_partial_maximizers

            if ready:
                logging.info(f"action: absolute_max_ready | client_id:{client_id} | all_partial_data_received")
                self.process_client_end(client_id, table_type)

        self.working_state.mark_processed(chunk.message_id())
        self._save_state(chunk.message_id())

    def _send_end_message(self, client_id: int, table_type: TableType, target: str):
        try:
            logging.info(f"action: sending_end_message_to_{target} | client_id:{client_id}")
            end_msg = MessageEnd(client_id, table_type, 1)
            self.data_sender.send(end_msg.encode())
            # Commit send ack? No message_id for end message here, but we could commit state if needed.
            # But end message is usually triggered by processing, so state save after processing covers it.
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

    def _extract_shard_from_row(self, row) -> Optional[str]:
        transaction_id = getattr(row, "transaction_id", None)
        if not transaction_id:
            return None
        match = re.search(r"partial_max_(?:selling|profit)_shard_([a-z0-9_]+)", transaction_id)
        if match:
            return match.group(1)
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
        # Liberar estructuras
        try:
            self.working_state = MaximizerWorkingState()
        except (OSError, RuntimeError, AttributeError):
            pass

        logging.info(f"Maximizer {self.maximizer_type} rango {self.maximizer_range} apagado correctamente.")

    def update_max(self, client_id: int, rows: list[TableProcessRow]) -> bool:
        """
        Actualiza los máximos relativos o absolutos.
        """
        updated_count = 0
        client_sellings = self.working_state.get_sellings_max(client_id)
        client_profit = self.working_state.get_profit_max(client_id)

        for row in rows:
            if hasattr(row, 'item_id') and hasattr(row, 'created_at'):
                # Extraer año/mes de created_at
                if hasattr(row.created_at, 'date'):
                    date_obj = row.created_at.date
                    month_year = MonthYear(date_obj.month, date_obj.year)  # Note: (month, year) order
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
        client_top3 = self.working_state.get_top3_by_store(client_id)

        for row in rows:
            if isinstance(row, PurchasesPerUserStoreRow):
                store_id = int(row.store_id)
                user_id = int(row.user_id)
                purchase_count = int(row.purchases_made)

                if store_id not in client_top3:
                    client_top3[store_id] = []

                heap = client_top3[store_id]
                if len(heap) < 3:
                    heapq.heappush(heap, (purchase_count, user_id))
                    logging.debug(f"action: push | store:{store_id} | user:{user_id} | count:{purchase_count}")
                else:
                    root_count, root_user = heap[0]
                    # Compara ambos campos sobre el mínimo del heap
                    if (purchase_count, user_id) > (root_count, root_user):
                        heapq.heapreplace(heap, (purchase_count, user_id))
                        logging.debug(
                            f"action: replace | store:{store_id} | user:{user_id} "
                            f"| count:{purchase_count} | replaced:{(root_count, root_user)}"
                        )

        logging.info(f"action: top3_result | type:{self.maximizer_type} | client_id:{client_id}  | stores_processed:{len(client_top3)} | stores_after:{list(client_top3.keys())}")

    def apply(self, client_id: int, chunk) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        if self.maximizer_type == "MAX":
            self.update_max(client_id, chunk.rows)
            client_sellings = self.working_state.get_sellings_max(client_id)
            client_profit = self.working_state.get_profit_max(client_id)

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
            self.update_top3(client_id, chunk.rows)
            return True
        else:
            logging.error(f"Maximizador desconocido: {self.maximizer_type}")
            return False
    
    def publish_partial_max_results(self, client_id: int):
        """
        Los maximizers parciales envían sus máximos locales al absolute max.
        """
        accumulated_results = []
        client_sellings = self.working_state.get_sellings_max(client_id)
        client_profit = self.working_state.get_profit_max(client_id)
        
        # Enviar máximos de ventas
        for (item_id, month_year), quantity in client_sellings.items():
            created_at = DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id=f"partial_max_selling_shard_{self.shard_slug}",
                item_id=item_id,
                quantity=quantity,
                subtotal=None,  # Solo cantidad para ventas
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.debug(f"action: send_partial_selling_max | shard:{self.shard_id} | item_id:{item_id} | month:{month_year.month}/{month_year.year} | quantity:{quantity}")
        
        # Enviar máximos de ganancias  
        for (item_id, month_year), profit in client_profit.items():
            created_at = DateTime(datetime.date(month_year.year, month_year.month, 1), datetime.time(0, 0))
            new_row = TransactionItemsProcessRow(
                transaction_id=f"partial_max_profit_shard_{self.shard_slug}",
                item_id=item_id,
                quantity=None,  # Solo ganancia para profits
                subtotal=profit,
                created_at=created_at
            )
            accumulated_results.append(new_row)
            logging.debug(f"action: send_partial_profit_max | shard:{self.shard_id} | item_id:{item_id} | month:{month_year.month}/{month_year.year} | profit:{profit}")
        
        # Enviar resultados al absolute max
        if accumulated_results:
            from utils.processing.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            
            chunk_data = chunk.serialize()
            self.data_sender.send(chunk_data)
            # We don't have an incoming message ID to ack here easily unless we pass it down.
            # But publish_partial_max_results is called from process_client_end which is called from handle_end_message or handle_data_chunk.
            # If called from handle_data_chunk, we save state after return.
            
            logging.info(f"action: publish_partial_max_results | shard:{self.shard_id} | client_id:{client_id} | rows_sent:{len(accumulated_results)} | selling_entries:{len(client_sellings)} | profit_entries:{len(client_profit)} | bytes_sent:{len(chunk_data)} | queue:{self.data_sender.queue_name}")
        else:
            logging.warning(f"action: no_partial_results_to_send | shard:{self.shard_id} | client_id:{client_id}")
        

    def publish_partial_top3_results(self, client_id: int):
        """
        Publica los resultados parciales de TOP3 al TOP3 absoluto después del END message.
        """
        accumulated_results = []
        marker_date = datetime.date(2024, 1, 1)  # Fecha marca
        client_top3 = self.working_state.get_top3_by_store(client_id)
        
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
            from utils.processing.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            # NO cerrar la conexión aquí - se cerrará después de enviar el END message
            
            logging.info(
                f"action: publish_partial_top3_results | shard:{self.shard_id} | client_id:{client_id} | stores:{len(client_top3)} | total_clients:{len(accumulated_results)}"
            )
        else:
            logging.warning(f"action: no_partial_top3_results_to_send | shard:{self.shard_id} | client_id:{client_id}")

    def publish_absolute_top3_results(self, client_id: int):
        """
        Publica los resultados finales del TOP3 absoluto.
        Los maximizers parciales ya enviaron su TOP3 calculado, solo necesitamos reenviarlos.
        """
        client_top3 = self.working_state.get_top3_by_store(client_id)
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
            from utils.processing.process_chunk import ProcessChunkHeader
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
        client_sellings = self.working_state.get_sellings_max(client_id)
        client_profit = self.working_state.get_profit_max(client_id)
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
            from utils.processing.process_chunk import ProcessChunkHeader
            from utils.file_utils.table_type import TableType
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk = ProcessChunk(header, accumulated_results)
            self.data_sender.send(chunk.serialize())
            
            logging.info(f"action: publish_absolute_max_results | result: success | client_id:{client_id} | months_selling:{len(monthly_max_selling)} | months_profit:{len(monthly_max_profit)} | total_rows:{len(accumulated_results)}")
        else:
            logging.warning(f"action: no_absolute_max_results | client_id:{client_id}")

    def _check_crash_point(self, point_name):
        if os.environ.get("CRASH_POINT") == point_name:
            logging.critical(f"Simulating crash at {point_name}")
            sys.exit(1)

    def _recover_state(self):
        state_data = self.persistence.recover_working_state()
        if state_data:
            try:
                self.working_state = MaximizerWorkingState.from_bytes(state_data)
                logging.info(f"State recovered for maximizer {self.maximizer_type}_{self.maximizer_range}")
            except Exception as e:
                logging.error(f"Error recovering state: {e}")

    def _save_state(self, last_processed_id):
        self.persistence.commit_working_state(self.working_state.to_bytes(), last_processed_id)
