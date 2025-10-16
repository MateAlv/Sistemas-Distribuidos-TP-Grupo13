from logging import log
from utils.file_utils.process_table import TransactionItemsProcessRow, TransactionsProcessRow, PurchasesPerUserStoreRow, TPVProcessRow
import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.file_utils.end_messages import MessageEnd, MessageQueryEnd
from utils.file_utils.table_type import TableType, ResultTableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from .aggregator_stats_messages import AggregatorStatsMessage, AggregatorStatsEndMessage
from collections import defaultdict
import datetime
import sys

TIMEOUT = 3

class Aggregator:
    def __init__(self, agg_type: str, agg_id: int = 1):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.__running = True
        
        self.aggregator_type = agg_type
        self.aggregator_id = agg_id
        self.middleware_queue_sender = {}
        
        # Stats tracking - similar a Filter
        self.end_message_received = {}
        self.chunks_received_per_client = {}
        self.chunks_processed_per_client = {}
        self.chunks_to_receive = {}
        self.already_sent_stats = {}
        
        # Exchange para coordinación entre aggregators del mismo tipo
        self.middleware_stats_exchange = MessageMiddlewareExchange("rabbitmq", f"end_exchange_aggregator_{self.aggregator_type}", [""], "fanout")
        
        # Acumuladores globales para mantener estado hasta el final
        self.global_accumulator = {}  # Por client_id: datos acumulados
        
        if self.aggregator_type == "PRODUCTS":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_1+2")
            self.middleware_queue_sender["to_max_1_3"] = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
            self.middleware_queue_sender["to_max_4_6"] = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
            self.middleware_queue_sender["to_max_7_8"] = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
        elif self.aggregator_type == "PURCHASES":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
            self.middleware_queue_sender["to_top_1_3"] = MessageMiddlewareQueue("rabbitmq", "to_top_1_3")
            self.middleware_queue_sender["to_top_4_6"] = MessageMiddlewareQueue("rabbitmq", "to_top_4_6")
            self.middleware_queue_sender["to_top_7_10"] = MessageMiddlewareQueue("rabbitmq", "to_top_7_10")
        elif self.aggregator_type == "TPV":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
            self.middleware_queue_sender["to_join_with_stores_tvp"] = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")

    def shutdown(self, signum, frame):
        logging.info(f"action: shutdown_signal_received | type:{self.aggregator_type} | agg_id:{self.aggregator_id}")
        # Enviar stats finales a otros aggregators - Tolerancia a fallos

        self.__running = False  # Stop the main loop

        try:
            # Cerrar conexiones de middleware
            try:
                self.middleware_queue_receiver.close()
            except Exception as e:
                pass

            for sender in self.middleware_queue_sender.values():
                try:
                    sender.close()
                except Exception as e:
                    pass
            try:
                self.middleware_stats_exchange.close()
            except Exception as e:
                pass

            # Liberar estructuras en memoria
            if self.global_accumulator:
                self.global_accumulator.clear()
            if self.end_message_received:
                self.end_message_received.clear()
            if self.chunks_received_per_client:
                self.chunks_received_per_client.clear()
            if self.chunks_processed_per_client:
                self.chunks_processed_per_client.clear()
            if self.chunks_to_receive:
                self.chunks_to_receive.clear()
            if self.already_sent_stats:
                self.already_sent_stats.clear()

            logging.info(f"action: shutdown_completed | type:{self.aggregator_type} | agg_id:{self.aggregator_id}")
        except Exception as e:
            logging.error(f"action: shutdown_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}")
    
    def run(self):
        logging.info(f"Agregador iniciado. Tipo: {self.aggregator_type}, ID: {self.aggregator_id}")
        results = []
        stats_results = []
        
        def callback(msg): results.append(msg)
        def stats_callback(msg): stats_results.append(msg)
        def stop():
            self.middleware_queue_receiver.stop_consuming()
        def stats_stop():
            self.middleware_stats_exchange.stop_consuming()

        while self.__running:
            # Escuchar stats de otros aggregators
            self.middleware_stats_exchange.connection.call_later(TIMEOUT, stats_stop)
            self.middleware_stats_exchange.start_consuming(stats_callback)
            
            # Escuchar datos de filters
            self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
            self.middleware_queue_receiver.start_consuming(callback)

            # Procesar mensajes de stats de otros aggregators
            for stats_msg in stats_results:
                try:
                    if stats_msg.startswith(b"AGG_STATS_END"):
                        stats_end = AggregatorStatsEndMessage.decode(stats_msg)
                        if stats_end.aggregator_id == self.aggregator_id:
                            stats_results.remove(stats_msg)
                            continue
                        
                        logging.info(f"action: stats_end_received | type:{self.aggregator_type} | agg_id:{stats_end.aggregator_id} | cli_id:{stats_end.client_id} | table_type:{stats_end.table_type}")
                        self.delete_stats_client_data(stats_end)
                        self.delete_client_accumulator(stats_end)
                    else:
                        stats = AggregatorStatsMessage.decode(stats_msg)
                        if stats.aggregator_id == self.aggregator_id:
                            stats_results.remove(stats_msg)
                            continue
                        
                        logging.info(f"action: stats_received | type:{self.aggregator_type} | agg_id:{stats.aggregator_id} | cli_id:{stats.client_id} | file_type:{stats.table_type} | chunks_received:{stats.chunks_received} | chunks_processed:{stats.chunks_processed}")
                        
                        # Marcar que recibimos end message de este client/table
                        if stats.client_id not in self.end_message_received:
                            self.end_message_received[stats.client_id] = {}
                        self.end_message_received[stats.client_id][stats.table_type] = True

                        # Acumular stats de otros aggregators
                        self._ensure_dict_entry(self.chunks_received_per_client, stats.client_id, stats.table_type)
                        self._ensure_dict_entry(self.chunks_processed_per_client, stats.client_id, stats.table_type)
                        
                        self.chunks_received_per_client[stats.client_id][stats.table_type] += stats.chunks_received
                        self.chunks_processed_per_client[stats.client_id][stats.table_type] += stats.chunks_processed
                        
                        total_received = self.chunks_received_per_client[stats.client_id][stats.table_type]
                        total_processed = self.chunks_processed_per_client[stats.client_id][stats.table_type]
                        
                        # Si aún no envié mis stats, enviarlos ahora
                        if (stats.client_id, stats.table_type) not in self.already_sent_stats:
                            self.already_sent_stats[(stats.client_id, stats.table_type)] = True
                            my_stats_msg = AggregatorStatsMessage(self.aggregator_id, stats.client_id, stats.table_type, 
                                                                stats.total_expected, total_received, total_processed)
                            self.middleware_stats_exchange.send(my_stats_msg.encode())
                            
                        # Verificar si puedo enviar end message
                        if self._can_send_end_message(stats.total_expected, stats.client_id, stats.table_type):
                            self._send_end_message(stats.client_id, stats.table_type, stats.total_expected, total_processed)

                except Exception as e:
                    logging.error(f"action: error_decoding_stats_message | error:{e}")
                    
                stats_results.remove(stats_msg)

            # Procesar datos reales
            for msg in results:
                try:
                    # Diagnosticar el formato del mensaje
                    msg_type = type(msg)
                    msg_len = len(msg) if hasattr(msg, '__len__') else 'unknown'
                    msg_preview = str(msg)[:100] if len(str(msg)) > 100 else str(msg)
                    logging.info(f"action: received_message | type:{self.aggregator_type} | msg_type:{msg_type} | msg_len:{msg_len} | preview:{msg_preview}")
                    
                    # PRIMERO: Verificar si es un END message
                    if msg.startswith(b"END;"):
                        try:
                            end_message = MessageEnd.decode(msg)
                            client_id = end_message.client_id()
                            table_type = end_message.table_type()
                            
                            if client_id not in self.end_message_received:
                                self.end_message_received[client_id] = {}
                            self.end_message_received[client_id][table_type] = True
                            
                            total_expected = end_message.total_chunks()
                            self._ensure_dict_entry(self.chunks_received_per_client, client_id, table_type)
                            self._ensure_dict_entry(self.chunks_processed_per_client, client_id, table_type)
                            
                            if client_id not in self.chunks_to_receive:
                                self.chunks_to_receive[client_id] = {}
                            self.chunks_to_receive[client_id][table_type] = total_expected

                            logging.info(f"action: end_message_received | type:{self.aggregator_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks_expected:{total_expected}")
                            
                            # Enviar stats message
                            stats_msg = AggregatorStatsMessage(self.aggregator_id, client_id, table_type, total_expected,
                                                              self.chunks_received_per_client[client_id][table_type],
                                                              self.chunks_processed_per_client[client_id][table_type])
                            self.middleware_stats_exchange.send(stats_msg.encode())

                            # Verificar si puedo enviar end message
                            if self._can_send_end_message(total_expected, client_id, table_type):
                                self._send_end_message(client_id, table_type, total_expected, self.chunks_processed_per_client[client_id][table_type])
                                
                        except Exception as e:
                            logging.error(f"action: error_processing_end_message | type:{self.aggregator_type} | error:{e}")
                    else:
                        # SEGUNDO: Procesar como chunk de datos
                        chunk = ProcessBatchReader.from_bytes(msg)
                        client_id = chunk.client_id()
                        table_type = chunk.table_type()
                        
                        logging.info(f"action: aggregate | type:{self.aggregator_type} | cli_id:{client_id} | file_type:{table_type} | rows_in:{len(chunk.rows)}")
                        
                        # Contar chunks recibidos y procesados
                        self._ensure_dict_entry(self.chunks_received_per_client, client_id, table_type)
                        self._ensure_dict_entry(self.chunks_processed_per_client, client_id, table_type)
                        self.chunks_received_per_client[client_id][table_type] += 1
                        
                        logging.info(f"action: processing_start | type:{self.aggregator_type} | cli_id:{client_id}")
                        
                        # Procesar según tipo de aggregator - ACUMULAR no enviar
                        has_output = False
                        if self.aggregator_type == "PRODUCTS":
                            logging.info(f"action: applying_products | cli_id:{client_id}")
                            aggregated_chunks = self.apply_products(chunk)
                            if aggregated_chunks:
                                rows_1_3, rows_4_6, rows_7_8 = aggregated_chunks
                                self.accumulate_products(client_id, rows_1_3, rows_4_6, rows_7_8)
                                has_output = True
                                    
                        elif self.aggregator_type == "PURCHASES":
                            logging.info(f"action: applying_purchases | cli_id:{client_id}")
                            aggregated_chunk = self.apply_purchases(chunk)
                            logging.info(f"action: apply_purchases_completed | cli_id:{client_id} | has_chunk:{aggregated_chunk is not None}")
                            if aggregated_chunk:
                                logging.info(f"action: accumulating_purchases | cli_id:{client_id}")
                                self.accumulate_purchases(client_id, aggregated_chunk)
                                has_output = True
                                logging.info(f"action: accumulate_purchases_completed | cli_id:{client_id}")
                                
                        elif self.aggregator_type == "TPV":
                            logging.info(f"action: applying_tpv | cli_id:{client_id}")
                            aggregated_chunk = self.apply_tpv(chunk)
                            if aggregated_chunk:
                                self.accumulate_tpv(client_id, aggregated_chunk)
                                has_output = True
                        
                        logging.info(f"action: processing_completed | type:{self.aggregator_type} | cli_id:{client_id} | has_output:{has_output}")
                        
                        # Solo contar como procesado si generó output
                        if has_output:
                            self.chunks_processed_per_client[client_id][table_type] += 1
                        
                        # Verificar si ya recibimos end message para este client/table
                        if client_id not in self.end_message_received:
                            self.end_message_received[client_id] = {}

                        if self.end_message_received[client_id].get(table_type, False):
                            total_expected = self.chunks_to_receive[client_id][table_type]
                            total_received = self.chunks_received_per_client[client_id][table_type]
                            total_processed = self.chunks_processed_per_client[client_id][table_type]

                            # Enviar stats si aún no lo hice
                            if (client_id, table_type) not in self.already_sent_stats:
                                self.already_sent_stats[(client_id, table_type)] = True
                                stats_msg = AggregatorStatsMessage(self.aggregator_id, client_id, table_type, 
                                                                  total_expected, total_received, total_processed)
                                self.middleware_stats_exchange.send(stats_msg.encode())

                            # Verificar si puedo enviar end message
                            if self._can_send_end_message(total_expected, client_id, table_type):
                                self._send_end_message(client_id, table_type, total_expected, total_processed)
                        
                except Exception as e:
                    logging.error(f"action: exception_in_main_processing | type:{self.aggregator_type} | error:{str(e)} | error_type:{type(e).__name__}")
                    # Evitar log spam - solo loggear errores que no parecen ser datos corruptos
                    msg_preview = msg[:100] if len(msg) > 100 else msg
                    if not any(delimiter in str(msg_preview) for delimiter in [',', '-', 'H1', 'H2']):
                        logging.error(f"action: error_processing_message | type:{self.aggregator_type} | error:{e}")
                    else:
                        logging.debug(f"action: skipping_corrupted_data | type:{self.aggregator_type} | size:{len(msg)}")

                results.remove(msg)

    def apply_products(self, chunk):
        """
        Aplica agregación para productos por mes/año.
        Procesa un chunk y retorna chunks agregados divididos por rangos.
        """
        YEARS = {2024, 2025}
        # Acumuladores temporales para este chunk
        chunk_sellings = defaultdict(int)
        chunk_profit = defaultdict(float)
        
        for row in chunk.rows:
            if hasattr(row, 'item_id') and hasattr(row, 'quantity') and hasattr(row, 'subtotal') and hasattr(row, 'created_at'):
                # Verificar que sea de los años correctos
                if hasattr(row, 'month_year_created_at'):
                    dt = row.month_year_created_at
                    if dt.year in YEARS:
                        key = (row.item_id, dt.year, dt.month)
                        chunk_sellings[key] += row.quantity
                        chunk_profit[key] += row.subtotal

        # Crear las listas de salida divididas por rangos
        if not chunk_sellings:
            return None
            
        new_rows_items_1_3 = []
        new_rows_items_4_6 = []
        new_rows_items_7_8 = []
        
        for key, total_qty in chunk_sellings.items():
            total_profit = chunk_profit[key]
            item_id, year, month = key
            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
            
            new_row = TransactionItemsProcessRow(
                transaction_id="",
                item_id=item_id,
                quantity=total_qty,
                subtotal=total_profit,
                created_at=created_at
            )
            
            # Dividir por rangos
            if 1 <= item_id <= 3:
                new_rows_items_1_3.append(new_row)
            elif 4 <= item_id <= 6:
                new_rows_items_4_6.append(new_row)
            elif 7 <= item_id <= 8:
                new_rows_items_7_8.append(new_row)

        return new_rows_items_1_3, new_rows_items_4_6, new_rows_items_7_8

    def apply_purchases(self, chunk):
        """
        Aplica agregación para contar compras por cliente y store.
        Procesa un chunk y retorna un chunk agregado.
        """
        YEARS = {2024, 2025}
        # Acumulador temporal para este chunk
        chunk_accumulator = defaultdict(int)
        
        processed_rows = 0
        valid_years = 0
        parsing_errors = 0
        
        for row in chunk.rows:
            processed_rows += 1
            if hasattr(row, 'store_id') and hasattr(row, 'user_id') and hasattr(row, 'created_at'):
                # Descartar filas con store_id o user_id NULL (necesarios para Query 4)
                if row.store_id is None or row.user_id is None:
                    continue
                    
                # Parsear fecha
                created_at = row.created_at
                if isinstance(created_at, str):
                    try:
                        dt = datetime.datetime.fromisoformat(created_at)
                    except ValueError:
                        try:
                            dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            parsing_errors += 1
                            continue
                elif hasattr(created_at, 'date'):
                    # Manejar objetos DateTime con atributo .date
                    dt = datetime.datetime.combine(created_at.date, created_at.time if hasattr(created_at, 'time') else datetime.time(0, 0))
                elif hasattr(created_at, 'year'):
                    dt = created_at
                else:
                    parsing_errors += 1
                    continue
                    
                # Filtrar por años 2024-2025
                if dt.year in YEARS:
                    valid_years += 1
                    key = (int(row.store_id), int(row.user_id))
                    chunk_accumulator[key] += 1

        logging.info(f"action: apply_purchases_stats | client_id:{chunk.header.client_id} | processed:{processed_rows} | valid_years:{valid_years} | parsing_errors:{parsing_errors} | accumulated_keys:{len(chunk_accumulator)}")

        # Crear chunk de salida con los datos agregados de este chunk
        if not chunk_accumulator:
            logging.warning(f"action: apply_purchases_no_output | client_id:{chunk.header.client_id} | processed:{processed_rows} | valid_years:{valid_years}")
            return None
            
        rows = []
        marker_date = DateTime(datetime.date(2024, 1, 1), datetime.time(0, 0))
        
        for (store_id, user_id), count in chunk_accumulator.items():
            row = TransactionsProcessRow(
                transaction_id="",
                store_id=store_id,
                user_id=user_id,
                final_amount=float(count),
                created_at=marker_date,
            )
            rows.append(row)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TRANSACTIONS)
        return ProcessChunk(header, rows)

    def apply_tpv(self, chunk):
        """
        Aplica agregación para TPV por store y semestre.
        Procesa un chunk y retorna un chunk agregado.
        """
        from utils.file_utils.process_table import YearHalf
        
        YEARS = {2024, 2025}
        # Acumulador temporal para este chunk
        chunk_accumulator = defaultdict(float)
        
        processed_rows = 0
        valid_years = 0
        parsing_errors = 0
        
        for row in chunk.rows:
            processed_rows += 1
            if hasattr(row, 'store_id') and hasattr(row, 'final_amount') and hasattr(row, 'created_at'):
                # Parsear fecha
                created_at = row.created_at
                if isinstance(created_at, str):
                    try:
                        dt = datetime.datetime.fromisoformat(created_at)
                    except ValueError:
                        try:
                            dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            parsing_errors += 1
                            continue
                elif hasattr(created_at, 'date'):
                    # Manejar objetos DateTime con atributo .date
                    dt = datetime.datetime.combine(created_at.date, created_at.time if hasattr(created_at, 'time') else datetime.time(0, 0))
                elif hasattr(created_at, 'year'):
                    dt = created_at
                else:
                    parsing_errors += 1
                    continue
                    
                # Filtrar por años 2024-2025
                if dt.year in YEARS:
                    valid_years += 1
                    # Determinar semestre (1: Ene-Jun, 2: Jul-Dec)
                    semester = 1 if 1 <= dt.month <= 6 else 2
                    
                    try:
                        amount = float(row.final_amount)
                    except (ValueError, TypeError):
                        try:
                            amount = float(str(row.final_amount).replace(",", "."))
                        except (ValueError, TypeError):
                            parsing_errors += 1
                            continue
                    
                    key = (dt.year, semester, int(row.store_id))
                    chunk_accumulator[key] += amount

        logging.info(f"action: apply_tpv_stats | client_id:{chunk.header.client_id} | processed:{processed_rows} | valid_years:{valid_years} | parsing_errors:{parsing_errors} | accumulated_keys:{len(chunk_accumulator)}")

        # Crear chunk de salida con los datos agregados de este chunk
        if not chunk_accumulator:
            logging.warning(f"action: apply_tpv_no_output | client_id:{chunk.header.client_id} | processed:{processed_rows} | valid_years:{valid_years}")
            return None
            
        rows = []
        
        for (year, semester, store_id), total_tpv in chunk_accumulator.items():
            # Crear YearHalf para representar año y semestre
            year_half = YearHalf(year, semester)
            
            row = TPVProcessRow(
                store_id=store_id,
                tpv=total_tpv,
                year_half=year_half
            )
            rows.append(row)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TPV)
        return ProcessChunk(header, rows)

    def publish_purchases_chunk(self, aggregated_chunk):
        """
        Publica un chunk agregado de compras por cliente y store.
        """
        import base64
        
        queue = MessageMiddlewareQueue("rabbitmq", "transactions_sum_by_client")
        payload_b64 = base64.b64encode(aggregated_chunk.serialize()).decode("utf-8")
        queue.send(payload_b64)
        queue.close()
        
        logging.info(f"action: publish_purchases_chunk | result: success | rows:{len(aggregated_chunk.rows)}")

    def publish_tpv_chunk(self, aggregated_chunk):
        """
        Publica un chunk agregado de TPV por store y semestre directamente al joiner.
        """
        queue = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        chunk_data = aggregated_chunk.serialize()
        queue.send(chunk_data)  # Enviar datos directos, no en base64
        queue.close()
        
        logging.info(f"action: publish_tpv_chunk | result: success | rows:{len(aggregated_chunk.rows)} | queue:to_join_with_stores_tvp")

    def _ensure_dict_entry(self, dictionary, client_id, table_type, default=0):
        """Helper para inicializar entradas en diccionarios anidados"""
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = default

    def _can_send_end_message(self, total_expected, client_id, table_type):
        """Determina si este aggregator puede enviar el END message final"""
        return (total_expected == self.chunks_received_per_client[client_id][table_type] and 
                self.aggregator_id == 1)

    def _send_end_message(self, client_id, table_type, total_expected, total_processed):
        """Envía datos finales y END message a maximizers cuando todos los aggregators terminaron"""
        logging.info(f"action: sending_end_message | type:{self.aggregator_type} | cli_id:{client_id} | file_type:{table_type.name} | total_chunks:{total_processed}")
        
        # PRIMERO: Enviar resultados finales acumulados
        self.publish_final_results(client_id, table_type)
        
        # SEGUNDO: Enviar END a maximizers (después de enviar los datos)
        try:
            end_msg = MessageEnd(client_id, table_type, total_processed)
            for queue in self.middleware_queue_sender.values():
                queue.send(end_msg.encode()) 
            logging.info(f"action: sent_end_to_maximizers | type:{self.aggregator_type} | chunks:{total_processed}")
        except Exception as e:
            logging.error(f"action: error_sending_end_message | error:{e}")
        
        # Limpiar estado
        end_msg = AggregatorStatsEndMessage(self.aggregator_id, client_id, table_type)
        self.middleware_stats_exchange.send(end_msg.encode())
        self.delete_client_stats_data(end_msg)
        self.delete_client_accumulator(end_msg)

    def delete_client_accumulator(self, stats_end):
        # Limpiar acumulador global
        logging.info(f"action: deleting_client_accumulator | cli_id:{stats_end.client_id}")
        try:
            if stats_end.client_id in self.global_accumulator:
                del self.global_accumulator[stats_end.client_id]
        except KeyError:
            pass  # Ya estaba limpio
        logging.info(f"action: client_accumulator_deleted | cli_id:{stats_end.client_id}")

    def delete_client_stats_data(self, stats_end):
        """Limpia datos del cliente después de procesar"""
        logging.info(f"action: deleting_client_stats_data | cli_id:{stats_end.client_id}")
        try:
            if stats_end.client_id in self.end_message_received:
                if stats_end.table_type in self.end_message_received[stats_end.client_id]:
                    del self.end_message_received[stats_end.client_id][stats_end.table_type]
                if not self.end_message_received[stats_end.client_id]:
                    del self.end_message_received[stats_end.client_id]
            
            if stats_end.client_id in self.chunks_received_per_client:
                if stats_end.table_type in self.chunks_received_per_client[stats_end.client_id]:
                    del self.chunks_received_per_client[stats_end.client_id][stats_end.table_type]
                if not self.chunks_received_per_client[stats_end.client_id]:
                    del self.chunks_received_per_client[stats_end.client_id]
            
            if stats_end.client_id in self.chunks_processed_per_client:
                if stats_end.table_type in self.chunks_processed_per_client[stats_end.client_id]:
                    del self.chunks_processed_per_client[stats_end.client_id][stats_end.table_type]
                if not self.chunks_processed_per_client[stats_end.client_id]:
                    del self.chunks_processed_per_client[stats_end.client_id]
            
            if stats_end.client_id in self.chunks_to_receive:
                if stats_end.table_type in self.chunks_to_receive[stats_end.client_id]:
                    del self.chunks_to_receive[stats_end.client_id][stats_end.table_type]
                if not self.chunks_to_receive[stats_end.client_id]:
                    del self.chunks_to_receive[stats_end.client_id]
                    
            if (stats_end.client_id, stats_end.table_type) in self.already_sent_stats:
                del self.already_sent_stats[(stats_end.client_id, stats_end.table_type)]
            

            logging.info(f"action: client_stats_data_deleted | cli_id:{stats_end.client_id}")
        except KeyError:
            pass  # Ya estaba limpio

    def accumulate_products(self, client_id, rows_1_3, rows_4_6, rows_7_8):
        """Acumula productos agregados en memoria para envío final"""
        if client_id not in self.global_accumulator:
            self.global_accumulator[client_id] = {
                'products_1_3': defaultdict(lambda: {'quantity': 0, 'subtotal': 0.0}),
                'products_4_6': defaultdict(lambda: {'quantity': 0, 'subtotal': 0.0}),
                'products_7_8': defaultdict(lambda: {'quantity': 0, 'subtotal': 0.0})
            }
        
        # Acumular rows_1_3
        for row in rows_1_3:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            self.global_accumulator[client_id]['products_1_3'][key]['quantity'] += row.quantity
            self.global_accumulator[client_id]['products_1_3'][key]['subtotal'] += row.subtotal
            
        # Acumular rows_4_6  
        for row in rows_4_6:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            self.global_accumulator[client_id]['products_4_6'][key]['quantity'] += row.quantity
            self.global_accumulator[client_id]['products_4_6'][key]['subtotal'] += row.subtotal
            
        # Acumular rows_7_8
        for row in rows_7_8:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            self.global_accumulator[client_id]['products_7_8'][key]['quantity'] += row.quantity
            self.global_accumulator[client_id]['products_7_8'][key]['subtotal'] += row.subtotal

    def accumulate_purchases(self, client_id, aggregated_chunk):
        """Acumula compras agregadas en memoria para envío final"""
        if client_id not in self.global_accumulator:
            self.global_accumulator[client_id] = {
                'purchases': defaultdict(lambda: defaultdict(int))  # store_id -> {user_id -> count}
            }
        
        for row in aggregated_chunk.rows:
            store_id = int(row.store_id)
            user_id = int(row.user_id)
            count = int(row.final_amount)
            self.global_accumulator[client_id]['purchases'][store_id][user_id] += count

    def accumulate_tpv(self, client_id, aggregated_chunk):
        """Acumula TPV agregado en memoria para envío final"""
        if client_id not in self.global_accumulator:
            self.global_accumulator[client_id] = {
                'tpv': defaultdict(float)  # (year, semester, store_id) -> total_tpv
            }
        
        for row in aggregated_chunk.rows:
            # Usar directamente los campos de TPVProcessRow
            if isinstance(row, TPVProcessRow):
                key = (row.year_half.year, row.year_half.half, int(row.store_id))
                self.global_accumulator[client_id]['tpv'][key] += float(row.tpv)
            else:
                # Fallback para compatibilidad con versión anterior
                year = row.created_at.year
                semester = 1 if row.created_at.month <= 6 else 2
                key = (year, semester, int(row.store_id))
                self.global_accumulator[client_id]['tpv'][key] += float(row.final_amount)

    def publish_final_results(self, client_id, table_type):
        """Publica los resultados finales acumulados"""
        if client_id not in self.global_accumulator:
            logging.warning(f"action: publish_final_results | client_id:{client_id} | warning: no_accumulated_data")
            return
        
        logging.info(f"action: publish_final_results | type:{self.aggregator_type} | client_id:{client_id}")
        
        if self.aggregator_type == "PRODUCTS":
            self._publish_final_products(client_id)
        elif self.aggregator_type == "PURCHASES":
            self._publish_final_purchases(client_id)
        elif self.aggregator_type == "TPV":
            self._publish_final_tpv(client_id)

    def _publish_final_products(self, client_id):
        """Publica resultados finales de productos"""
        data = self.global_accumulator[client_id]
        
        # Crear header base
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
        
        # Publicar rangos 1-3
        if data['products_1_3']:
            rows_1_3 = []
            for (item_id, year, month), totals in data['products_1_3'].items():
                created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
                row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=totals['quantity'],
                    subtotal=totals['subtotal'],
                    created_at=created_at
                )
                rows_1_3.append(row)

            queue = self.middleware_queue_sender["to_max_1_3"]
            chunk_data = ProcessChunk(header, rows_1_3).serialize()
            queue.send(chunk_data)
            logging.info(f"action: publish_final_products_1_3 | client_id:{client_id} | rows:{len(rows_1_3)} | bytes_sent:{len(chunk_data)} | queue:to_max_1_3")
        
        # Publicar rangos 4-6
        if data['products_4_6']:
            rows_4_6 = []
            for (item_id, year, month), totals in data['products_4_6'].items():
                created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
                row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=totals['quantity'],
                    subtotal=totals['subtotal'],
                    created_at=created_at
                )
                rows_4_6.append(row)
            
            queue = self.middleware_queue_sender["to_max_4_6"]
            chunk_data = ProcessChunk(header, rows_4_6).serialize()
            queue.send(chunk_data)
            logging.info(f"action: publish_final_products_4_6 | client_id:{client_id} | rows:{len(rows_4_6)} | bytes_sent:{len(chunk_data)} | queue:to_max_4_6")
        
        # Publicar rangos 7-8
        if data['products_7_8']:
            rows_7_8 = []
            for (item_id, year, month), totals in data['products_7_8'].items():
                created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
                row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=totals['quantity'],
                    subtotal=totals['subtotal'],
                    created_at=created_at
                )
                rows_7_8.append(row)
            
            queue = self.middleware_queue_sender["to_max_7_8"]
            chunk_data = ProcessChunk(header, rows_7_8).serialize()
            queue.send(chunk_data)
            logging.info(f"action: publish_final_products_7_8 | client_id:{client_id} | rows:{len(rows_7_8)} | bytes_sent:{len(chunk_data)} | queue:to_max_7_8")

    def _publish_final_purchases(self, client_id):
        """Publica resultados finales de compras a maximizers según rangos de store_id"""
        data = self.global_accumulator[client_id]
        
        if not data['purchases']:
            return
        
        # Separar por rangos de store_id
        stores_1_3 = []
        stores_4_6 = []
        stores_7_10 = []
        
        placeholder_date = datetime.date(2024, 1, 1)
        
        for store_id, users_data in data['purchases'].items():
            # Crear filas con user_id y su conteo de transacciones para este store
            rows_for_store = []
            for user_id, count in users_data.items():
                # Usar PurchasesPerUserStoreRow con placeholders para store_name y user_birthdate
                # Los joiners harán el join después para obtener los datos reales
                row = PurchasesPerUserStoreRow(
                    store_id=store_id,
                    store_name="",  # Placeholder - lo llenará el joiner
                    user_id=user_id,
                    user_birthdate=placeholder_date,  # Placeholder - lo llenará el joiner  
                    purchases_made=count,
                )
                rows_for_store.append(row)
            
            # Asignar a la lista correspondiente según el rango de store_id
            if 1 <= store_id <= 3:
                stores_1_3.extend(rows_for_store)
            elif 4 <= store_id <= 6:
                stores_4_6.extend(rows_for_store)
            elif 7 <= store_id <= 10:
                stores_7_10.extend(rows_for_store)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        
        # Enviar a top 1-3
        if stores_1_3:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_1_3)
            chunk_data = chunk.serialize()
            queue = self.middleware_queue_sender["to_top_1_3"]
            queue.send(chunk_data)
            logging.info(f"action: publish_final_purchases_1_3 | client_id:{client_id} | rows:{len(stores_1_3)} | bytes_sent:{len(chunk_data)} | queue:to_top_1_3")
        
        # Enviar a top 4-6
        if stores_4_6:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_4_6)
            chunk_data = chunk.serialize()
            queue = self.middleware_queue_sender["to_top_4_6"]
            queue.send(chunk_data)
            logging.info(f"action: publish_final_purchases_4_6 | client_id:{client_id} | rows:{len(stores_4_6)} | bytes_sent:{len(chunk_data)} | queue:to_top_4_6")
        
        # Enviar a top 7-10
        if stores_7_10:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_7_10)
            chunk_data = chunk.serialize()
            queue = self.middleware_queue_sender["to_top_7_10"]
            queue.send(chunk_data)
            logging.info(f"action: publish_final_purchases_7_10 | client_id:{client_id} | rows:{len(stores_7_10)} | bytes_sent:{len(chunk_data)} | queue:to_top_7_10")

    def _publish_final_tpv(self, client_id):
        """Publica resultados finales de TPV"""
        data = self.global_accumulator[client_id]
        
        if not data['tpv']:
            return
        
        rows = []
        
        for (year, semester, store_id), total_tpv in data['tpv'].items():
            # Crear YearHalf para representar año y semestre
            from utils.file_utils.process_table import YearHalf
            year_half = YearHalf(year, semester)
            
            row = TPVProcessRow(
                store_id=store_id,
                tpv=total_tpv,
                year_half=year_half
            )
            rows.append(row)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TPV)
        chunk = ProcessChunk(header, rows)
        
        import base64
        queue = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        chunk_data = chunk.serialize()
        queue.send(chunk_data)  # Enviar datos directos, no en base64
        queue.close()
        
        logging.info(f"action: publish_final_tpv | client_id:{client_id} | rows:{len(rows)} | bytes_sent:{len(chunk_data)} | queue:to_join_with_stores_tvp")