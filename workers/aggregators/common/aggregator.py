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

TIMEOUT = 3

class Aggregator:
    def __init__(self, agg_type: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.aggregator_type = agg_type
        self.middleware_queue_sender = {}
        
        # No necesitamos acumuladores globales - todo se procesa por chunks
        
        if self.aggregator_type == "PRODUCTS":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_1")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        elif self.aggregator_type == "PURCHASES":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "transactions")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        elif self.aggregator_type == "TPV":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "transactions")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")

    def run(self):
        logging.info(f"Agregador iniciado. Tipo: {self.aggregator_type}")
        results = []
        end_received = False
        
        def callback(msg): results.append(msg)
        def stop():
            self.middleware_queue_receiver.stop_consuming()
        def end_callback(msg): 
            nonlocal end_received
            end_received = True
            self.middleware_queue_receiver.stop_consuming()

        while not end_received:
            self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
            self.middleware_queue_receiver.start_consuming(callback)
            
            # Escuchar end message en paralelo
            try:
                self.middleware_exchange_receiver.start_consuming(end_callback, timeout=0.1)
            except:
                pass  # Timeout es normal
                
            for msg in results:
                chunk = ProcessBatchReader.from_bytes(msg)
                logging.info(f"action: aggregate | type:{self.aggregator_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                
                if self.aggregator_type == "PRODUCTS":
                    aggregated_chunks = self.apply_products(chunk)
                    if aggregated_chunks:
                        rows_1_3, rows_4_6, rows_7_8 = aggregated_chunks
                        if rows_1_3:
                            self.publish_results_1_3(chunk, rows_1_3)
                        if rows_4_6:
                            self.publish_results_4_6(chunk, rows_4_6)
                        if rows_7_8:
                            self.publish_results_7_8(chunk, rows_7_8)
                elif self.aggregator_type == "PURCHASES":
                    aggregated_chunk = self.apply_purchases(chunk)
                    if aggregated_chunk:
                        self.publish_purchases_chunk(aggregated_chunk)
                elif self.aggregator_type == "TPV":
                    aggregated_chunk = self.apply_tpv(chunk)
                    if aggregated_chunk:
                        self.publish_tpv_chunk(aggregated_chunk)
                    
                results.remove(msg)
            
        # Enviar end message
        try:
            self.middleware_exchange_sender.send("")
            self.middleware_exchange_sender.close()
        except:
            pass

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
                        logging.debug(f"action: aggregate_product | item_id:{row.item_id} | year:{dt.year} | month:{dt.month} | qty:{row.quantity} | profit:{row.subtotal}")

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
        
        for row in chunk.rows:
            if hasattr(row, 'store_id') and hasattr(row, 'user_id') and hasattr(row, 'created_at'):
                # Parsear fecha
                created_at = row.created_at
                if isinstance(created_at, str):
                    try:
                        dt = datetime.datetime.fromisoformat(created_at)
                    except ValueError:
                        dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                elif hasattr(created_at, 'year'):
                    dt = created_at
                else:
                    continue
                    
                # Filtrar por años 2024-2025
                if dt.year in YEARS:
                    key = (int(row.store_id), int(row.user_id))
                    chunk_accumulator[key] += 1
                    logging.debug(f"action: count_purchase | store_id:{row.store_id} | user_id:{row.user_id} | year:{dt.year}")

        # Crear chunk de salida con los datos agregados de este chunk
        if not chunk_accumulator:
            return None
            
        rows = []
        marker_date = datetime.date(2024, 1, 1)
        
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
        YEARS = {2024, 2025}
        # Acumulador temporal para este chunk
        chunk_accumulator = defaultdict(float)
        
        for row in chunk.rows:
            if hasattr(row, 'store_id') and hasattr(row, 'final_amount') and hasattr(row, 'created_at'):
                # Parsear fecha
                created_at = row.created_at
                if isinstance(created_at, str):
                    try:
                        dt = datetime.datetime.fromisoformat(created_at)
                    except ValueError:
                        dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                elif hasattr(created_at, 'year'):
                    dt = created_at
                else:
                    continue
                    
                # Filtrar por años 2024-2025
                if dt.year in YEARS:
                    # Determinar semestre (1: Ene-Jun, 2: Jul-Dec)
                    semester = 1 if 1 <= dt.month <= 6 else 2
                    
                    try:
                        amount = float(row.final_amount)
                    except (ValueError, TypeError):
                        amount = float(str(row.final_amount).replace(",", "."))
                    
                    key = (dt.year, semester, int(row.store_id))
                    chunk_accumulator[key] += amount
                    logging.debug(f"action: sum_tpv | store_id:{row.store_id} | year:{dt.year} | semester:{semester} | amount:{amount}")

        # Crear chunk de salida con los datos agregados de este chunk
        if not chunk_accumulator:
            return None
            
        rows = []
        
        for (year, semester, store_id), total_tpv in chunk_accumulator.items():
            # Crear fecha representativa del semestre
            first_month = 1 if semester == 1 else 7
            created_date = datetime.date(year, first_month, 1)
            
            row = TransactionsProcessRow(
                transaction_id="",
                store_id=store_id,
                user_id=0,  # Valor dummy para TPV
                final_amount=total_tpv,
                created_at=created_date,
            )
            rows.append(row)
        
        from utils.file_utils.process_chunk import ProcessChunkHeader
        from utils.file_utils.table_type import TableType
        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TRANSACTIONS)
        return ProcessChunk(header, rows)
    
    def publish_results_1_3(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_1_3 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())

    def publish_results_4_6(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_4_6 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())
    
    def publish_results_7_8(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_7_8 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())

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
        Publica un chunk agregado de TPV por store y semestre.
        """
        import base64
        
        queue = MessageMiddlewareQueue("rabbitmq", "results_query_3")
        payload_b64 = base64.b64encode(aggregated_chunk.serialize()).decode("utf-8")
        queue.send(payload_b64)
        queue.close()
        
        logging.info(f"action: publish_tpv_chunk | result: success | rows:{len(aggregated_chunk.rows)}")
        
        # Cerrar conexiones
        try:
            self.middleware_queue_receiver.close()
        except:
            pass