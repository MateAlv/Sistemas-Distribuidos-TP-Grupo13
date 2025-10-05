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

class Aggregator:
    def __init__(self, agg_type: str):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.aggregator_type = agg_type
        self.middleware_queue_sender = {}
        
        if self.aggregator_type == "PRODUCTS":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_1")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")

    def run(self):
        logging.info(f"Agregador iniciado. Tipo: {self.aggregator_type}")
        results = []
        def callback(msg): results.append(msg)
        def stop():
            self.middleware_queue_receiver.stop_consuming()

        while True:
            self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
            self.middleware_queue_receiver.start_consuming(callback)
            for msg in results:
                chunk = ProcessBatchReader.from_bytes(msg)
                logging.info(f"action: aggregate | type:{self.aggregator_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                rows_1_3, rows_4_6, rows_7_8 = self.apply(chunk.rows)
                len_rows_out = len(rows_1_3) + len(rows_4_6) + len(rows_7_8)
                logging.info(f"action: filter_result | type:{self.aggregator_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len_rows_out}")
                self.publish_results_1_3(chunk, rows_1_3)
                self.publish_results_4_6(chunk, rows_4_6)
                self.publish_results_7_8(chunk, rows_7_8)
                results.remove(msg)

    def apply(self, rows: list[TableProcessRow]) -> bool:
        """
        Aplica el agrupador según el tipo configurado.
        """
        if self.aggregator_type == "PRODUCTS":
            # Implementar lógica de agregación por productos
            sellings_accumulator = defaultdict(int)
            profit_accumulator = defaultdict(float)
            for row in rows:
                key = (row.item_id, row.month_year_created_at.year, row.month_year_created_at.month)
                sellings_accumulator[key] += row.quantity
                profit_accumulator[key] += row.subtotal

            new_rows_items_1_3 = []
            new_rows_items_4_6 = []
            new_rows_items_7_8 = []
            for key, total_qty in sellings_accumulator.items():
                total_profit = profit_accumulator[key]
                item_id, year, month = key
                created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
                new_row = TransactionItemsProcessRow(
                    transaction_id="",
                    item_id=item_id,
                    quantity=total_qty,
                    subtotal=total_profit,
                    created_at=created_at
                )
                assert new_row.quantity == total_qty
                assert new_row.subtotal == total_profit
                assert new_row.item_id == item_id
                assert new_row.month_year_created_at.year == year
                assert new_row.month_year_created_at.month == month
                logging.debug(f"action: aggregated_row | item_id:{item_id} | year:{year} | month:{month} | total_qty:{total_qty} | total_profit:{total_profit}")
                if 1 <= item_id <= 3:
                    new_rows_items_1_3.append(new_row)
                elif 4 <= item_id <= 6:
                    new_rows_items_4_6.append(new_row)
                elif 7 <= item_id <= 8:
                    new_rows_items_7_8.append(new_row)

            return new_rows_items_1_3, new_rows_items_4_6, new_rows_items_7_8

        logging.error(f"Agrupador desconocido: {self.aggregator_type}")
        return []
    
    def publish_results_1_3(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_products_1_3")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_1_3 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())

    def publish_results_4_6(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_products_4_6")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_4_6 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())
    
    def publish_results_7_8(self, chunk, aggregated_rows: list[TableProcessRow]):
        # Enviar los resultados a la cola correspondiente
        queue = MessageMiddlewareQueue("rabbitmq", "to_max_products_7_8")
        logging.info(f"action: sending_to_queue | type:{self.aggregator_type} | queue:to_max_products_7_8 | rows:{len(aggregated_rows)}")
        queue.send(ProcessChunk(chunk.header, aggregated_rows).serialize())