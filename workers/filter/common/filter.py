import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
TIMEOUT = 3

class Filter:
    def __init__(self, cfg: dict):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]
        self.middleware_queue_sender = {}
        
        if self.filter_type == "year":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_1")
            self.middleware_queue_sender["to_filter_2"] = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_agg_1"] = MessageMiddlewareQueue("rabbitmq", "to_agg_1")
            self.middleware_queue_sender["to_agg_4"] = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "FIRST_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
        elif self.filter_type == "hour":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_filter_3"] = MessageMiddlewareQueue("rabbitmq", "to_filter_amount")
            self.middleware_queue_sender["to_agg_3"] = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "SECOND_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "THIRD_END_MESSAGE", [""], exchange_type="fanout")
        elif self.filter_type == "amount":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_amount")
            self.middleware_queue_sender["to_merge_data"] = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
            self.middleware_exchange_receiver = MessageMiddlewareExchange("rabbitmq", "THIRD_END_MESSAGE", [""], exchange_type="fanout")
            self.middleware_exchange_sender = MessageMiddlewareExchange("rabbitmq", "...", [""], exchange_type="fanout")
        else:
            raise ValueError(f"Tipo de filtro inválido: {self.filter_type}")



    def run(self):
        logging.info(f"Filtro iniciado. Tipo: {self.filter_type}")
        results = []
        def callback(msg): results.append(msg)
        def stop():
            self.middleware_queue_receiver.stop_consuming()

        while True:
            self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
            self.middleware_queue_receiver.start_consuming(callback)
            for msg in results:
                chunk = ProcessBatchReader.from_bytes(msg)
                logging.info(f"action: filter | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                filtered_rows = [tx for tx in chunk.rows if self.apply(tx)]
                logging.info(f"action: filter_result | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
                if filtered_rows:
                    for queue_name, queue in self.middleware_queue_sender.items():
                        logging.info(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)}")
                        queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())
                results.remove(msg)
            
    def apply(self, tx: TableProcessRow) -> bool:
        """
        Aplica el filtro según el tipo configurado.
        """
        if self.filter_type == "year":
            return self.cfg["year_start"] <= tx.created_at.date.year <= self.cfg["year_end"]

        elif self.filter_type == "hour":
            return self.cfg["hour_start"] <= tx.created_at.time.hour <= self.cfg["hour_end"]

        elif self.filter_type == "amount":
            # Verificar el tipo de fila para acceder al atributo correcto
            if hasattr(tx, 'final_amount'):
                # TransactionsProcessRow - usar final_amount (monto final)
                amount = tx.final_amount
                result = amount >= self.cfg["min_amount"]
                if not result:
                    logging.debug(f"FILTERED OUT: TransactionsProcessRow final_amount={amount} < min_amount={self.cfg['min_amount']}")
                return result
            elif hasattr(tx, 'subtotal'):
                # TransactionsItemsProcessRow - usar subtotal (monto por item)
                amount = tx.subtotal
                result = amount >= self.cfg["min_amount"]
                if not result:
                    logging.debug(f"FILTERED OUT: TransactionsItemsProcessRow subtotal={amount} < min_amount={self.cfg['min_amount']}")
                return result
            else:
                logging.warning(f"Tipo de fila no reconocido para filtro amount: {type(tx)}")
                return False

        logging.error(f"Filtro desconocido: {self.filter_type}")
        return False
