import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.end_message import MessageEnd
from middleware.middleware_interface import MessageMiddlewareQueue

TIMEOUT = 3

class Filter:
    def __init__(self, cfg: dict):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.id = cfg["id"]
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]
        self.end_message_received = False
        self.number_of_chunks_received_per_client = {}
        self.number_of_chunks_not_sent_per_client = {}
        self.middleware_queue_sender = {}
        
        if self.filter_type == "year":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_1")
            self.middleware_queue_sender["to_filter_2"] = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_agg_1"] = MessageMiddlewareQueue("rabbitmq", "to_agg_1")
            self.middleware_queue_sender["to_agg_4"] = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
        elif self.filter_type == "hour":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_filter_3"] = MessageMiddlewareQueue("rabbitmq", "to_filter_amount")
            self.middleware_queue_sender["to_agg_3"] = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
        elif self.filter_type == "amount":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_amount")
            self.middleware_queue_sender["to_merge_data"] = MessageMiddlewareQueue("rabbitmq", "to_merge_data")
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
                chunk = None
                try:
                    chunk = ProcessBatchReader.from_bytes(msg)
                    client_id = chunk.client_id()
                    table_type = chunk.table_type()

                    logging.info(f"action: filter | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                    filtered_rows = [tx for tx in chunk.rows if self.apply(tx)]
                    logging.info(f"action: filter_result | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
                    if filtered_rows:
                        for queue_name, queue in self.middleware_queue_sender.items():
                            logging.info(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)}")
                            queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())
                    else:
                        logging.info(f"action: no_rows_to_send | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
                        if client_id not in self.number_of_chunks_not_sent_per_client:
                            self.number_of_chunks_not_sent_per_client[client_id] = 0
                        self.number_of_chunks_not_sent_per_client[client_id] += 1
                    
                    if client_id not in self.number_of_chunks_received_per_client:
                        self.number_of_chunks_received_per_client[client_id] = 0
                    self.number_of_chunks_received_per_client[client_id] += 1
                except:
                    chunk = MessageEnd.decode(msg)
                    client_id = chunk.client_id()
                    table_type = chunk.table_type()
                    self.end_message_received = True
                    logging.info(f"action: end_message_received | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks_received:{self.number_of_chunks_received_per_client.get(client_id, 0)} | total_chunks_not_sent:{self.number_of_chunks_not_sent_per_client.get(client_id, 0)}")
                    if client_id not in self.number_of_chunks_received_per_client:
                        self.number_of_chunks_received_per_client[client_id] = 0
                    if chunk.count() == self.number_of_chunks_received_per_client[client_id]:
                        logging.info(f"action: sending_end_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks:{chunk.count()}")
                        for queue_name, queue in self.middleware_queue_sender.items():
                            logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{chunk.count()}")
                            queue.send(chunk.encode())


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
