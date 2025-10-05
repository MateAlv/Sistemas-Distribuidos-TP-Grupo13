import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.end_message import MessageEnd
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange
from .filter_stats_message import FilterStatsMessage

TIMEOUT = 3

class Filter:
    def __init__(self, cfg: dict):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.id = cfg["id"]
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]
        self.end_message_received = {}
        self.number_of_chunks_received_per_client = {}
        self.number_of_chunks_not_sent_per_client = {}
        self.middleware_queue_sender = {}
        self.number_of_chunks_to_receive = {}
        self.already_sent_stats = {}
        self.middleware_end_exchange = MessageMiddlewareExchange("rabbitmq", f"end_exchange_filter_{self.filter_type}", [""], "fanout")
        
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
        stats_results = []
        def callback(msg): results.append(msg)
        def stats_callback(msg): stats_results.append(msg)
        def stop():
            self.middleware_queue_receiver.stop_consuming()
        def stats_stop():
            self.middleware_end_exchange.stop_consuming()

        while True:
            self.middleware_end_exchange.connection.call_later(TIMEOUT, stats_stop)
            self.middleware_end_exchange.start_consuming(stats_callback)       

            self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
            self.middleware_queue_receiver.start_consuming(callback)

            for stats_msg in stats_results:
                try:
                    stats = FilterStatsMessage.decode(stats_msg)
                    if stats.filter_id == self.id:
                        stats_results.remove(stats_msg)
                        continue
                    logging.info(f"action: stats_received | type:{self.filter_type} | filter_id:{stats.filter_id} | cli_id:{stats.client_id} | file_type:{stats.table_type} | chunks_received:{stats.chunks_received} | chunks_not_sent:{stats.chunks_not_sent}")
                    if stats.client_id not in self.end_message_received:
                        self.end_message_received[stats.client_id] = True

                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, stats.client_id, stats.table_type)
                    self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, stats.client_id, stats.table_type)
                    self.number_of_chunks_received_per_client[stats.client_id][stats.table_type] += stats.chunks_received
                    self.number_of_chunks_not_sent_per_client[stats.client_id][stats.table_type] += stats.chunks_not_sent
                    
                except Exception as e:
                    logging.error(f"Error decoding stats message: {e}")
                stats_results.remove(stats_msg)

            for msg in results:
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
                            if table_type == TableType.TRANSACTION_ITEMS and queue_name in ["to_filter_2", "to_agg_4"]:
                                continue
                            elif table_type == TableType.TRANSACTIONS and queue_name in ["to_agg_1"]:
                                continue
                            queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())
                    else:
                        logging.info(f"action: no_rows_to_send | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
                        self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                        self.number_of_chunks_not_sent_per_client[client_id][table_type] += 1
                    
                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)
                    self.number_of_chunks_received_per_client[client_id][table_type] += 1
                    
                    if self.end_message_received.get(client_id, False):
                        total_expected = self.number_of_chunks_to_receive[client_id][table_type]
                        self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)
                        self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                        total_received = self.number_of_chunks_received_per_client[client_id][table_type]
                        total_not_sent = self.number_of_chunks_not_sent_per_client[client_id][table_type]


                        if (client_id, table_type) not in self.already_sent_stats:
                            self.already_sent_stats[(client_id, table_type)] = True
                            stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_received, total_not_sent)
                            self.middleware_end_exchange.send(stats_msg.encode())
                        else:
                            stats_msg = FilterStatsMessage(self.id, client_id, table_type, 1, 0 if filtered_rows else 1)

                        if total_expected == self.number_of_chunks_received_per_client[client_id][table_type] and self.id == 1:
                            logging.info(f"action: sending_end_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks:{chunk.count()}")
                            for queue_name, queue in self.middleware_queue_sender.items():
                                logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{chunk.count()}")
                                if table_type == TableType.TRANSACTION_ITEMS and queue_name in ["to_filter_2", "to_agg_4"]:
                                    continue
                                elif table_type == TableType.TRANSACTIONS and queue_name in ["to_agg_1"]:
                                    continue
                                msg_to_send = MessageEnd(client_id, table_type, total_expected - total_not_sent)
                                queue.send(msg_to_send.encode())
                        
                except:
                    end_message = MessageEnd.decode(msg)
                    client_id = end_message.client_id()
                    table_type = end_message.table_type()
                    self.end_message_received[client_id] = True
                    total_expected = end_message.total_chunks()
                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)

                    logging.info(f"action: end_message_received | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks_received:{self.number_of_chunks_received_per_client[client_id][table_type]}")
                    
                    
                    self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                    self.number_of_chunks_to_receive[client_id] = {table_type: total_expected}

                    stats_msg = FilterStatsMessage(self.id, client_id, table_type, 
                                self.number_of_chunks_received_per_client[client_id][table_type],
                                self.number_of_chunks_not_sent_per_client[client_id][table_type])
                    self.middleware_end_exchange.send(stats_msg.encode())

                    if total_expected == self.number_of_chunks_received_per_client[client_id][table_type]:
                        logging.info(f"action: sending_end_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type} | total_chunks:{total_expected}")

                        for queue_name, queue in self.middleware_queue_sender.items():
                            logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{total_expected}")
                            if table_type == TableType.TRANSACTION_ITEMS and queue_name in ["to_filter_2", "to_agg_4"]:
                                    continue
                            elif table_type == TableType.TRANSACTIONS and queue_name in ["to_agg_1"]:
                                continue
                            msg_to_send = MessageEnd(client_id, table_type, total_expected - self.number_of_chunks_not_sent_per_client[client_id][table_type])
                            queue.send(msg_to_send.encode())

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
            return tx.total_amount >= self.cfg["min_amount"]

        logging.error(f"Filtro desconocido: {self.filter_type}")
        return False
    
    def _ensure_dict_entry(self, dictionary, client_id, table_type):
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = 0
