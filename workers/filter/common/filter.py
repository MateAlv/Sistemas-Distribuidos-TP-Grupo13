import logging
from utils.file_utils.process_table import TableProcessRow
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.result_chunk import ResultChunkHeader, ResultChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.end_messages import MessageEnd, MessageQueryEnd
from utils.file_utils.table_type import TableType, ResultTableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange, TIMEOUT
from .filter_stats_messages import FilterStatsMessage, FilterStatsEndMessage



class Filter:
    def __init__(self, cfg: dict):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
        self.id = cfg["id"]
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]
        self.end_message_received = {}
        self.number_of_chunks_received_per_client = {}
        self.number_of_chunks_not_sent_per_client = {}
        self.number_of_chunks_to_receive = {}
        self.already_sent_stats = {}
        self.middleware_queue_sender = {}

        self.middleware_end_exchange = MessageMiddlewareExchange("rabbitmq", f"end_exchange_filter_{self.filter_type}", [""], "fanout")
        
        if self.filter_type == "year":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_1")
            self.middleware_queue_sender["to_filter_2"] = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_agg_1+2"] = MessageMiddlewareQueue("rabbitmq", "to_agg_1+2")
            self.middleware_queue_sender["to_agg_4"] = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
        elif self.filter_type == "hour":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_2")
            self.middleware_queue_sender["to_filter_3"] = MessageMiddlewareQueue("rabbitmq", "to_filter_3")
            self.middleware_queue_sender["to_agg_3"] = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
        elif self.filter_type == "amount":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_filter_3")
        else:
            raise ValueError(f"Tipo de filtro inválido: {self.filter_type}")

    def run(self):
        logging.info(f"Filtro iniciado. Tipo: {self.filter_type}, ID: {self.id}")
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
                    logging.debug(f"action: stats_received | type:{self.filter_type} | filter_id:{stats.filter_id} | cli_id:{stats.client_id} | file_type:{stats.table_type} | chunks_received:{stats.chunks_received} | chunks_not_sent:{stats.chunks_not_sent}")
                    if stats.client_id not in self.end_message_received:
                        self.end_message_received[stats.client_id] = {}
                    self.end_message_received[stats.client_id][stats.table_type] = True

                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, stats.client_id, stats.table_type)
                    self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, stats.client_id, stats.table_type)
                    self.number_of_chunks_received_per_client[stats.client_id][stats.table_type] += stats.chunks_received
                    self.number_of_chunks_not_sent_per_client[stats.client_id][stats.table_type] += stats.chunks_not_sent
                    
                    total_received = self.number_of_chunks_received_per_client[stats.client_id][stats.table_type]
                    total_not_sent= self.number_of_chunks_not_sent_per_client[stats.client_id][stats.table_type]
                    
                    if (stats.client_id, stats.table_type) not in self.already_sent_stats:
                        self.already_sent_stats[(stats.client_id, stats.table_type)] = True
                        stats_msg = FilterStatsMessage(self.id, stats.client_id, stats.table_type, stats.total_expected, total_received, total_not_sent)
                        self.middleware_end_exchange.send(stats_msg.encode())
                        
                    if self._can_send_end_message(stats.total_expected, stats.client_id, stats.table_type):
                        self._send_end_message(stats.client_id, stats.table_type, stats.total_expected, self.number_of_chunks_not_sent_per_client[stats.client_id][stats.table_type])

                except:
                    stats_end = FilterStatsEndMessage.decode(stats_msg)
                    if stats_end.filter_id == self.id:
                        stats_results.remove(stats_msg)
                        continue
                    logging.debug(f"action: stats_end_received | type:{self.filter_type} | filter_id:{stats_end.filter_id} | cli_id:{stats_end.client_id} | table_type:{stats_end.table_type}")
                    self.delete_client_data(stats_end)
                    stats_results.remove(stats_msg)

            for msg in results:
                try:
                    chunk = ProcessBatchReader.from_bytes(msg)
                    client_id = chunk.client_id()
                    
                    if self.filter_type == "amount" and client_id not in self.middleware_queue_sender:
                        self.middleware_queue_sender[f"to_merge_data_{client_id}"] = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

                    table_type = chunk.table_type()
                    logging.debug(f"action: filter | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
                    filtered_rows = [tx for tx in chunk.rows if self.apply(tx)]
                    logging.debug(f"action: filter_result | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
                    if filtered_rows:
                        for queue_name, queue in self.middleware_queue_sender.items():
                            logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()}")
                            if self._should_skip_queue(table_type, queue_name):
                                continue
                            if self.filter_type != "amount":
                                queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())
                            else:
                                from utils.file_utils.result_table import Query1ResultRow
                                converted_rows = [ Query1ResultRow(tx.transaction_id, tx.final_amount) for tx in filtered_rows]
                                queue.send(ResultChunk(ResultChunkHeader(client_id, ResultTableType.QUERY_1), converted_rows).serialize())
                    else:
                        logging.info(f"action: no_rows_to_send | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
                        self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                        self.number_of_chunks_not_sent_per_client[client_id][table_type] += 1
                    
                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)
                    self.number_of_chunks_received_per_client[client_id][table_type] += 1
                    if client_id not in self.end_message_received:
                        self.end_message_received[client_id] = {}

                    if self.end_message_received[client_id].get(table_type, False):
                        total_expected = self.number_of_chunks_to_receive[client_id][table_type]
                        self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)
                        self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                        total_received = self.number_of_chunks_received_per_client[client_id][table_type]
                        total_not_sent = self.number_of_chunks_not_sent_per_client[client_id][table_type]


                        if (client_id, table_type) not in self.already_sent_stats:
                            self.already_sent_stats[(client_id, table_type)] = True
                            stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, total_received, total_not_sent)
                            self.middleware_end_exchange.send(stats_msg.encode())
                        else:
                            stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, 1, 0 if filtered_rows else 1)

                        if self._can_send_end_message(total_expected, client_id, table_type):
                            self._send_end_message(client_id, table_type, total_expected, total_not_sent)
                        
                except:
                    end_message = MessageEnd.decode(msg)
                    client_id = end_message.client_id()
                    table_type = end_message.table_type()
                    if client_id not in self.end_message_received:
                        self.end_message_received[client_id] = {}
                    self.end_message_received[client_id][table_type] = True
                    total_expected = end_message.total_chunks()
                    self._ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)

                    logging.info("action: end_message_received | type:%s | cli_id:%s | file_type:%s | chunks_received:%d | chunks_not_sent:%d | chunks_expected:%d",
                                self.filter_type, client_id, table_type, 
                                self.number_of_chunks_received_per_client[client_id][table_type],
                                self.number_of_chunks_not_sent_per_client.get(client_id, {}).get(table_type, 0),
                                total_expected)
                    
                    self._ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
                    self.number_of_chunks_to_receive[client_id] = {table_type: total_expected}

                    stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected,
                                self.number_of_chunks_received_per_client[client_id][table_type],
                                self.number_of_chunks_not_sent_per_client[client_id][table_type])
                    self.middleware_end_exchange.send(stats_msg.encode())

                    if self._can_send_end_message(total_expected, client_id, table_type):
                        self._send_end_message(client_id, table_type, total_expected, self.number_of_chunks_not_sent_per_client[client_id][table_type])

                results.remove(msg)

    def delete_client_data(self, stats_end):
        del self.end_message_received[stats_end.client_id][stats_end.table_type]
        del self.number_of_chunks_received_per_client[stats_end.client_id][stats_end.table_type]
        del self.number_of_chunks_not_sent_per_client[stats_end.client_id][stats_end.table_type]
        del self.number_of_chunks_to_receive[stats_end.client_id][stats_end.table_type]

        if (stats_end.client_id, stats_end.table_type) in self.already_sent_stats:
            del self.already_sent_stats[(stats_end.client_id, stats_end.table_type)]
        if self.end_message_received[stats_end.client_id] == {}:
            del self.end_message_received[stats_end.client_id]
        if self.number_of_chunks_received_per_client[stats_end.client_id] == {}:
            del self.number_of_chunks_received_per_client[stats_end.client_id]
        if self.number_of_chunks_not_sent_per_client[stats_end.client_id] == {}:
            del self.number_of_chunks_not_sent_per_client[stats_end.client_id]
        if self.number_of_chunks_to_receive[stats_end.client_id] == {}:
            del self.number_of_chunks_to_receive[stats_end.client_id]

    def _can_send_end_message(self, total_expected, client_id, table_type):
        return total_expected == self.number_of_chunks_received_per_client[client_id][table_type] and self.id == 1

    def _send_end_message(self, client_id, table_type, total_expected, total_not_sent):
        logging.info(f"action: sending_end_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type.name} | total_chunks:{total_expected-total_not_sent}")
        
        for queue_name, queue in self.middleware_queue_sender.items():
            logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{total_expected-total_not_sent}")
            if self._should_skip_queue(table_type, queue_name):
                continue
            msg_to_send = self._end_message_to_send(client_id, table_type, total_expected, total_not_sent)
            queue.send(msg_to_send.encode())
        end_msg = FilterStatsEndMessage(self.id, client_id, table_type)
        self.middleware_end_exchange.send(end_msg.encode())
        self.delete_client_data(end_msg)

    def _end_message_to_send(self, client_id, table_type, total_expected, total_not_sent):
        if self.filter_type != "amount":
            return MessageEnd(client_id, table_type, total_expected - total_not_sent)
        else:
            return MessageQueryEnd(client_id, ResultTableType.QUERY_1, total_expected - total_not_sent)
            
    def apply(self, tx: TableProcessRow) -> bool:
        """
        Aplica el filtro según el tipo configurado.
        """
        if self.filter_type == "year":
            return self.cfg["year_start"] <= tx.created_at.date.year <= self.cfg["year_end"]

        elif self.filter_type == "hour":
            return self.cfg["hour_start"] <= tx.created_at.time.hour <= self.cfg["hour_end"]

        elif self.filter_type == "amount":
            return tx.final_amount >= self.cfg["min_amount"]


        logging.error(f"Filtro desconocido: {self.filter_type}")
        return False
    
    def _ensure_dict_entry(self, dictionary, client_id, table_type, default=0):
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = default

    def _should_skip_queue(self, table_type: TableType, queue_name: str) -> bool:
        if table_type == TableType.TRANSACTION_ITEMS and queue_name in ["to_filter_2", "to_agg_4"]:
            return True
        if table_type == TableType.TRANSACTIONS and queue_name in ["to_agg_1+2"]:
            return True
        return False