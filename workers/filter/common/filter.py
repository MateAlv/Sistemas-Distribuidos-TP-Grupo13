import logging
from collections import deque
from utils.processing.process_table import TableProcessRow
from utils.processing.process_chunk import ProcessChunk
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.eof_protocol.end_messages import MessageEnd, MessageQueryEnd
from utils.protocol import COORDINATION_EXCHANGE, MSG_WORKER_END, MSG_WORKER_STATS, MSG_BARRIER_FORWARD, DEFAULT_SHARD, STAGE_FILTER_YEAR, STAGE_FILTER_HOUR, STAGE_FILTER_AMOUNT
from utils.file_utils.table_type import TableType, ResultTableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange, TIMEOUT, \
    MessageMiddlewareMessageError
from utils.tolerance.persistence_service import PersistenceService
from .filter_stats_messages import FilterStatsMessage, FilterStatsEndMessage
from .filter_working_state import FilterWorkingState
from utils.results.result_table import Query1ResultRow

class Filter:
    def __init__(self, cfg: dict, monitor=None):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        self.monitor = monitor
        
        self.__running = True
        
        self.id = cfg["id"]
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]
        # Stage name for centralized barrier
        if self.filter_type == "year":
            self.stage = STAGE_FILTER_YEAR
        elif self.filter_type == "hour":
            self.stage = STAGE_FILTER_HOUR
        elif self.filter_type == "amount":
            self.stage = STAGE_FILTER_AMOUNT
        else:
            self.stage = f"filter_{self.filter_type}"

        self.working_state = FilterWorkingState()

        self.middleware_queue_sender = {}

        self.middleware_end_exchange = MessageMiddlewareExchange("rabbitmq", 
                                                                 f"end_exchange_filter_{self.filter_type}", 
                                                                 f"{self.id}", 
                                                                 "fanout")
        # Coordination publisher (fire-and-forget)
        self.middleware_coordination = MessageMiddlewareExchange(
            "rabbitmq",
            COORDINATION_EXCHANGE,
            f"filter_{self.filter_type}_{self.id}",
            "topic",
            routing_keys=[f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"],
        )
        self.stats_timer = None
        self.consume_timer = None
        self.coord_timer = None
        self.barrier_forwarded = set()
        
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
        
        logging.info(f"Filtro inicializado. Tipo: {self.filter_type}, ID: {self.id}"
                     f" | Receiver Queue: {self.middleware_queue_receiver.queue_name}"
                     f" | Sender Queues: {list(self.middleware_queue_sender.keys())}"
                     f" | End Exchange: end_exchange_filter_{self.filter_type}"
                     f" | Monitor: {self.monitor}"
                     )

        logging.info("Verificando recuperación de procesamiento previo")
        self.persistence_service = PersistenceService()

    def handle_processing_recovery(self):

        last_processing_chunk = self.persistence_service.recover_last_processing_chunk()

        if last_processing_chunk:
            logging.info("Chunk recuperado pendiente de procesamiento")

            client_id = last_processing_chunk.client_id()
            table_type = last_processing_chunk.table_type()
            message_id = last_processing_chunk.message_id()

            chunk_was_sent = self.process_chunk(last_processing_chunk)
            # Se actualiza el working state según si el chunk fue enviado o no
            if self.persistence_service.process_has_been_counted(last_processing_chunk):
                if chunk_was_sent:
                    self.working_state.increase_received_chunks(client_id, table_type, self.id, 1)
                else:
                    self.working_state.increase_not_sent_chunks(client_id, table_type, self.id, 1)
            # Se commitea el working state
            self.persistence_service.commit_working_state(self.working_state.to_bytes(), message_id)

    def run(self):
        logging.info(f"Filtro iniciado. Tipo: {self.filter_type}, ID: {self.id}")

        self.handle_processing_recovery()

        results = deque()
        stats_results = deque()
        def callback(msg): results.append(msg)
        def stats_callback(msg): stats_results.append(msg)
        def coord_callback(msg): results.append((b"BARRIER", msg))
        def stop():
            if not self.middleware_queue_receiver.connection or not self.middleware_queue_receiver.connection.is_open:
                return
            self.middleware_queue_receiver.stop_consuming()

        def stats_stop():
            if not self.middleware_end_exchange.connection or not self.middleware_end_exchange.connection.is_open:
                return
            self.middleware_end_exchange.stop_consuming()

        def coord_stop():
            if not self.middleware_coordination.connection or not self.middleware_coordination.connection.is_open:
                return
            self.middleware_coordination.stop_consuming()

        while self.__running:


            try:
                logging.debug("action: start_consuming_stats")
                self.stats_timer = self.middleware_end_exchange.connection.call_later(TIMEOUT, stats_stop)
                self.middleware_end_exchange.start_consuming(stats_callback)
                logging.debug("action: stop_consuming_stats")
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo: {e}")

            try:
                logging.debug("action: start_consuming_receiver")
                self.consume_timer = self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
                self.middleware_queue_receiver.start_consuming(callback)
                logging.debug("action: stop_consuming_receiver")
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                 logging.error(f"Error en consumo: {e}")

            # Consume coordination barrier forwards
            try:
                logging.debug("action: start_consuming_coordination")
                self.coord_timer = self.middleware_coordination.connection.call_later(TIMEOUT, coord_stop)
                self.middleware_coordination.start_consuming(coord_callback)
                logging.debug("action: stop_consuming_coordination")
            except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                logging.error(f"Error en consumo coordination: {e}")

            while stats_results:

                stats_msg = stats_results.popleft()
                try:
                    if stats_msg.startswith(b"STATS_END"):
                        stats_end = FilterStatsEndMessage.decode(stats_msg)
                        if stats_end.filter_id == self.id:
                            continue
                        logging.debug(f"action: stats_end_received | type:{self.filter_type} | filter_id:{stats_end.filter_id} | cli_id:{stats_end.client_id} | table_type:{stats_end.table_type}")
                        self.working_state.delete_client_stats_data(stats_end)
                    else:
                        stats = FilterStatsMessage.decode(stats_msg)
                        if stats.filter_id == self.id:
                            continue
                        logging.debug(f"action: stats_received | type:{self.filter_type} | filter_id:{stats.filter_id} | cli_id:{stats.client_id} | file_type:{stats.table_type} | chunks_received:{stats.chunks_received} | chunks_not_sent:{stats.chunks_not_sent}")

                        # Get OWN stats to send (not the sum of all filters)
                        own_received = self.working_state.get_own_chunks_received(stats.client_id, stats.table_type, self.id)
                        own_not_sent = self.working_state.get_own_chunks_not_sent(stats.client_id, stats.table_type, self.id)

                        # Si no se han enviado las estadísticas o si los valores cambiaron, enviarlas
                        if self.working_state.should_send_stats(stats.client_id, stats.table_type, own_received, own_not_sent):
                            stats_msg = FilterStatsMessage(self.id, stats.client_id, stats.table_type, stats.total_expected, own_received, own_not_sent)
                            self.middleware_end_exchange.send(stats_msg.encode())
                            # Se registra que ya se enviaron las estadísticas con estos valores
                            self.working_state.mark_stats_sent(stats.client_id, stats.table_type, own_received, own_not_sent)

                        self.working_state.end_received(stats.client_id, stats.table_type)

                        self.working_state.update_stats_received(stats.client_id, stats.table_type, stats)

                        if self.working_state.can_send_end_message(stats.client_id, stats.table_type, stats.total_expected, self.id):
                            chunks_not_sent = self.working_state.get_total_not_sent_chunks(stats.client_id, stats.table_type)
                            self._send_end_message(stats.client_id, stats.table_type, stats.total_expected, chunks_not_sent)

                except Exception as e:
                    logging.error(f"action: error_decoding_stats_message | error:{e}")

            while results:

                msg = results.popleft()
                try:
                    if isinstance(msg, tuple) and msg[0] == b"BARRIER":
                        self._handle_barrier_forward(msg[1])
                    elif msg.startswith(b"END;"):
                        self._handle_end_message(msg)

                    else:
                        self._handle_process_message(msg)

                except Exception as e:
                    logging.error(f"action: error_decoding_message | error:{e}")

    def _handle_process_message(self, msg):
        chunk = ProcessBatchReader.from_bytes(msg)
        client_id = chunk.client_id()
        table_type = chunk.table_type()

        # Idempotency check
        if chunk.message_id() in self.working_state.processed_ids:
            logging.info(f"action: duplicate_chunk_ignored | message_id:{chunk.message_id()}")
            return

        # Se commitea el chunk a procesar
        self.persistence_service.commit_processing_chunk(chunk)
        # Se procesa el chunk
        chunk_was_sent = self.process_chunk(chunk)

        if chunk_was_sent:
            self.working_state.increase_received_chunks(client_id, table_type, self.id, 1)
        else:
            self.working_state.increase_not_sent_chunks(client_id, table_type, self.id, 1)

        # Se commitea el working state
        self.working_state.processed_ids.add(chunk.message_id())
        self.persistence_service.commit_working_state(self.working_state.to_bytes(), chunk.message_id())

        if self.working_state.end_is_received(client_id, table_type):
            total_expected = self.working_state.get_total_chunks_to_receive(client_id, table_type)
            total_received = self.working_state.get_total_chunks_received(client_id, table_type)
            total_not_sent = self.working_state.get_total_not_sent_chunks(client_id, table_type)

            # Send OWN stats (not the sum of all filters)
            own_received = self.working_state.get_own_chunks_received(client_id, table_type, self.id)
            own_not_sent = self.working_state.get_own_chunks_not_sent(client_id, table_type, self.id)

            if self.working_state.should_send_stats(client_id, table_type, own_received, own_not_sent):
                stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, own_received, own_not_sent)
                self.middleware_end_exchange.send(stats_msg.encode())
                self.working_state.mark_stats_sent(client_id, table_type, own_received, own_not_sent)
            """
            else:
                stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, 1, 0 if chunk_was_sent else 1)
            """
            # Use TOTAL to check if all filters are done
            total_received = self.working_state.get_total_chunks_received(client_id, table_type)
            total_not_sent = self.working_state.get_total_not_sent_chunks(client_id, table_type)
            
            if self.working_state.can_send_end_message(client_id, table_type, total_expected, self.id):
                self._send_end_message(client_id, table_type, total_expected, total_not_sent)

    def _handle_end_message(self, msg):
        end_message = MessageEnd.decode(msg)
        client_id = end_message.client_id()
        table_type = end_message.table_type()
        total_expected = end_message.total_chunks()
        
        # For amount filter, ensure the output queue exists even if no chunks were processed
        if self.filter_type == "amount" and f"to_merge_data_{client_id}" not in self.middleware_queue_sender:
            self.middleware_queue_sender[f"to_merge_data_{client_id}"] = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            logging.info(f"action: created_merge_queue_on_end | cli_id:{client_id} | queue:to_merge_data_{client_id}")
        
        # Get own stats for this filter
        own_received = self.working_state.get_own_chunks_received(client_id, table_type, self.id)
        own_not_sent = self.working_state.get_own_chunks_not_sent(client_id, table_type, self.id)

        sender_id = end_message.sender_id()

        self.working_state.end_received(client_id, table_type)
        logging.info(
            "action: end_message_received | type:%s | cli_id:%s | file_type:%s | sender_id:%s | chunks_received:%d | chunks_not_sent:%d | chunks_expected:%d",
            self.filter_type, client_id, table_type, sender_id,
            own_received,
            own_not_sent,
            total_expected)

        self.working_state.set_total_chunks_expected(client_id, table_type, total_expected)

        # Only send stats if not already sent or if values changed - send OWN stats, not total
        if self.working_state.should_send_stats(client_id, table_type, own_received, own_not_sent):
            stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, own_received, own_not_sent)

            logging.info(
                f"action: sending_stats_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type.name} | chunks_received:{own_received} | chunks_not_sent:{own_not_sent} | chunks_expected:{total_expected}")
            logging.debug(f"action: sending_stats_message_raw | msg:{stats_msg.encode()}")
            self.middleware_end_exchange.send(stats_msg.encode())
            # Publish coordination stats (throttled by should_send_stats)
            try:
                payload = {
                    "type": MSG_WORKER_STATS,
                    "id": str(self.id),
                    "client_id": client_id,
                    "stage": self.stage,
                    "expected": total_expected,
                    "chunks": own_received,
                    "not_sent": own_not_sent,
                    "sender": str(self.id),
                }
                rk = f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"
                self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
                logging.debug(f"action: coordination_stats_sent | stage:{self.stage} | cli_id:{client_id} | received:{own_received} | not_sent:{own_not_sent}")
            except Exception as e:
                logging.error(f"action: coordination_stats_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
            # Mark stats as sent with current values
            self.working_state.mark_stats_sent(client_id, table_type, own_received, own_not_sent)

        # Use TOTAL to check if all filters are done
        total_received = self.working_state.get_total_chunks_received(client_id, table_type)
        total_not_sent = self.working_state.get_total_not_sent_chunks(client_id, table_type)
        
        if self.working_state.can_send_end_message(client_id, table_type, total_expected, self.id):
            self._send_end_message(client_id, table_type, total_expected, total_not_sent)
        else:
            logging.debug(
                f"action: not_sending_end_message_yet | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type.name} | chunks_received:{total_received} | chunks_not_sent:{total_not_sent} | chunks_expected:{total_expected}")

    def _send_end_message(self, client_id, table_type, total_expected, total_not_sent):
        logging.info(f"action: sending_end_message | type:{self.filter_type} | cli_id:{client_id} | file_type:{table_type.name} | total_chunks:{total_expected-total_not_sent}")
        
        for queue_name, queue in self.middleware_queue_sender.items():
            if self._should_skip_queue(table_type, queue_name, client_id):
                continue
            logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{total_expected-total_not_sent}")
            msg_to_send = self._end_message_to_send(client_id, table_type, total_expected, total_not_sent)
            queue.send(msg_to_send.encode())
        # Publish to coordination for centralized barrier
        try:
            payload = {
                "type": MSG_WORKER_END,
                "id": str(self.id),
                "client_id": client_id,
                "stage": self.stage,
                "expected": total_expected,
                "chunks": total_expected - total_not_sent,
                "sender": str(self.id),
            }
            rk = f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            logging.debug(f"action: coordination_end_sent | stage:{self.stage} | cli_id:{client_id} | chunks:{total_expected-total_not_sent}")
        except Exception as e:
            logging.error(f"action: coordination_end_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
        end_msg = FilterStatsEndMessage(self.id, client_id, table_type)
        self.middleware_end_exchange.send(end_msg.encode())
        self.working_state.delete_client_stats_data(end_msg)

    def _end_message_to_send(self, client_id, table_type, total_expected, total_not_sent):
        if self.filter_type != "amount":
            return MessageEnd(client_id, table_type, total_expected - total_not_sent, str(self.id))
        else:
            return MessageQueryEnd(client_id, ResultTableType.QUERY_1, total_expected - total_not_sent)

    def _handle_barrier_forward(self, raw_msg):
        try:
            data = json.loads(raw_msg)
            if data.get("type") != MSG_BARRIER_FORWARD:
                return
            client_id = data.get("client_id")
            stage = data.get("stage")
            shard = data.get("shard", DEFAULT_SHARD)
            if stage != self.stage:
                return
            key = (client_id, stage, shard)
            if key in self.barrier_forwarded:
                return
            total_chunks = data.get("total_chunks", 0)
            total_expected = total_chunks
            # Send stats to monitor to record expected
            try:
                own_received = self.working_state.get_own_chunks_received(client_id, TableType.TRANSACTIONS, self.id)
                own_not_sent = self.working_state.get_own_chunks_not_sent(client_id, TableType.TRANSACTIONS, self.id)
                payload = {
                    "type": MSG_WORKER_STATS,
                    "id": str(self.id),
                    "client_id": client_id,
                    "stage": self.stage,
                    "expected": total_expected,
                    "chunks": own_received,
                    "not_sent": own_not_sent,
                    "sender": str(self.id),
                }
                self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key="coordination.filter")
            except Exception as e:
                logging.error(f"action: coordination_stats_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
            # Forward END downstream
            self._send_end_message(client_id, TableType.TRANSACTIONS, total_expected, 0)
            self.barrier_forwarded.add(key)
            logging.info(f"action: barrier_forward_consumed | stage:{self.stage} | cli_id:{client_id} | total_chunks:{total_chunks}")
        except Exception as e:
            logging.error(f"action: barrier_forward_error | stage:{self.stage} | error:{e}")
            
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

    def process_chunk(self, chunk: ProcessChunk):
        client_id = chunk.client_id()
        message_id = chunk.message_id()

        if self.filter_type == "amount" and f"to_merge_data_{client_id}" not in self.middleware_queue_sender:
            self.middleware_queue_sender[f"to_merge_data_{client_id}"] = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

        table_type = chunk.table_type()
        logging.debug(f"action: filter | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
        filtered_rows = [tx for tx in chunk.rows if self.apply(tx)]
        logging.debug(f"action: filter_result | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
        if filtered_rows:
            for queue_name, queue in self.middleware_queue_sender.items():
                if self._should_skip_queue(table_type, queue_name, client_id):
                    continue
                logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()}")
                if self.filter_type != "amount":
                    queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())

                else:
                    converted_rows = [ Query1ResultRow(tx.transaction_id, tx.final_amount) for tx in filtered_rows]
                    queue.send(ResultChunk(ResultChunkHeader(client_id, ResultTableType.QUERY_1), converted_rows).serialize())

            logging.info(f"action: rows_sent | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
            # Se commitea el envío del chunk procesado
            self.persistence_service.commit_send_ack(client_id, message_id)
        else:
            logging.info(f"action: no_rows_to_send | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")
            return False

        return True


    def _should_skip_queue(self, table_type: TableType, queue_name: str, client_id: int) -> bool:
        if table_type == TableType.TRANSACTION_ITEMS and queue_name in ["to_filter_2", "to_agg_4"]:
            return True
        if table_type == TableType.TRANSACTIONS and queue_name in ["to_agg_1+2"]:
            return True
        if self.filter_type == "amount" and queue_name != f"to_merge_data_{client_id}":
            return True
        
        return False

    def shutdown(self, signum=None, frame=None):
        logging.info(f"SIGTERM recibido: cerrando filtro {self.filter_type} (ID: {self.id})")
        try:
            if self.stats_timer is not None:
                self.stats_timer.cancel()
        except Exception:
            pass

        try:
            if self.consume_timer is not None:
                self.consume_timer.cancel()
        except Exception:
            pass

        # Detener consumo de las colas
        try:
            self.middleware_queue_receiver.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass

        try:
            self.middleware_end_exchange.stop_consuming()
        except (OSError, RuntimeError, AttributeError):
            pass

        for sender in getattr(self, "middleware_queue_sender", {}).values():
            try:
                sender.stop_consuming()
            except (OSError, RuntimeError, AttributeError):
                pass

        # Cerrar conexiones
        try:
            self.middleware_queue_receiver.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        try:
            self.middleware_end_exchange.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        for sender in getattr(self, "middleware_queue_sender", {}).values():
            try:
                sender.close()
            except (OSError, RuntimeError, AttributeError):
                pass

        try:
            self.middleware_coordination.close()
        except (OSError, RuntimeError, AttributeError):
            pass

        # Detener el loop principal
        self.__running = False

        # Limpiar estructuras
        self.working_state.destroy()

        logging.info(f"Filtro {self.filter_type} cerrado correctamente.")
