import logging
import json
import os
import uuid
from collections import deque, defaultdict
from utils.processing.process_table import TableProcessRow
from utils.processing.process_chunk import ProcessChunk
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.eof_protocol.end_messages import MessageEnd, MessageQueryEnd
from utils.protocol import (
    COORDINATION_EXCHANGE,
    MSG_WORKER_END,
    MSG_WORKER_STATS,
    STAGE_FILTER_YEAR,
    STAGE_FILTER_HOUR,
    STAGE_FILTER_AMOUNT,
    STAGE_AGG_PRODUCTS,
    STAGE_AGG_PURCHASES,
    STAGE_AGG_TPV,
)
from utils.file_utils.table_type import TableType, ResultTableType
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange, TIMEOUT, \
    MessageMiddlewareMessageError
from utils.tolerance.persistence_service import PersistenceService
from utils.tolerance.crash_helper import crash_after_two_chunks, crash_after_end_processed
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
        # Shard config
        self.shard_id = os.getenv("FILTER_SHARD_ID", "1")
        self.shard_count = int(os.getenv("FILTER_SHARDS", "1"))
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
        # Shard counts for downstream routing
        self.products_shards = int(os.getenv("PRODUCTS_SHARDS", "1"))
        self.purchases_shards = int(os.getenv("PURCHASES_SHARDS", "1"))
        self.tpv_shards = int(os.getenv("TPV_SHARDS", "1"))
        # Track chunk index per destination/client/table to compute shard deterministically
        self.chunk_counters = defaultdict(int)
        # Track how many chunks we actually sent to each shard so END carries correct per-shard expected count
        # Key: (stage, client_id, table_type, shard_id) -> chunks_sent
        self.shard_chunks_sent = defaultdict(int)

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
            routing_keys=[f"coordination.barrier.{self.stage}.shard.{self.shard_id}"],
        )

        self.force_end_exchange = MessageMiddlewareExchange(
            "rabbitmq",
            "FORCE_END_EXCHANGE",
            "server",
            "fanout",
            routing_keys=[""],
        )
        
        self.stats_timer = None
        self.consume_timer = None
        
        if self.filter_type == "year":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_filter_year_shard_{self.shard_id}")
            self.middleware_queue_sender["to_filter_hour"] = MessageMiddlewareQueue("rabbitmq", f"to_filter_hour_shard_{self.shard_id}")
        elif self.filter_type == "hour":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_filter_hour_shard_{self.shard_id}")
            self.middleware_queue_sender["to_filter_amount"] = MessageMiddlewareQueue("rabbitmq", f"to_filter_amount_shard_{self.shard_id}")
        elif self.filter_type == "amount":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_filter_amount_shard_{self.shard_id}")
        else:
            raise ValueError(f"Tipo de filtro inválido: {self.filter_type}")
        
        logging.info(f"Filtro inicializado. Tipo: {self.filter_type}, ID: {self.id}"
                     f" | Receiver Queue: {self.middleware_queue_receiver.queue_name}"
                     f" | Sender Queues: {list(self.middleware_queue_sender.keys())}"
                     f" | End Exchange: end_exchange_filter_{self.filter_type}"
                     f" | Monitor: {self.monitor}"
                     )

        logging.info("Verificando recuperación de procesamiento previo")
        # Support PERSISTENCE_DIR for testing (defaults to /data/persistence for production)
        persistence_dir = os.getenv("PERSISTENCE_DIR", "/data/persistence")
        self.persistence_service = PersistenceService(f"{persistence_dir}/filter_{self.filter_type}_{self.id}")
        self.handle_processing_recovery()

    def handle_processing_recovery(self):

        # First, recover the working state (processed_ids, global_processed_ids, etc.)
        recovered_state_bytes = self.persistence_service.recover_working_state()
        if recovered_state_bytes:
            try:
                # Deserialize and restore working state
                self.working_state = self.working_state.__class__.from_bytes(recovered_state_bytes)
                logging.info(f"Working state recovered: {len(self.working_state.processed_ids)} processed chunks, "
                           f"{len(self.working_state.global_processed_ids)} global processed")
            except Exception as e:
                logging.warning(f"Could not recover working state: {e}")
        
        # Then, recover any last processing chunk that was interrupted
        last_processing_chunk = self.persistence_service.recover_last_processing_chunk()

        if last_processing_chunk:
            logging.info("Chunk recuperado pendiente de procesamiento")

            client_id = last_processing_chunk.client_id()
            table_type = last_processing_chunk.table_type()
            message_id = last_processing_chunk.message_id()

            filtered_rows = self.process_chunk(last_processing_chunk, force=True)
            # Se actualiza el working state solo si el chunk NO fue contado previamente
            if not self.persistence_service.process_has_been_counted(last_processing_chunk.message_id()):
                if filtered_rows:
                    self.working_state.increase_received_chunks(client_id, table_type, self.id, 1)
                else:
                    self.working_state.increase_not_sent_chunks(client_id, table_type, self.id, 1)
            # Se commitea el working state
            self.persistence_service.commit_working_state(self.working_state.to_bytes(), message_id)

            # Send filtered rows (crucial for recovery if crash happened before send)
            self.send_filtered_rows(filtered_rows, last_processing_chunk, client_id, table_type, message_id)

            # Check if we need to send END/Stats (logic copied from _handle_process_message)
            # This is needed if we crashed after processing the last chunk but before sending END
            if self.working_state.end_is_received(client_id, table_type):
                total_expected = self.working_state.get_total_chunks_to_receive(client_id, table_type)
                
                # Send OWN stats (not the sum of all filters)
                own_received = self.working_state.get_own_chunks_received(client_id, table_type, self.id)
                own_not_sent = self.working_state.get_own_chunks_not_sent(client_id, table_type, self.id)

                if self.working_state.should_send_stats(client_id, table_type, own_received, own_not_sent):
                    stats_msg = FilterStatsMessage(self.id, client_id, table_type, total_expected, own_received, own_not_sent)
                    self.middleware_end_exchange.send(stats_msg.encode())
                    self.working_state.mark_stats_sent(client_id, table_type, own_received, own_not_sent)

                if self.working_state.can_send_end_message(client_id, table_type, total_expected, self.id):
                    total_not_sent = self.working_state.get_total_not_sent_chunks(client_id, table_type)
                    self._send_end_message(client_id, table_type, total_expected, total_not_sent)

    def run(self):
        logging.info(f"Filtro iniciado. Tipo: {self.filter_type}, ID: {self.id}")

        results = deque()
        stats_results = deque()
        def callback(msg):
            if msg.startswith(b"END;"):
                end_message = MessageEnd.decode(msg)
                client_id = end_message.client_id()
                table_type = end_message.table_type()
                self.working_state.end_received(client_id, table_type)
                self.persistence_service.commit_working_state(self.working_state.to_bytes(), uuid.uuid4())
                # REVISAR LÓGICA DE PERSISTENCIA Y RECOVERY DE END
            else:
                # Crash point for testing: before commit_processing_chunk
                if os.getenv("CRASH_POINT") == "CRASH_BEFORE_COMMIT_PROCESSING":
                    raise SystemExit("Simulated crash before commit_processing_chunk")
                
                chunk = ProcessBatchReader.from_bytes(msg)
                # Se commitea el chunk a procesar
                self.persistence_service.commit_processing_chunk(chunk)
                
                # Crash point for testing: after commit_processing_chunk
                if os.getenv("CRASH_POINT") == "CRASH_AFTER_COMMIT_PROCESSING":
                    raise SystemExit("Simulated crash after commit_processing_chunk")
            results.append(msg)
            
        def stats_callback(msg): stats_results.append(msg)
        def stop():
            if not self.middleware_queue_receiver.connection or not self.middleware_queue_receiver.connection.is_open:
                return
            self.middleware_queue_receiver.stop_consuming()

        def stats_stop():
            if not self.middleware_end_exchange.connection or not self.middleware_end_exchange.connection.is_open:
                return
            self.middleware_end_exchange.stop_consuming()

        while self.__running:


            # Skip end_exchange consumption in test mode (no downstream workers)
            test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
            if not test_mode:
                try:
                    self.stats_timer = self.middleware_end_exchange.connection.call_later(TIMEOUT, stats_stop)
                    self.middleware_end_exchange.start_consuming(stats_callback)
                except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                    logging.error(f"Error en consumo: {e}")

            # Skip coordination exchange in test mode (no coordination infrastructure)
            if not test_mode:
                try:
                    self.consume_timer = self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
                    self.middleware_queue_receiver.start_consuming(callback)
                except (OSError, RuntimeError, MessageMiddlewareMessageError) as e:
                     logging.error(f"Error en consumo: {e}")

            while stats_results:

                stats_msg = stats_results.popleft()
                try:
                    if stats_msg.startswith(b"STATS_END"):
                        # Ignore peer STATS_END to avoid mixing replicas
                        continue
                    else:
                        stats = FilterStatsMessage.decode(stats_msg)

                        self.working_state.update_stats_received(stats.client_id, stats.table_type, stats)

                        if self.working_state.can_send_end_message(stats.client_id, stats.table_type, stats.total_expected, self.id):
                            chunks_not_sent = self.working_state.get_total_not_sent_chunks(stats.client_id, stats.table_type)
                            self._send_end_message(stats.client_id, stats.table_type, stats.total_expected, chunks_not_sent)
                        
                        # Persist state after updating stats from other filters
                        self.persistence_service.commit_working_state(self.working_state.to_bytes(), uuid.uuid4())

                except Exception as e:
                    logging.error(f"action: error_decoding_stats_message | error:{e}")

            while results:

                msg = results.popleft()
                try:
                    if msg.startswith(b"END;"):
                        self._handle_end_message(msg)

                    else:
                        self._handle_process_message(msg)

                except Exception as e:
                    logging.error(f"action: error_decoding_message | error:{e}")

    def _handle_process_message(self, msg):
        chunk = ProcessBatchReader.from_bytes(msg)
        client_id = chunk.client_id()
        table_type = chunk.table_type()
        message_id = chunk.message_id()

        # Idempotency check
        msg_id = chunk.message_id()
        msg_id_str = str(msg_id)
        if msg_id in self.working_state.processed_ids:
            logging.info(f"action: duplicate_chunk_ignored | message_id:{msg_id}")
            return
        # Global dedup across replicas per filter type
        if msg_id_str in self.working_state.global_processed_ids:
            logging.info(f"action: global_duplicate_chunk_ignored | message_id:{msg_id}")
            return

        crash_after_two_chunks("filter")

        # 1. Apply filter - returns filtered rows
        filtered_rows = self.process_chunk(chunk)

        # 2. Update working state (in-memory)
        if filtered_rows:
            self.working_state.increase_received_chunks(client_id, table_type, self.id, 1)
        else:
            self.working_state.increase_not_sent_chunks(client_id, table_type, self.id, 1)

        # Se commitea el working state
        self.working_state.processed_ids.add(msg_id)
        self.working_state.global_processed_ids.add(msg_id_str)
        
        # Crash point for testing: before commit_working_state
        if os.getenv("CRASH_POINT") == "CRASH_BEFORE_COMMIT_WORKING_STATE":
            raise SystemExit("Simulated crash before commit_working_state")
        self.persistence_service.commit_working_state(self.working_state.to_bytes(), msg_id)

        # 4. Send to next stage (only if there are filtered rows)
        self.send_filtered_rows(filtered_rows, chunk, client_id, table_type, message_id)

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
                crash_after_end_processed("filter")

    def send_filtered_rows(self, filtered_rows, chunk, client_id, table_type, message_id):
        
        if filtered_rows:
            # Dispatch according to filter type and destination
            if self.filter_type == "year":
                if table_type == TableType.TRANSACTIONS:
                    queue = self.middleware_queue_sender["to_filter_hour"]
                    logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue.queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()} | shard:{self.shard_id}")
                    queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())

                if table_type == TableType.TRANSACTION_ITEMS:
                    # Q2: route items to products aggregator shards
                    shard_id = self._next_shard("agg_products", client_id, table_type, self.products_shards)
                    queue_name = f"to_agg_products_shard_{shard_id}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()} | shard:{shard_id}")
                    self.middleware_queue_sender[queue_name].send(ProcessChunk(chunk.header, filtered_rows).serialize())
                    self.shard_chunks_sent[(STAGE_AGG_PRODUCTS, client_id, table_type, shard_id)] += 1
                elif table_type == TableType.TRANSACTIONS:
                    # Q4: route transactions to purchases aggregator shards
                    shard_id = self._next_shard("agg_purchases", client_id, table_type, self.purchases_shards)
                    queue_name = f"to_agg_purchases_shard_{shard_id}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()} | shard:{shard_id}")
                    self.middleware_queue_sender[queue_name].send(ProcessChunk(chunk.header, filtered_rows).serialize())
                    self.shard_chunks_sent[(STAGE_AGG_PURCHASES, client_id, table_type, shard_id)] += 1

            elif self.filter_type == "hour":
                # Forward to amount filter for Q1
                queue = self.middleware_queue_sender["to_filter_amount"]
                logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue.queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()} | shard:{self.shard_id}")
                queue.send(ProcessChunk(chunk.header, filtered_rows).serialize())

                if table_type == TableType.TRANSACTIONS:
                    # Q3: route transactions to TPV aggregator shards
                    shard_id = self._next_shard("agg_tpv", client_id, table_type, self.tpv_shards)
                    queue_name = f"to_agg_tpv_shard_{shard_id}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()} | shard:{shard_id}")
                    self.middleware_queue_sender[queue_name].send(ProcessChunk(chunk.header, filtered_rows).serialize())
                    self.shard_chunks_sent[(STAGE_AGG_TPV, client_id, table_type, shard_id)] += 1
                    logging.info(
                        f"DEBUGGING_QUERY_3 | filter_hour_to_agg_tpv | cli_id:{client_id} | shard:{shard_id} | rows_out:{len(filtered_rows)} | shard_total:{self.shard_chunks_sent[(STAGE_AGG_TPV, client_id, table_type, shard_id)]}"
                    )

            elif self.filter_type == "amount":
                converted_rows = [Query1ResultRow(tx.transaction_id, tx.final_amount) for tx in filtered_rows]
                # Amount filter only sends to merge queue per client
                queue_name = f"to_merge_data_{client_id}"
                if queue_name in self.middleware_queue_sender:
                    queue = self.middleware_queue_sender[queue_name]
                    logging.debug(f"action: sending_to_queue | type:{self.filter_type} | queue:{queue_name} | rows:{len(filtered_rows)/len(chunk.rows):.2%} | cli_id:{chunk.client_id()}")
                    queue.send(ResultChunk(ResultChunkHeader(client_id, ResultTableType.QUERY_1), converted_rows).serialize())
                else:
                    logging.error(f"action: queue_not_found | queue:{queue_name} | cli_id:{client_id}")

            logging.info(f"action: rows_sent | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
        else:
            logging.info(f"action: no_rows_to_send | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()}")

        # Se commitea el envío del chunk procesado (incluso si no hubo filas, para marcarlo como completado)
        self.persistence_service.commit_send_ack(client_id, message_id)
        return True


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

        logging.info(
            "action: end_message_received | type:%s | cli_id:%s | file_type:%s | sender_id:%s | chunks_received:%d | chunks_not_sent:%d | chunks_expected:%d",
            self.filter_type, client_id, table_type, sender_id,
            own_received,
            own_not_sent,
            total_expected)

        self.working_state.set_total_chunks_expected(client_id, table_type, total_expected)
        self.working_state.end_received(client_id, table_type)

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
                    "shard": self.shard_id,
                }
                rk = f"coordination.barrier.{self.stage}.shard.{self.shard_id}"
                self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
                logging.debug(f"action: coordination_stats_sent | stage:{self.stage} | cli_id:{client_id} | received:{own_received} | not_sent:{own_not_sent}")
            except Exception as e:
                logging.error(f"action: coordination_stats_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
            # Mark stats as sent with current values
            self.working_state.mark_stats_sent(client_id, table_type, own_received, own_not_sent)

        # Persist state after handling END message and potential stats update
        self.persistence_service.commit_working_state(self.working_state.to_bytes(), uuid.uuid4())

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
        
        # 1. Send to static queues (next filters)
        for queue_name, queue in self.middleware_queue_sender.items():
            if "shard" in queue_name: continue # Skip dynamic shard queues for now
            if self._should_skip_queue(table_type, queue_name, client_id):
                continue
            if self.filter_type == "amount" and "to_merge_data" in queue_name:
                msg = MessageQueryEnd(client_id, ResultTableType.QUERY_1, total_expected - total_not_sent)
                queue.send(msg.encode())
                logging.info(f"action: sent_query_end_q1 | client_id:{client_id} | count:{total_expected - total_not_sent}")
                continue

            logging.info(f"action: sending_end_to_queue | type:{self.filter_type} | queue:{queue_name} | total_chunks:{total_expected-total_not_sent}")
            msg_to_send = self._end_message_to_send(client_id, table_type, total_expected, total_not_sent)
            queue.send(msg_to_send.encode())

        # 2. Send to dynamic shard queues (Aggregators) - Ensure ALL shards get END
        if self.filter_type == "year":
            if table_type == TableType.TRANSACTION_ITEMS:
                # Q2: Products
                for i in range(1, self.products_shards + 1):
                    queue_name = f"to_agg_products_shard_{i}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    shard_total = self.shard_chunks_sent.get((STAGE_AGG_PRODUCTS, client_id, table_type, i), 0)
                    logging.info(f"action: sending_end_to_shard | type:{self.filter_type} | queue:{queue_name} | total_chunks:{shard_total}")
                    msg_to_send = self._end_message_to_send(client_id, table_type, shard_total, 0)
                    self.middleware_queue_sender[queue_name].send(msg_to_send.encode())
            elif table_type == TableType.TRANSACTIONS:
                # Q4: Purchases
                for i in range(1, self.purchases_shards + 1):
                    queue_name = f"to_agg_purchases_shard_{i}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    shard_total = self.shard_chunks_sent.get((STAGE_AGG_PURCHASES, client_id, table_type, i), 0)
                    logging.info(f"action: sending_end_to_shard | type:{self.filter_type} | queue:{queue_name} | total_chunks:{shard_total}")
                    msg_to_send = self._end_message_to_send(client_id, table_type, shard_total, 0)
                    self.middleware_queue_sender[queue_name].send(msg_to_send.encode())

        elif self.filter_type == "hour":
            if table_type == TableType.TRANSACTIONS:
                # Q3: TPV
                for i in range(1, self.tpv_shards + 1):
                    queue_name = f"to_agg_tpv_shard_{i}"
                    if queue_name not in self.middleware_queue_sender:
                        self.middleware_queue_sender[queue_name] = MessageMiddlewareQueue("rabbitmq", queue_name)
                    shard_total = self.shard_chunks_sent.get((STAGE_AGG_TPV, client_id, table_type, i), 0)
                    logging.info(f"action: sending_end_to_shard | type:{self.filter_type} | queue:{queue_name} | total_chunks:{shard_total}")
                    msg_to_send = self._end_message_to_send(client_id, table_type, shard_total, 0)
                    self.middleware_queue_sender[queue_name].send(msg_to_send.encode())

        # Publish to coordination for centralized barrier - MONITOR
        try:
            payload = {
                "type": MSG_WORKER_END,
                "id": str(self.id),
                "client_id": client_id,
                "stage": self.stage,
                "expected": total_expected,
                "chunks": total_expected - total_not_sent,
                "sender": str(self.id),
                "shard": self.shard_id,
            }
            rk = f"coordination.barrier.{self.stage}.shard.{self.shard_id}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            logging.debug(f"action: coordination_end_sent | stage:{self.stage} | cli_id:{client_id} | chunks:{total_expected-total_not_sent}")
        except Exception as e:
            logging.error(f"action: coordination_end_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
        
        # Send explicit stats for the NEXT stage (Aggregator) to Monitor
        # This allows Monitor to know 'agg_expected'
        if self.filter_type in ["year", "hour"]:
             # Determine next stage name
             next_stage = None
             if self.filter_type == "year":
                 if table_type == TableType.TRANSACTION_ITEMS: next_stage = STAGE_AGG_PRODUCTS
                 elif table_type == TableType.TRANSACTIONS: next_stage = STAGE_AGG_PURCHASES
             elif self.filter_type == "hour":
                 if table_type == TableType.TRANSACTIONS: next_stage = STAGE_AGG_TPV
             
             if next_stage:
                 # We need to send stats for EACH shard we sent data to
                 # self.shard_chunks_sent tracks chunks sent to each shard
                 # We iterate over shards and send stats to Monitor
                 # The 'chunks' field in stats will be the expected count for that shard

                 target_shards = 0
                 if next_stage == STAGE_AGG_PRODUCTS: target_shards = self.products_shards
                 elif next_stage == STAGE_AGG_PURCHASES: target_shards = self.purchases_shards
                 elif next_stage == STAGE_AGG_TPV: target_shards = self.tpv_shards
                 
                 for i in range(1, target_shards + 1):
                     shard_total = self.shard_chunks_sent.get((next_stage, client_id, table_type, i), 0)
                     if self.filter_type == "hour" and table_type == TableType.TRANSACTIONS:
                        logging.info(
                            f"DEBUGGING_QUERY_3 | hour_end_shard_totals | cli_id:{client_id} | shard:{i} | sent_chunks:{shard_total}"
                        )
                     try:
                        logging.info(
                            f"action: sending_expected_for_next_stage | stage:{next_stage} | shard:{i} | cli_id:{client_id} | expected_chunks:{shard_total} | sender_filter:{self.id}"
                        )
                        payload = {
                            "type": MSG_WORKER_STATS,
                            "id": str(self.id),
                            "client_id": client_id,
                            "stage": next_stage, # Target stage
                            "shard": str(i),     # Target shard
                            "expected": shard_total, # Tell Monitor how many chunks to expect for this shard
                            "chunks": shard_total,
                            "processed": 0,      # Explicitly 0 so we don't update agg_processed in Monitor
                            "not_sent": 0,
                            "sender": str(self.id),
                        }
                        rk = f"coordination.stats.{next_stage}.{i}"
                        self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
                        logging.info(f"action: sent_next_stage_expected | stage:{next_stage} | shard:{i} | expected:{shard_total}")
                     except Exception as e:
                        logging.error(f"action: next_stage_stats_error | stage:{next_stage} | shard:{i} | error:{e}")

        end_msg = FilterStatsEndMessage(self.id, client_id, table_type)
        self.middleware_end_exchange.send(end_msg.encode())
        self.working_state.delete_client_stats_data(end_msg)

    def _end_message_to_send(self, client_id, table_type, total_expected, total_not_sent):
        if self.filter_type != "amount":
            return MessageEnd(client_id, table_type, total_expected - total_not_sent, str(self.id))
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

    def process_chunk(self, chunk: ProcessChunk, force=False):
        """
        Applies the filter to the chunk and returns filtered rows.
        Does NOT send the rows - that's handled by the caller after state commits.
        """
        client_id = chunk.client_id()
        message_id = chunk.message_id()

        if not force and self.working_state.is_processed(message_id):
            logging.info(f"action: duplicate_chunk_ignored | message_id:{message_id}")
            return

        self.working_state.mark_processed(message_id)

        if self.filter_type == "amount" and f"to_merge_data_{client_id}" not in self.middleware_queue_sender:
            self.middleware_queue_sender[f"to_merge_data_{client_id}"] = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

        table_type = chunk.table_type()
        logging.debug(f"action: filter | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_in:{len(chunk.rows)}")
        filtered_rows = [tx for tx in chunk.rows if self.apply(tx)]
        logging.debug(f"action: filter_result | type:{self.filter_type} | cli_id:{chunk.client_id()} | file_type:{chunk.table_type()} | rows_out:{len(filtered_rows)}")
        return filtered_rows


    def _should_skip_queue(self, table_type: TableType, queue_name: str, client_id: int) -> bool:
        # Legacy skip logic is superseded by explicit routing
        if self.filter_type == "amount" and queue_name != f"to_merge_data_{client_id}":
            return True
        return False

    def _next_shard(self, dest: str, client_id: int, table_type: TableType, shard_count: int) -> int:
        """
        Compute deterministic shard id per destination/client/table using chunk index.
        """
        key = (dest, client_id, table_type)
        idx = self.chunk_counters[key]
        shard_id = (idx % shard_count) + 1
        self.chunk_counters[key] += 1
        return shard_id

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
