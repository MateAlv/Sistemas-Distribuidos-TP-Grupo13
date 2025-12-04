import datetime
import time
import logging
from collections import defaultdict, deque
from types import SimpleNamespace
import sys
import os
import json

from utils.processing.process_table import (
    TransactionItemsProcessRow,
    TransactionsProcessRow,
    PurchasesPerUserStoreRow,
    TPVProcessRow,
    YearHalf,
)
from utils.processing.process_chunk import ProcessChunk
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.eof_protocol.end_messages import MessageEnd
from utils.protocol import (
    COORDINATION_EXCHANGE,
    MSG_WORKER_END,
    MSG_WORKER_STATS,
    MSG_BARRIER_FORWARD,
    DEFAULT_SHARD,
    STAGE_AGG_PRODUCTS,
    STAGE_AGG_TPV,
    STAGE_AGG_PURCHASES,
)
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    TIMEOUT,
)
from workers.common.sharding import (
    ShardConfig,
    ShardingConfigError,
    build_id_lookup,
    load_shards_from_env,
)
from .aggregator_stats_messages import (
    AggregatorStatsMessage,
    AggregatorStatsEndMessage,
    AggregatorDataMessage,
)
import pickle
import uuid
from utils.tolerance.persistence_service import PersistenceService
from utils.tolerance.crash_helper import crash_after_two_chunks, crash_after_end_processed
from .aggregator_working_state import AggregatorWorkingState


def default_product_value():
    return {"quantity": 0, "subtotal": 0.0}

def default_purchase_value():
    return defaultdict(int)

class Aggregator:
    def __init__(self, agg_type: str, agg_id: int = 1, monitor=None):
        logging.getLogger("pika").setLevel(logging.CRITICAL)
        self.monitor = monitor

        self._running = True

        self.aggregator_type = agg_type
        self.aggregator_id = agg_id
        self.middleware_queue_sender = {}
        self.barrier_forwarded = set()

        # Estado distribuido encapsulado
        self.working_state = AggregatorWorkingState()

        # Exchanges para coordinación
        # [REMOVED] Peer stats exchange
        # self.middleware_stats_exchange = ...
        
        self.middleware_data_exchange = MessageMiddlewareExchange(
            "rabbitmq",
            f"data_exchange_aggregator_{self.aggregator_type}",
            f"{self.aggregator_type}_{self.aggregator_id}_data",
            "fanout",
        )
        try:
            self.middleware_data_exchange.purge()
        except MessageMiddlewareMessageError as purge_error:
            logging.warning(
                f"action: purge_exchange_warning | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{purge_error}"
            )

        self.shard_configs: list[ShardConfig] = []
        self.id_to_shard: dict[int, ShardConfig] = {}
        self.shard_id = os.getenv("AGGREGATOR_SHARD_ID", None)
        self.shard_count = int(os.getenv("AGGREGATOR_SHARDS", "1"))

        # Initialize persistence with chunk buffering
        # Support PERSISTENCE_DIR for testing (defaults to /data/persistence for production)
        persistence_dir = os.getenv("PERSISTENCE_DIR", "/data/persistence")
        commit_interval = int(os.getenv("AGGREGATOR_COMMIT_INTERVAL", "10"))
        self.persistence = PersistenceService(
            f"{persistence_dir}/aggregator_{self.aggregator_type}_{self.aggregator_id}",
            commit_interval=commit_interval
        )
        self._recover_state()



        if self.aggregator_type == "PRODUCTS":
            self.stage = STAGE_AGG_PRODUCTS
            try:
                self.shard_configs = load_shards_from_env("MAX_SHARDS", worker_kind="MAX")
            except ShardingConfigError as exc:
                raise ShardingConfigError(
                    f"Invalid MAX shards configuration: {exc}"
                ) from exc
            self.id_to_shard = build_id_lookup(self.shard_configs)
            # If shard_id is numeric index, map to shard_configs order; otherwise treat as name
            shard_to_use = None
            if self.shard_id is None:
                raise ShardingConfigError("AGGREGATOR_SHARD_ID must be set for PRODUCTS aggregator")
            try:
                shard_idx = int(self.shard_id) - 1
                shard_to_use = self.shard_configs[shard_idx]
            except (ValueError, IndexError):
                shard_to_use = shard_by_id(self.shard_configs, self.shard_id)
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_agg_products_shard_{self.aggregator_id}")
            # Send aggregated partials to the absolute maximizer
            self.middleware_queue_sender["to_absolute_max"] = MessageMiddlewareQueue("rabbitmq", "to_absolute_max")

        elif self.aggregator_type == "PURCHASES":
            self.stage = STAGE_AGG_PURCHASES
            try:
                self.shard_configs = load_shards_from_env("TOP3_SHARDS", worker_kind="TOP3")
            except ShardingConfigError as exc:
                raise ShardingConfigError(
                    f"Invalid TOP3 shards configuration: {exc}"
                ) from exc
            self.id_to_shard = build_id_lookup(self.shard_configs)
            shard_to_use = None
            if self.shard_id is None:
                raise ShardingConfigError("AGGREGATOR_SHARD_ID must be set for PURCHASES aggregator")
            try:
                shard_idx = int(self.shard_id) - 1
                shard_to_use = self.shard_configs[shard_idx]
            except (ValueError, IndexError):
                shard_to_use = shard_by_id(self.shard_configs, self.shard_id)
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_agg_purchases_shard_{self.aggregator_id}")
            # Send aggregated partials to the absolute TOP3 maximizer
            self.middleware_queue_sender["to_top3_absolute"] = MessageMiddlewareQueue("rabbitmq", "to_top3_absolute")

        elif self.aggregator_type == "TPV":
            if self.shard_id is None:
                raise ShardingConfigError("AGGREGATOR_SHARD_ID must be set for TPV aggregator")
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", f"to_agg_tpv_shard_{self.shard_id}")
            # Send aggregated partials to TPV absolute (to be consumed by max/joiner)
            self.middleware_queue_sender["to_absolute_tpv_max"] = MessageMiddlewareQueue("rabbitmq", "to_absolute_tpv_max")
            self.stage = STAGE_AGG_TPV
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")

        # Coordination publisher with shard-aware routing (agg is non-sharded -> global)
        # Wait, Aggregators ARE sharded now.
        # Routing key for barrier: coordination.barrier.<stage>.<shard>
        self.middleware_coordination = MessageMiddlewareExchange(
            "rabbitmq",
            COORDINATION_EXCHANGE,
            f"aggregator_{self.aggregator_type}_{self.aggregator_id}",
            "topic",
            routing_keys=[f"coordination.barrier.{self.stage}.{self.shard_id or DEFAULT_SHARD}"],
        )


    def shutdown(self, signum=None, frame=None):
        logging.info(
            f"SIGTERM recibido: cerrando aggregator {self.aggregator_type} (ID: {self.aggregator_id})"
        )

        for timer in [self.chunk_timer, self.data_timer, self.stats_timer]:
            try:
                if timer is not None:
                    timer.cancel()
            except (OSError, RuntimeError, AttributeError):
                pass

        self._running = False

        # Detener consumos activos
        for middleware in [
            getattr(self, "middleware_queue_receiver", None),
            # getattr(self, "middleware_stats_exchange", None), # Removed
            getattr(self, "middleware_data_exchange", None),
        ]:
            if middleware is None:
                continue
            try:
                middleware.stop_consuming()
            except (OSError, RuntimeError, AttributeError, ValueError):
                pass

        for sender in self.middleware_queue_sender.values():
            try:
                sender.stop_consuming()
            except (OSError, RuntimeError, AttributeError, ValueError):
                pass

        # Cerrar conexiones
        for middleware in [
            getattr(self, "middleware_queue_receiver", None),
            # getattr(self, "middleware_stats_exchange", None), # Removed
            getattr(self, "middleware_data_exchange", None),
        ]:
            if middleware is None:
                continue
            try:
                middleware.close()
            except (OSError, RuntimeError, AttributeError, ValueError):
                pass

        for sender in self.middleware_queue_sender.values():
            try:
                sender.close()
            except (OSError, RuntimeError, AttributeError, ValueError):
                pass

        try:
            self.middleware_coordination.close()
        except (OSError, RuntimeError, AttributeError, ValueError):
            pass

        # Limpiar estructuras internas
        # Limpiar estado
        try:
            # Re-initialize state instead of clearing individual dicts
            self.working_state = AggregatorWorkingState()
        except (OSError, RuntimeError, AttributeError, ValueError):
            pass

        logging.info(
            f"Aggregator {self.aggregator_type} (ID: {self.aggregator_id}) cerrado correctamente."
        )

    def handle_processing_recovery(self):
        """
        Recover processing state following the pattern:
        1. Restore estado (working_state) from disk
        2. Recover buffered chunks
        3. Process buffered chunks and update estado
        4. Commit working_state
        5. Check for pending END/Results and resend idempotently
        """
        logging.info(f"Starting processing recovery | type:{self.aggregator_type} | agg_id:{self.aggregator_id}")
        
        # 1. Estado already recovered in _recover_state() during __init__
        # estado = estado.from_bytes(service.recover_working_state())
        
        # 2. [x] = service.recover_buffer_chunk() - busco último valor x commiteado en disco
        buffered_chunks = self.persistence.recover_buffered_chunks()
        
        if buffered_chunks:
            logging.info(f"Recovering {len(buffered_chunks)} buffered chunks...")
            for chunk in buffered_chunks:
                try:
                    # 3. apply(x) - Procesar + actualizar estado(x)
                    self._apply_and_update_state(chunk)
                except Exception as e:
                    logging.error(f"Error recovering buffered chunk: {e}")
            
            # 4. commit_working_state(self, estado.to_bytes, x.message_id())
            if buffered_chunks:
                last_chunk = buffered_chunks[-1]
                self._save_state(last_chunk.message_id())
                logging.info(f"Committed working state after recovery | chunks_recovered:{len(buffered_chunks)}")
        else:
            logging.info("No buffered chunks to recover")
        
        # 5. Check for pending END messages and resend results/END idempotently
        # If we received END but crashed before/during sending results/END, resend them
        # Deterministic IDs ensure idempotency
        try:
            for client_id in list(self.working_state.chunks_to_receive.keys()):
                for table_type in list(self.working_state.chunks_to_receive[client_id].keys()):
                    if self.working_state.is_end_message_received(client_id, table_type):
                        logging.info(
                            f"action: recovery_resend_pending_end | client_id:{client_id} | table:{table_type}"
                        )
                        # Resend results - deterministic IDs prevent duplicates downstream
                        self.publish_final_results(client_id, table_type)
                        # Resend END - receiver should handle duplicates
                        self._send_end_message(client_id, table_type)
        except Exception as e:
            logging.error(f"Error during pending END recovery: {e}", exc_info=True)

    def run(self):
        logging.info(
            f"Agregador iniciado. Tipo: {self.aggregator_type}, ID: {self.aggregator_id}"
        )

        data_results = deque()
        # stats_results = deque() # Removed
        data_chunks = deque()
        coord_results = deque()

        def data_callback(msg):
            data_results.append(msg)

        # def stats_callback(msg):
        #     stats_results.append(msg)

        def chunk_callback(msg):
            """
            Callback pattern:
            1. Pop de cola → x (done by RabbitMQ)
            2. [CRASH POINT] SE ROMPE ANTES DEL COMMIT
            3. append_chunk_to_buffer(x)
            4. result.append(x)
            5. [CRASH POINT] SE ROMPE ANTES DEL ACK
            6. ACK a cola - RabbitMQ (handled automatically by pika)
            7. [CRASH POINT] SE ROMPE DESPUÉS DEL COMMIT (before processing)
            """
            # 2. [CRASH POINT] SE ROMPE ANTES DEL COMMIT
            
            if msg.startswith(b"END;"):
                # END messages: persist immediately (no buffering)
                end_message = MessageEnd.decode(msg)
                client_id = end_message.client_id()
                table_type = end_message.table_type()
                total_expected = end_message.total_chunks()
                self.working_state.mark_end_message_received(client_id, table_type)
                self.working_state.set_chunks_to_receive(client_id, table_type, total_expected)
                self._save_state(uuid.uuid4())
            else:
                # 3. append_chunk_to_buffer(x)
                chunk = ProcessBatchReader.from_bytes(msg)
                self.persistence.append_chunk_to_buffer(chunk)
            
            # 4. result.append(x)
            data_chunks.append(msg)
            
            # 5. [CRASH POINT] SE ROMPE ANTES DEL ACK
            # 6. ACK a cola - RabbitMQ (automatic after callback returns)
            # 7. [CRASH POINT] SE ROMPE DESPUÉS DEL COMMIT
            
        def coord_callback(msg):
            coord_results.append(msg)

        def data_stop():
            self.middleware_data_exchange.stop_consuming()

        # def stats_stop():
        #     self.middleware_stats_exchange.stop_consuming()

        def chunk_stop():
            self.middleware_queue_receiver.stop_consuming()
        def coord_stop():
            self.middleware_coordination.stop_consuming()


        # Recovery: handle_processing_recovery()
        self.handle_processing_recovery()

        while self._running:
            try:
                self.chunk_timer = self.middleware_queue_receiver.connection.call_later(TIMEOUT, chunk_stop)
                self.middleware_queue_receiver.start_consuming(chunk_callback)
            except Exception as e:
                logging.error(
                    f"action: data_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            try:
                self.data_timer = self.middleware_data_exchange.connection.call_later(TIMEOUT, data_stop)
                self.middleware_data_exchange.start_consuming(data_callback)
            except Exception as e:
                logging.error(
                    f"action: data_exchange_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            # try:
            #     self.stats_timer = self.middleware_stats_exchange.connection.call_later(TIMEOUT, stats_stop)
            #     self.middleware_stats_exchange.start_consuming(stats_callback)
            # except Exception as e:
            #     logging.error(
            #         f"action: stats_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
            #     )

            try:
                coord_timer = self.middleware_coordination.connection.call_later(TIMEOUT, coord_stop)
                self.middleware_coordination.start_consuming(coord_callback)
            except Exception as e:
                logging.error(
                    f"action: coordination_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            while data_results:
    
                raw_data = data_results.popleft()
                self._process_data_message(raw_data)

            # while stats_results:
            #    
            #     raw_stats = stats_results.popleft()
            #     self._process_stats_message(raw_stats)

            while coord_results:
                raw_coord = coord_results.popleft()
                self._process_coord_message(raw_coord)

            while data_chunks:
    
                msg = data_chunks.popleft()
                try:
                    logging.info(f"DEBUG: Aggregator received message: {msg[:50]}...")
                    if msg.startswith(b"END;"):
                        self._handle_end_message(msg)
                    else:
                        self._handle_data_chunk(msg)
                except Exception as e:
                    logging.error(
                        f"action: exception_in_main_processing | type:{self.aggregator_type} | error:{e}"
                    )

    def _process_data_message(self, raw_msg: bytes):
        try:
            data = AggregatorDataMessage.decode(raw_msg)
        except Exception as e:
            logging.error(f"action: error_decoding_data_message | error:{e}")
            return

        if data.aggregator_id == self.aggregator_id:
            return
        if data.aggregator_type != self.aggregator_type:
            logging.warning(
                f"action: aggregator_data_type_mismatch | expected:{self.aggregator_type} | received:{data.aggregator_type}"
            )
            return

        logging.debug(
            f"action: aggregator_data_received | type:{self.aggregator_type} | from_agg:{data.aggregator_id} "
            f"| client_id:{data.client_id} | table_type:{data.table_type}"
        )

        if self.working_state.is_processed(data.message_id):
            logging.info(f"action: duplicate_data_message_ignored | message_id:{data.message_id}")
            return

        self._apply_remote_aggregation(data)
        self.working_state.increment_accumulated_chunks(data.client_id, data.table_type, data.aggregator_id)

        self.working_state.mark_processed(data.message_id)
        self._save_state(data.message_id)

        # [NEW] Send stats to Monitor after processing remote data
        self._send_stats_to_monitor(data.client_id, data.table_type)

        # Removed: self._can_send_end_message check (now driven by Monitor barrier)

    # Removed _process_stats_message (peer stats)

    def _process_coord_message(self, raw_msg: bytes):
        try:
                data = json.loads(raw_msg)
                if data.get("type") != MSG_BARRIER_FORWARD:
                    return
                stage = data.get("stage")
                if stage != self.stage:
                    return
                shard = data.get("shard", DEFAULT_SHARD)
                # Ignore barrier forwards for other shards
                if str(shard) != str(self.shard_id or DEFAULT_SHARD):
                    return
                client_id = data.get("client_id")
                key = (client_id, stage, shard)
                
                # CRITICAL: Check and mark as processed BEFORE sending
                # This prevents race condition where multiple barriers publish partial data
                if key in self.barrier_forwarded:
                    logging.info(f"action: barrier_already_processed | key:{key}")
                    return
                
                # Mark IMMEDIATELY to prevent concurrent barrier forwards
                self.barrier_forwarded.add(key)
                
                total_chunks = data.get("total_chunks", 0)
                # [NEW] Barrier Forward Handling
                # When Monitor says "Barrier Reached", we flush results and send END.
                
                # Map stage to table_type
                if self.aggregator_type == "PRODUCTS":
                    table_type = TableType.TRANSACTION_ITEMS
                elif self.aggregator_type in ["PURCHASES", "TPV"]:
                    table_type = TableType.TRANSACTIONS
                else:
                    return
                
                logging.info(f"action: barrier_forward_received | type:{self.aggregator_type} | cli_id:{client_id} | shard:{shard} | total_chunks:{total_chunks}")
                
                # Flush results and send END
                # Safe to call now - barrier marked as processed above
                self._send_end_message(client_id, table_type)
                
        except Exception as e:
            logging.error(f"action: barrier_forward_error | type:{self.aggregator_type} | error:{e}", exc_info=True)

    def _handle_end_message(self, raw_msg: bytes):
        try:
            end_message = MessageEnd.decode(raw_msg)
        except Exception as e:
            logging.error(f"action: error_processing_end_message | type:{self.aggregator_type} | error:{e}")
            return

        client_id = end_message.client_id()
        table_type = end_message.table_type()
        total_expected = end_message.total_chunks()

        logging.info(
            f"action: end_message_received | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type} | total_chunks_expected:{total_expected}"
        )

        # IMPORTANT: Do NOT process buffer here!
        # Chunks may still arrive AFTER this END message due to network delays.
        # Buffer will be processed when BARRIER arrives (in _send_end_message).
        
        # Mark END received and persist (FT improvement)
        self.working_state.mark_end_message_received(client_id, table_type)
        self.working_state.set_chunks_to_receive(client_id, table_type, total_expected)
        self._save_state(uuid.uuid4())

        # Send stats to Monitor
        # Note: Stats may be incomplete if chunks still arriving, but that's OK
        # Monitor will wait for all aggregators to report before triggering barrier
        self._send_stats_to_monitor(client_id, table_type)
        # Announce END to Monitor - Monitor will coordinate barrier and trigger publish
        self._send_end_to_monitor(client_id, table_type, total_expected)

        # REMOVED: Buffer processing moved to _send_end_message (barrier handler)
        # This allows late-arriving chunks to be processed before publication

    def _apply_and_update_state(self, chunk: ProcessChunk):
        """
        Apply aggregation and update working state for a chunk.
        This is the core processing logic separated for clarity.
        
        Steps:
        1. Validate table_type (prevent state corruption)
        2. Check idempotency
        3. apply(x) - Process/aggregate the chunk
        4. actualizar estado(x) - Update working state counters
        """
        client_id = chunk.client_id()
        table_type = chunk.table_type()
        
        # CRITICAL: Validate table_type BEFORE marking as processed
        # Invalid chunks should not alter state or be marked as processed
        if not self._is_valid_table_type(table_type):
            logging.warning(
                f"action: invalid_table_type_ignored | agg_type:{self.aggregator_type} | table_type:{table_type}"
            )
            # Clear processing commit without marking as processed
            try:
                self.persistence.clear_processing_commit()
            except Exception:
                pass
            return  # Exit early - do NOT process or mark as processed
        
        # Check idempotency
        if self.working_state.is_processed(chunk.message_id()):
            logging.info(f"action: duplicate_chunk_ignored | message_id:{chunk.message_id()}")
            try:
                self.persistence.clear_processing_commit()
            except Exception:
                pass
            return
        
        logging.info(
            f"action: aggregate | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type} | rows_in:{len(chunk.rows)}"
        )

        # Update state: increment chunks received
        self.working_state.increment_chunks_received(
            client_id,
            table_type,
            self.aggregator_id,
            1,
        )

        # 1. apply(x) - Procesar
        has_output = False
        payload = None

        if self.aggregator_type == "PRODUCTS":
            aggregated_rows = self.apply_products(chunk)
            if aggregated_rows:
                self.accumulate_products(client_id, aggregated_rows)
                payload = self._build_products_payload(aggregated_rows)
                has_output = True
        elif self.aggregator_type == "PURCHASES":
            aggregated_chunk = self.apply_purchases(chunk)
            if aggregated_chunk:
                self.accumulate_purchases(client_id, aggregated_chunk)
                payload = self._build_purchases_payload(aggregated_chunk)
                has_output = True
        elif self.aggregator_type == "TPV":
            aggregated_chunk = self.apply_tpv(chunk)
            if aggregated_chunk:
                self.accumulate_tpv(client_id, aggregated_chunk)
                payload = self._build_tpv_payload(aggregated_chunk)
                has_output = True

        if not has_output:
            logging.info(
                f"action: aggregate_no_output | type:{self.aggregator_type} | cli_id:{client_id} "
                f"| file_type:{table_type} | rows_in:{len(chunk.rows)}"
            )

        # 2. actualizar estado(x)
        self.working_state.increment_chunks_processed(
            client_id,
            table_type,
            self.aggregator_id,
            1,
        )
        self.working_state.increment_accumulated_chunks(client_id, table_type, self.aggregator_id)
        
        # Send payload to peers (fanout exchange)
        if payload:
            try:
                data_msg = AggregatorDataMessage(
                    self.aggregator_type,
                    self.aggregator_id,
                    client_id,
                    table_type,
                    payload,
                )
                logging.info(
                    f"DEBUG: aggregator_data_sent | type:{self.aggregator_type} | agg_id:{self.aggregator_id} "
                    f"| client_id:{client_id} | table_type:{table_type} | payload_size:{len(payload)}"
                )
                
                # CRITICAL: TPV does NOT use fanout - only sends final results after barrier
                # PURCHASES uses fanout for peer-to-peer aggregation
                # PRODUCTS does not use fanout
                if self.aggregator_type == "PURCHASES":
                    self.middleware_data_exchange.send(data_msg.encode())
                    # Only commit send_ack when actually sending to peers
                    self.persistence.commit_send_ack(client_id, chunk.message_id())
                elif self.aggregator_type == "PRODUCTS":
                    # PRODUCTS: no fanout, only sends final results
                    logging.debug("action: skip_fanout_products")
                elif self.aggregator_type == "TPV":
                    # TPV: no fanout, only accumulates locally and sends final results after barrier
                    logging.debug("DEBUGGING_QUERY_3 | skip_fanout_tpv_peers")
                    logging.info(
                        f"DEBUGGING_QUERY_3 | agg_tpv_payload_accumulated | cli_id:{client_id} | rows:{len(aggregated_chunk.rows)} | accumulated_keys:{len(self.working_state.get_tpv_accumulator(client_id))}"
                    )
            except Exception as e:
                logging.error(f"action: error_sending_data_message | error:{e}")

        self.working_state.mark_processed(chunk.message_id())
        
        # Track message_id to client_id mapping for pruning
        if not hasattr(self.working_state, 'message_id_to_client'):
            self.working_state.message_id_to_client = {}
        self.working_state.message_id_to_client[chunk.message_id()] = client_id

    def _handle_data_chunk(self, raw_msg: bytes):
        """
        Handle incoming data chunk following the pattern:
        1. apply(x) - Process (via _apply_and_update_state)
        2. actualizar estado(x) - Update state (via _apply_and_update_state)
        3. if can_commit_working_state:
            - [CRASH POINT] SE ROMPE ANTES DEL COMMIT DE WS
            - commit_working_state(estado.to_bytes, x.message_id())
            - clean buffer (done automatically in commit_working_state)
        4. _send_end_message() if END received
        5. [CRASH POINT] SE ROMPE ANTES DE ENVIAR
        6. Mando a cola
        7. commit_send_ack(x.client_id(), x.message_id())
        8. [CRASH POINT] SE ROMPE DESPUÉS DE ENVIAR
        """
        logging.info(f"DEBUG: _handle_data_chunk called | len:{len(raw_msg)}")
        self._check_crash_point("CRASH_BEFORE_PROCESS")
        
        chunk = ProcessBatchReader.from_bytes(raw_msg)
        client_id = chunk.client_id()
        table_type = chunk.table_type()

        crash_after_two_chunks("aggregator")
        # 1 & 2. apply(x) + actualizar estado(x)
        self._apply_and_update_state(chunk)

        self._check_crash_point("CRASH_AFTER_PROCESS_BEFORE_COMMIT")

        # 3. if can_commit_working_state:
        if self.persistence.should_commit_state():
            # [CRASH POINT] SE ROMPE ANTES DEL COMMIT DE WS
            self._save_state(chunk.message_id())
            # clean buffer (done automatically in _save_state → commit_working_state)
            logging.info(f"Periodic state commit | chunks_processed:{self.persistence.chunks_since_last_commit}")

        # 4. Check if END message was received and we should send results
        if self.working_state.is_end_message_received(client_id, table_type):
            self._send_stats_to_monitor(client_id, table_type)

    def _recover_state(self):
        state_data = self.persistence.recover_working_state()
        if state_data:
            try:
                self.working_state = AggregatorWorkingState.from_bytes(state_data)
                logging.info(f"State recovered for aggregator {self.aggregator_type}_{self.aggregator_id}")
            except Exception as e:
                logging.error(f"Error recovering state: {e}")

    def _save_state(self, last_processed_id):
        self.persistence.commit_working_state(self.working_state.to_bytes(), last_processed_id)


    def _send_stats_to_monitor(self, client_id, table_type):
        processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
        try:
            payload = {
                "type": MSG_WORKER_STATS,
                "id": str(self.aggregator_id),
                "client_id": client_id,
                "stage": self.stage,
                "shard": self.shard_id or DEFAULT_SHARD,
                "processed": processed,
                "sender": str(self.aggregator_id),
            }
            rk = f"coordination.stats.{self.stage}.{self.shard_id or DEFAULT_SHARD}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            logging.info(f"DEBUG: stats_sent_to_monitor | stage:{self.stage} | cli_id:{client_id} | processed:{processed}")
        except Exception as e:
            logging.error(f"action: stats_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")

    def _send_end_to_monitor(self, client_id, table_type, expected_from_upstream):
        """Publish END to Monitor for barrier tracking."""
        try:
            processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
            payload = {
                "type": MSG_WORKER_END,
                "id": str(self.aggregator_id),
                "client_id": client_id,
                "stage": self.stage,
                "shard": self.shard_id or DEFAULT_SHARD,
                "expected": expected_from_upstream,
                "chunks": processed,
                "sender": str(self.aggregator_id),
            }
            rk = f"coordination.barrier.{self.stage}.{self.shard_id or DEFAULT_SHARD}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            logging.info(
                f"DEBUG: end_sent_to_monitor | stage:{self.stage} | cli_id:{client_id} | expected:{expected_from_upstream} | processed:{processed}"
            )
        except Exception as e:
            logging.error(f"action: end_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
            

    def _send_end_message(self, client_id, table_type):
        total_processed = self.working_state.get_total_processed(client_id, table_type)
        logging.info(
            f"action: sending_end_message | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type.name} | total_chunks:{total_processed} | expected_chunks:{self.working_state.get_chunks_to_receive(client_id, table_type)}"
        )

        logging.debug(
            f"action: publish_final_results_trigger | type:{self.aggregator_type}"
        ) # Removed: self._can_send_end_message check
        
        # CRITICAL: Process buffered chunks BEFORE publishing
        # This catches ALL chunks including those that arrived after END message
        # Chunks can arrive out-of-order due to network delays
        buffered_chunks = self.persistence.recover_buffered_chunks()
        if buffered_chunks:
            logging.info(f"action: processing_buffered_chunks_on_barrier | client_id:{client_id} | count:{len(buffered_chunks)}")
            for chunk in buffered_chunks:
                try:
                    self._apply_and_update_state(chunk)
                except Exception as e:
                    logging.error(f"Error processing buffered chunk on barrier: {e}")
            # Save state after processing buffer
            if buffered_chunks:
                self._save_state(buffered_chunks[-1].message_id())
                # Clear buffer after processing
                try:
                    self.persistence._clear_chunk_buffer()
                    self.persistence.chunks_since_last_commit = 0
                except Exception as e:
                    logging.error(f"Error clearing buffer after barrier processing: {e}")

        self.publish_final_results(client_id, table_type)

        # Send END message with OUR ID
        try:
            send_table_type = table_type
            if self.aggregator_type == "TPV":
                send_table_type = TableType.TPV
            elif self.aggregator_type == "PURCHASES":
                send_table_type = TableType.PURCHASES_PER_USER_STORE

            my_processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
            end_msg = MessageEnd(client_id, send_table_type, my_processed, str(self.aggregator_id))
            
            for queue in self.middleware_queue_sender.values():
                queue.send(end_msg.encode())
            logging.info(
                f"action: sent_end_to_next_stage | type:{self.aggregator_type} | cli_id:{client_id} | table:{send_table_type} | my_processed:{my_processed} | expected_global:{self.working_state.get_chunks_to_receive(client_id, table_type)}"
            )
            # Publish coordination END
            try:
                payload = {
                    "type": MSG_WORKER_END,
                    "id": str(self.aggregator_id),
                    "client_id": client_id,
                    "stage": self.stage,
                    "shard": self.shard_id or DEFAULT_SHARD,
                    "expected": self.working_state.get_chunks_to_receive(client_id, table_type),
                    "chunks": my_processed,
                    "sender": str(self.aggregator_id),
                }
                rk = f"coordination.barrier.{self.stage}.{self.shard_id or DEFAULT_SHARD}"
                self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
                logging.info(f"DEBUG: coordination_end_sent | stage:{self.stage} | cli_id:{client_id} | chunks:{my_processed}")
            except Exception as e:
                logging.error(f"action: coordination_end_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
        except Exception as e:
            logging.error(f"action: error_sending_end_message | error:{e}")

        self.delete_client_data(client_id, table_type)
        crash_after_end_processed("aggregator")
        
    def delete_client_data(self, client_id, table_type):
        """
        Delete all accumulated data for a specific client.
        Persists the cleanup to prevent zombie data after crash.
        """
        logging.info(f"action: delete_client_data | client_id:{client_id} | table_type:{table_type}")
        
        # Clear global accumulator
        if client_id in self.working_state.global_accumulator:
            del self.working_state.global_accumulator[client_id]
        
        # Clear chunks tracking
        if client_id in self.working_state.chunks_to_receive:
            del self.working_state.chunks_to_receive[client_id]
        
        # Clear chunks_received_per_client tracking
        if hasattr(self.working_state, 'chunks_received_per_client') and client_id in self.working_state.chunks_received_per_client:
            del self.working_state.chunks_received_per_client[client_id]
        
        # Clear chunks_processed tracking
        if hasattr(self.working_state, 'chunks_processed') and client_id in self.working_state.chunks_processed:
            del self.working_state.chunks_processed[client_id]
        
        # Clear accumulated_chunks tracking
        if hasattr(self.working_state, 'accumulated_chunks') and client_id in self.working_state.accumulated_chunks:
            del self.working_state.accumulated_chunks[client_id]
        
        # Prune processed_ids for this client (memory management)
        # Remove message_ids that belong to this client's chunks
        if hasattr(self.working_state, 'processed_ids') and hasattr(self.working_state, 'message_id_to_client'):
            # Find and remove all message_ids for this client
            ids_to_remove = [msg_id for msg_id, cid in self.working_state.message_id_to_client.items() if cid == client_id]
            for msg_id in ids_to_remove:
                self.working_state.processed_ids.discard(msg_id)
                del self.working_state.message_id_to_client[msg_id]
            logging.info(f"action: processed_ids_pruned | client_id:{client_id} | pruned_count:{len(ids_to_remove)}")
        
        # Persist the cleanup (prevent zombie data after crash)
        try:
            self._save_state(uuid.uuid4())
            logging.info(f"action: delete_client_data_persisted | client_id:{client_id}")
        except Exception as e:
            logging.error(f"Error persisting delete_client_data: {e}")
    
    def _is_valid_table_type(self, table_type):
        """
        Validate that the table_type is valid for this aggregator.
        Prevents processing chunks with wrong table types that could corrupt state.
        """
        valid_types = {
            "PRODUCTS": [TableType.TRANSACTION_ITEMS],
            "PURCHASES": [TableType.PURCHASES_PER_USER_STORE, TableType.TRANSACTIONS],
            "TPV": [TableType.TRANSACTIONS, TableType.TPV]
        }
        
        expected = valid_types.get(self.aggregator_type, [])
        return table_type in expected

    def _check_crash_point(self, point_name):
        if os.environ.get("CRASH_POINT") == point_name:
            logging.critical(f"Simulating crash at {point_name}")
            sys.exit(1)

    def _accumulator_key(self):
        if self.aggregator_type == "PRODUCTS":
            return "products"
        if self.aggregator_type == "PURCHASES":
            return "purchases"
        if self.aggregator_type == "TPV":
            return "tpv"
        return "unknown"

    def accumulate_products(self, client_id, rows):
        data = self.working_state.get_product_accumulator(client_id)

        for row in rows:
            created_at = getattr(row, "created_at", None)
            if not created_at or not hasattr(created_at, "date"):
                logging.warning(
                    f"action: accumulate_products_invalid_date | client_id:{client_id} | row_item:{getattr(row, 'item_id', 'unknown')}"
                )
                continue
            year = created_at.date.year
            month = created_at.date.month
            key = (int(row.item_id), year, month)
            data[key]["quantity"] += int(row.quantity)
            data[key]["subtotal"] += float(row.subtotal)

    def accumulate_purchases(self, client_id, aggregated_chunk: ProcessChunk):
        data = self.working_state.get_purchase_accumulator(client_id)

        for row in aggregated_chunk.rows:
            store_id = int(row.store_id)
            user_id = int(row.user_id)
            count = int(row.purchases_made)
            data[store_id][user_id] += count

    def accumulate_tpv(self, client_id, aggregated_chunk: ProcessChunk):
        data = self.working_state.get_tpv_accumulator(client_id)

        for row in aggregated_chunk.rows:
            if isinstance(row, TPVProcessRow):
                key = (row.year_half.year, row.year_half.half, int(row.store_id))
                data[key] += float(row.tpv)
            else:
                year = row.created_at.year
                semester = 1 if row.created_at.month <= 6 else 2
                key = (year, semester, int(row.store_id))
                data[key] += float(row.final_amount)

    def _build_products_payload(self, rows):
        return {
            "products": [
                [
                    int(row.item_id),
                    int(row.created_at.date.year),
                    int(row.created_at.date.month),
                    int(row.quantity),
                    float(row.subtotal),
                ]
                for row in rows
            ]
        }

    def _build_purchases_payload(self, aggregated_chunk):
        return {
            "purchases": [
                [int(row.store_id), int(row.user_id), int(row.purchases_made)]
                for row in aggregated_chunk.rows
            ]
        }

    def _build_tpv_payload(self, aggregated_chunk: ProcessChunk):
        payload = []
        for row in aggregated_chunk.rows:
            if isinstance(row, TPVProcessRow):
                payload.append(
                    [int(row.year_half.year), int(row.year_half.half), int(row.store_id), float(row.tpv)]
                )
            else:
                semester = 1 if row.created_at.month <= 6 else 2
                payload.append(
                    [int(row.created_at.year), semester, int(row.store_id), float(row.final_amount)]
                )
        return {"tpv": payload}

    def _apply_remote_aggregation(self, data_msg: AggregatorDataMessage):
        client_id = data_msg.client_id
        payload = data_msg.payload
        if self.aggregator_type == "TPV":
            # Skip remote fanout for TPV to avoid duplicate accumulation
            logging.debug("DEBUGGING_QUERY_3 | skip_remote_tpv_fanout")
            return

        if self.aggregator_type == "PRODUCTS":
            rows = []
            for item_id, year, month, quantity, subtotal in payload.get("products", []):
                created_at = DateTime(datetime.date(int(year), int(month), 1), datetime.time(0, 0))
                rows.append(
                    TransactionItemsProcessRow(
                        "",
                        int(item_id),
                        int(quantity),
                        float(subtotal),
                        created_at,
                    )
                )
            if rows:
                self.accumulate_products(client_id, rows)

        elif self.aggregator_type == "PURCHASES":
            rows = []
            marker_date = DateTime(datetime.date(2024, 1, 1), datetime.time(0, 0))
            for store_id, user_id, count in payload.get("purchases", []):
                rows.append(
                    PurchasesPerUserStoreRow(
                        int(store_id), "", int(user_id), marker_date.date, int(count)
                    )
                )
            if rows:
                self.accumulate_purchases(client_id, SimpleNamespace(rows=rows))

        elif self.aggregator_type == "TPV":
            rows = []
            for year, semester, store_id, total_tpv in payload.get("tpv", []):
                year_half = YearHalf(int(year), int(semester))
                rows.append(TPVProcessRow(int(store_id), float(total_tpv), year_half))
            if rows:
                self.accumulate_tpv(client_id, SimpleNamespace(rows=rows))

    def publish_final_results(self, client_id, table_type):
        if client_id not in self.working_state.global_accumulator:
            logging.warning(
                f"action: publish_final_results | client_id:{client_id} | warning: no_accumulated_data"
            )
            return

        logging.info(
            f"action: publish_final_results | type:{self.aggregator_type} | client_id:{client_id} | table_type:{table_type}"
        )

        if self.aggregator_type == "PRODUCTS":
            self._publish_final_products(client_id)
        elif self.aggregator_type == "PURCHASES":
            self._publish_final_purchases(client_id)
        elif self.aggregator_type == "TPV":
            self._publish_final_tpv(client_id)
        else:
            logging.warning(
                f"action: publish_final_results_unknown_type | aggregator_type:{self.aggregator_type} | client_id:{client_id}"
            )

    def _publish_final_products(self, client_id):
        data = self.working_state.global_accumulator[client_id].get("products")
        if not data:
            return

        from utils.processing.process_chunk import ProcessChunkHeader

        # Use deterministic ID for idempotent re-sends
        msg_id = self._deterministic_msg_id("agg-products", client_id)
        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS, message_id=msg_id)
        rows = []
        for (item_id, year, month), totals in data.items():
            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
            rows.append(
                TransactionItemsProcessRow(
                    f"agg_shard_{self.shard_id or 'global'}",
                    item_id,
                    totals["quantity"],
                    totals["subtotal"],
                    created_at,
                )
            )

        if not rows:
            return

        chunk = ProcessChunk(header, rows).serialize()
        self.middleware_queue_sender["to_absolute_max"].send(chunk)
        logging.info(
            f"action: publish_final_products | client_id:{client_id} | queue:to_absolute_max | rows:{len(rows)}"
        )

    def _publish_final_purchases(self, client_id):
        data = self.working_state.global_accumulator[client_id].get("purchases")
        if not data:
            return

        from utils.processing.process_chunk import ProcessChunkHeader

        placeholder_date = datetime.date(2024, 1, 1)

        rows = []
        for store_id, users in data.items():
            for user_id, count in users.items():
                rows.append(
                    PurchasesPerUserStoreRow(
                        store_id=store_id,
                        store_name=f"agg_shard_{self.shard_id or 'global'}",
                        user_id=user_id,
                        user_birthdate=placeholder_date,
                        purchases_made=count,
                    )
                )

        if not rows:
            return

        header = ProcessChunkHeader(
            client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE
        )
        chunk = ProcessChunk(header, rows)
        chunk_data = chunk.serialize()
        self.middleware_queue_sender["to_top3_absolute"].send(chunk_data)
        logging.info(
            f"action: publish_final_purchases | client_id:{client_id} | queue:to_top3_absolute | rows:{len(rows)}"
        )

    def _publish_final_tpv(self, client_id):
        data = self.working_state.global_accumulator[client_id].get("tpv")
        if not data:
            return

        from utils.processing.process_chunk import ProcessChunkHeader

        rows = []
        for (year, semester, store_id), total in data.items():
            year_half = YearHalf(year, semester)
            row = TPVProcessRow(store_id=store_id, tpv=total, year_half=year_half, shard_id=str(self.shard_id))
            rows.append(row)

        # Use deterministic ID for idempotent re-sends
        msg_id = self._deterministic_msg_id("agg-tpv", client_id)
        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TPV, message_id=msg_id)
        chunk = ProcessChunk(header, rows)
        # TPV aggregation
        # Send to TPV Maximizer
        queue = self.middleware_queue_sender["to_absolute_tpv_max"]
        logging.info(
            f"DEBUGGING_QUERY_3 | agg_tpv_publish_final | cli_id:{client_id} | rows:{len(rows)} | keys:{len(data)} | queue:{queue.queue_name}"
        )
        queue.send(chunk.serialize())
        logging.info(f"action: sent_tpv_chunk | client_id:{client_id} | rows:{len(chunk.rows)}")

    def _deterministic_msg_id(self, label: str, client_id: int) -> uuid.UUID:
        """Generate deterministic message ID for idempotent re-sends during recovery."""
        shard_component = self.shard_id or DEFAULT_SHARD
        return uuid.uuid5(uuid.NAMESPACE_DNS, f"{label}-{client_id}-{self.aggregator_type}-{self.aggregator_id}-{shard_component}")

    def apply_products(self, chunk):
        YEARS = {2024, 2025}
        chunk_sellings = defaultdict(int)
        chunk_profit = defaultdict(float)

        for row in chunk.rows:
            if (
                hasattr(row, "item_id")
                and hasattr(row, "quantity")
                and hasattr(row, "subtotal")
                and hasattr(row, "created_at")
            ):
                if hasattr(row, "month_year_created_at"):
                    dt = row.month_year_created_at
                    if dt.year in YEARS:
                        key = (row.item_id, dt.year, dt.month)
                        chunk_sellings[key] += row.quantity
                        chunk_profit[key] += row.subtotal

        if not chunk_sellings:
            return None

        rows = []

        for key, total_qty in chunk_sellings.items():
            total_profit = chunk_profit[key]
            item_id, year, month = key
            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))

            new_row = TransactionItemsProcessRow(
                transaction_id="",
                item_id=item_id,
                quantity=total_qty,
                subtotal=total_profit,
                created_at=created_at,
            )


            rows.append(new_row)

        return rows

    def apply_purchases(self, chunk):
        YEARS = {2024, 2025}
        chunk_accumulator = defaultdict(int)

        processed_rows = 0
        valid_years = 0
        parsing_errors = 0

        for row in chunk.rows:
            processed_rows += 1
            if hasattr(row, "store_id") and hasattr(row, "user_id") and hasattr(row, "created_at"):
                if row.store_id is None or row.user_id is None:
                    continue

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
                elif hasattr(created_at, "date"):
                    dt = datetime.datetime.combine(
                        created_at.date,
                        created_at.time if hasattr(created_at, "time") else datetime.time(0, 0),
                    )
                elif hasattr(created_at, "year"):
                    dt = created_at
                else:
                    parsing_errors += 1
                    continue

                if dt.year in YEARS:
                    valid_years += 1
                    key = (int(row.store_id), int(row.user_id))
                    chunk_accumulator[key] += 1

        logging.info(
            f"action: apply_purchases_stats | client_id:{chunk.header.client_id} | processed:{processed_rows} "
            f"| valid_years:{valid_years} | parsing_errors:{parsing_errors} | accumulated_keys:{len(chunk_accumulator)}"
        )

        if not chunk_accumulator:
            logging.warning(
                f"action: apply_purchases_no_output | client_id:{chunk.header.client_id} | processed:{processed_rows} "
                f"| valid_years:{valid_years}"
            )
            return None

        rows = []
        marker_date = DateTime(datetime.date(2024, 1, 1), datetime.time(0, 0))

        for (store_id, user_id), count in chunk_accumulator.items():
            # Use PurchasesPerUserStoreRow so TOP3 maximizer can consume directly
            row = PurchasesPerUserStoreRow(
                store_id=store_id,
                store_name="",
                user_id=user_id,
                user_birthdate=marker_date.date,
                purchases_made=count,
            )
            rows.append(row)

        from utils.processing.process_chunk import ProcessChunkHeader

        # Use deterministic ID for idempotent re-sends
        msg_id = self._deterministic_msg_id("agg-purchases", chunk.header.client_id)
        header = ProcessChunkHeader(
            client_id=chunk.header.client_id, table_type=TableType.PURCHASES_PER_USER_STORE, message_id=msg_id
        )
        return ProcessChunk(header, rows)

    def apply_tpv(self, chunk):
        YEARS = {2024, 2025}
        chunk_accumulator = defaultdict(float)

        processed_rows = 0
        valid_years = 0
        parsing_errors = 0

        for row in chunk.rows:
            processed_rows += 1
            if hasattr(row, "store_id") and hasattr(row, "final_amount") and hasattr(row, "created_at"):
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
                elif hasattr(created_at, "date"):
                    dt = datetime.datetime.combine(
                        created_at.date,
                        created_at.time if hasattr(created_at, "time") else datetime.time(0, 0),
                    )
                elif hasattr(created_at, "year"):
                    dt = created_at
                else:
                    parsing_errors += 1
                    continue

                if dt.year in YEARS:
                    valid_years += 1
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

        logging.info(
            f"action: apply_tpv_stats | client_id:{chunk.header.client_id} | processed:{processed_rows} "
            f"| valid_years:{valid_years} | parsing_errors:{parsing_errors} | accumulated_keys:{len(chunk_accumulator)}"
        )

        if not chunk_accumulator:
            logging.warning(
                f"action: apply_tpv_no_output | client_id:{chunk.header.client_id} | processed:{processed_rows} "
                f"| valid_years:{valid_years}"
            )
            return None

        # Return ProcessChunk (original behavior that worked)
        rows = []
        for (year, semester, store_id), total_tpv in chunk_accumulator.items():
            year_half = YearHalf(year, semester)
            row = TPVProcessRow(store_id=store_id, tpv=total_tpv, year_half=year_half)
            rows.append(row)

        from utils.processing.process_chunk import ProcessChunkHeader

        # Use deterministic ID for idempotent re-sends during recovery
        msg_id = self._deterministic_msg_id("agg-tpv", chunk.header.client_id)
        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TPV, message_id=msg_id)
        return ProcessChunk(header, rows)
