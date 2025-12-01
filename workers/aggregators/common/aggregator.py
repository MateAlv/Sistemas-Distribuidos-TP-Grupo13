import datetime
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
        self.middleware_stats_exchange = MessageMiddlewareExchange(
            "rabbitmq",
            f"end_exchange_aggregator_{self.aggregator_type}",
            f"{self.aggregator_type}_{self.aggregator_id}_stats",
            "fanout",
        )
        self.middleware_data_exchange = MessageMiddlewareExchange(
            "rabbitmq",
            f"data_exchange_aggregator_{self.aggregator_type}",
            f"{self.aggregator_type}_{self.aggregator_id}_data",
            "fanout",
        )
        self.middleware_coordination = MessageMiddlewareExchange(
            "rabbitmq",
            COORDINATION_EXCHANGE,
            [""],
            "topic",
            routing_keys=[f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"],
        )
        try:
            self.middleware_stats_exchange.purge()
            self.middleware_data_exchange.purge()
        except MessageMiddlewareMessageError as purge_error:
            logging.warning(
                f"action: purge_exchange_warning | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{purge_error}"
            )

        self.shard_configs: list[ShardConfig] = []
        self.shard_configs: list[ShardConfig] = []
        self.id_to_shard: dict[int, ShardConfig] = {}

        self.persistence = PersistenceService(f"/data/persistence/aggregator_{self.aggregator_type}_{self.aggregator_id}")
        self.persistence = PersistenceService(f"/data/persistence/aggregator_{self.aggregator_type}_{self.aggregator_id}")
        self._recover_state()



        if self.aggregator_type == "PRODUCTS":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_1+2")
            self.stage = STAGE_AGG_PRODUCTS
            try:
                self.shard_configs = load_shards_from_env("MAX_SHARDS", worker_kind="MAX")
            except ShardingConfigError as exc:
                raise ShardingConfigError(
                    f"Invalid MAX shards configuration: {exc}"
                ) from exc
            self.id_to_shard = build_id_lookup(self.shard_configs)
            for shard in self.shard_configs:
                self.middleware_queue_sender[shard.queue_name] = MessageMiddlewareQueue("rabbitmq", shard.queue_name)

        elif self.aggregator_type == "PURCHASES":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
            self.stage = STAGE_AGG_PURCHASES
            try:
                self.shard_configs = load_shards_from_env("TOP3_SHARDS", worker_kind="TOP3")
            except ShardingConfigError as exc:
                raise ShardingConfigError(
                    f"Invalid TOP3 shards configuration: {exc}"
                ) from exc
            self.id_to_shard = build_id_lookup(self.shard_configs)
            for shard in self.shard_configs:
                self.middleware_queue_sender[shard.queue_name] = MessageMiddlewareQueue("rabbitmq", shard.queue_name)

        elif self.aggregator_type == "TPV":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
            self.middleware_queue_sender["to_join_with_stores_tvp"] = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
            self.stage = STAGE_AGG_TPV
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")


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
            getattr(self, "middleware_stats_exchange", None),
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
            getattr(self, "middleware_stats_exchange", None),
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

    def run(self):
        logging.info(
            f"Agregador iniciado. Tipo: {self.aggregator_type}, ID: {self.aggregator_id}"
        )

        data_results = deque()
        stats_results = deque()
        data_chunks = deque()
        coord_results = deque()

        def data_callback(msg):
            data_results.append(msg)

        def stats_callback(msg):
            stats_results.append(msg)

        def chunk_callback(msg):
            data_chunks.append(msg)
        def coord_callback(msg):
            coord_results.append(msg)

        def data_stop():
            self.middleware_data_exchange.stop_consuming()

        def stats_stop():
            self.middleware_stats_exchange.stop_consuming()

        def chunk_stop():
            self.middleware_queue_receiver.stop_consuming()
        def coord_stop():
            self.middleware_coordination.stop_consuming()

        # Recover last processing chunk if exists
        last_chunk = self.persistence.recover_last_processing_chunk()
        if last_chunk:
            logging.info("Recovering last processing chunk...")
            try:
                # We pass the serialized chunk to _handle_data_chunk
                # But _handle_data_chunk expects bytes that ProcessBatchReader can parse.
                # ProcessChunk.serialize() returns exactly that.
                self._handle_data_chunk(last_chunk.serialize())
            except Exception as e:
                logging.error(f"Error recovering last chunk: {e}")

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

            try:
                self.stats_timer = self.middleware_stats_exchange.connection.call_later(TIMEOUT, stats_stop)
                self.middleware_stats_exchange.start_consuming(stats_callback)
            except Exception as e:
                logging.error(
                    f"action: stats_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            # Consume coordination barrier forwards
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

            while stats_results:
    
                raw_stats = stats_results.popleft()
                self._process_stats_message(raw_stats)

            while coord_results:
                raw_coord = coord_results.popleft()
                self._process_coord_message(raw_coord)

            while data_chunks:
    
                msg = data_chunks.popleft()
                try:
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

        if self._can_send_end_message(data.client_id, data.table_type):
            self._send_end_message(data.client_id, data.table_type)

    def _process_stats_message(self, raw_msg: bytes):
        try:
            if raw_msg.startswith(b"AGG_STATS_END"):
                stats_end = AggregatorStatsEndMessage.decode(raw_msg)
                logging.info(
                    f"action: stats_end_received | type:{self.aggregator_type} | agg_id:{stats_end.aggregator_id} "
                    f"| cli_id:{stats_end.client_id} | table_type:{stats_end.table_type}"
                )
                self.delete_client_data(stats_end)
                return

            stats = AggregatorStatsMessage.decode(raw_msg)
        except Exception as e:
            logging.error(f"action: error_decoding_stats_message | error:{e}")
            return

        # logging.debug(
        #    f"action: stats_received | type:{self.aggregator_type} | agg_id:{stats.aggregator_id} "
        #    f"| cli_id:{stats.client_id} | file_type:{stats.table_type} "
        #    f"| chunks_received:{stats.chunks_received} | chunks_processed:{stats.chunks_processed}"
        # )

        if self.working_state.is_processed(stats.message_id):
            logging.debug(f"action: duplicate_stats_message_ignored | message_id:{stats.message_id}")
            return

        current_received = self.working_state.get_received_for_aggregator(
            stats.client_id, stats.table_type, stats.aggregator_id
        )
        current_processed = self.working_state.get_processed_for_aggregator(
            stats.client_id, stats.table_type, stats.aggregator_id
        )

        # Always record END/expected totals, even if counts did not change
        self.working_state.mark_end_message_received(stats.client_id, stats.table_type)
        self.working_state.set_chunks_to_receive(stats.client_id, stats.table_type, stats.total_expected)

        if current_received == stats.chunks_received and current_processed == stats.chunks_processed:
            # logging.debug(
            #    f"action: stats_no_change | type:{self.aggregator_type} | agg_id:{stats.aggregator_id} "
            #    f"| cli_id:{stats.client_id} | file_type:{stats.table_type} | expected:{stats.total_expected}"
            # )
            pass
        else:
            self.working_state.update_chunks_received(
                stats.client_id,
                stats.table_type,
                stats.aggregator_id,
                stats.chunks_received,
            )
            self.working_state.update_chunks_processed(
                stats.client_id,
                stats.table_type,
                stats.aggregator_id,
                stats.chunks_processed,
            )

        self.working_state.mark_processed(stats.message_id)
        self._save_state(stats.message_id)

        # Si aún no conocemos el total global esperado, tomarlo de los stats recibidos
        if self.working_state.get_global_total_expected(stats.client_id, stats.table_type) is None:
            self.working_state.set_global_total_expected(stats.client_id, stats.table_type, stats.total_expected)

        if self._can_send_end_message(stats.client_id, stats.table_type):
            self._send_end_message(stats.client_id, stats.table_type)

    def _process_coord_message(self, raw_msg: bytes):
        try:
                data = json.loads(raw_msg)
                if data.get("type") != MSG_BARRIER_FORWARD:
                    return
                stage = data.get("stage")
                if stage != self.stage:
                    return
                shard = data.get("shard", DEFAULT_SHARD)
                client_id = data.get("client_id")
                key = (client_id, stage, shard)
                if key in self.barrier_forwarded:
                    return
                total_chunks = data.get("total_chunks", 0)
                # Map stage to table_type
                if self.aggregator_type == "PRODUCTS":
                    table_type = TableType.TRANSACTION_ITEMS
                elif self.aggregator_type in ["PURCHASES", "TPV"]:
                    table_type = TableType.TRANSACTIONS
                else:
                    return
                self.working_state.set_global_total_expected(client_id, table_type, total_chunks)
                self.working_state.set_chunks_to_receive(client_id, table_type, total_chunks)
                self.working_state.mark_end_message_received(client_id, table_type)
                self._maybe_send_stats(client_id, table_type, force=True)
                self._send_end_message(client_id, table_type)
                self.barrier_forwarded.add(key)
                logging.info(f"action: barrier_forward_consumed | type:{self.aggregator_type} | cli_id:{client_id} | shard:{shard} | total_chunks:{total_chunks}")
        except Exception as e:
            logging.error(f"action: barrier_forward_error | type:{self.aggregator_type} | error:{e}")

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

        self.working_state.mark_end_message_received(client_id, table_type)
        self.working_state.set_global_total_expected(client_id, table_type, total_expected)
        
        # Broadcast this global total to peers so they also know when to terminate
        # We can use a new message type or reuse AggregatorStatsMessage with a special flag/value?
        # Better to add a field to AggregatorStatsMessage or use a dedicated message.
        # For now, let's assume AggregatorStatsMessage has 'total_expected'. 
        # When we receive END, we update our 'total_expected' and broadcast it in our next stats update.
        # The _maybe_send_stats uses get_chunks_to_receive which we just updated.
        
        self.working_state.set_chunks_to_receive(client_id, table_type, total_expected)
        self.working_state.set_global_total_expected(client_id, table_type, total_expected)

        self._save_state(uuid.uuid4())

        self._maybe_send_stats(client_id, table_type, force=True)

        if self._can_send_end_message(client_id, table_type):
            self._send_end_message(client_id, table_type)

    def _handle_data_chunk(self, raw_msg: bytes):
        self._check_crash_point("CRASH_BEFORE_PROCESS")
        chunk = ProcessBatchReader.from_bytes(raw_msg)
        client_id = chunk.client_id()
        table_type = chunk.table_type()

        if self.working_state.is_processed(chunk.message_id()):
            logging.info(f"action: duplicate_chunk_ignored | message_id:{chunk.message_id()}")
            return

        self.persistence.commit_processing_chunk(chunk)

        logging.info(
            f"action: aggregate | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type} | rows_in:{len(chunk.rows)}"
        )

        self.working_state.increment_chunks_received(
            client_id,
            table_type,
            self.aggregator_id,
            1,
        )

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

        self.working_state.increment_chunks_processed(
            client_id,
            table_type,
            self.aggregator_id,
            1,
        )
        self.working_state.increment_accumulated_chunks(client_id, table_type, self.aggregator_id)

        self._check_crash_point("CRASH_AFTER_PROCESS_BEFORE_COMMIT")

        if payload:
            try:
                data_msg = AggregatorDataMessage(
                    self.aggregator_type,
                    self.aggregator_id,
                    client_id,
                    table_type,
                    payload,
                )
                logging.debug(
                    f"action: aggregator_data_sent | type:{self.aggregator_type} | agg_id:{self.aggregator_id} "
                    f"| client_id:{client_id} | table_type:{table_type} | payload_size:{len(payload)}"
                )
                self.middleware_data_exchange.send(data_msg.encode())
                self.persistence.commit_send_ack(client_id, chunk.message_id())
            except Exception as e:
                logging.error(f"action: error_sending_data_message | error:{e}")

        self.working_state.mark_processed(chunk.message_id())
        self._save_state(chunk.message_id())

        self._maybe_send_stats(client_id, table_type)

        if self._can_send_end_message(client_id, table_type):
            self._send_end_message(client_id, table_type)

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


    # Helper methods removed as they are now in WorkingState

    def _maybe_send_stats(self, client_id, table_type, force=False):
        total_expected = self.working_state.get_chunks_to_receive(client_id, table_type)
        if total_expected is None:
            return

        received = self.working_state.get_received_for_aggregator(client_id, table_type, self.aggregator_id)
        processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
    
        stats_msg = AggregatorStatsMessage(
            self.aggregator_id,
            client_id,
            table_type,
            total_expected,
            received,
            processed,
        )
        # Evitar spam: solo loggear en DEBUG cuando hay cambios o force
        if force or not self.working_state.was_stats_sent(client_id, table_type, (received, processed, total_expected)):
            logging.debug(
                f"action: aggregator_stats_sent | type:{self.aggregator_type} | agg_id:{self.aggregator_id} "
                f"| client_id:{client_id} | table_type:{table_type} | received:{received} | processed:{processed} | expected:{total_expected}"
            )
            
        # Log stack trace to find loop source
        # import traceback
        # logging.debug(f"action: maybe_send_stats_called | stack:{''.join(traceback.format_stack())}")

        self.middleware_stats_exchange.send(stats_msg.encode())
        self.working_state.mark_stats_sent(client_id, table_type, (received, processed, total_expected))
        # Publish coordination stats (throttled by was_stats_sent)
        try:
            payload = {
                "type": MSG_WORKER_STATS,
                "id": str(self.aggregator_id),
                "client_id": client_id,
                "stage": self.stage,
                "shard": DEFAULT_SHARD,
                "expected": total_expected,
                "chunks": received,
                "processed": processed,
                "sender": str(self.aggregator_id),
            }
            rk = f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"
            self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
            logging.debug(f"action: coordination_stats_sent | stage:{self.stage} | cli_id:{client_id} | received:{received} | processed:{processed}")
        except Exception as e:
            logging.error(f"action: coordination_stats_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")



    def _can_send_end_message(self, client_id, table_type):
        # Symmetric Termination Logic
        
        # 1. Check if we have received the global total expected (from upstream)
        global_total = self.working_state.get_global_total_expected(client_id, table_type)
        if global_total is None:
            return False

        # 2. Check if we have processed everything we received locally
        received = self.working_state.get_received_for_aggregator(client_id, table_type, self.aggregator_id)
        processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
        
        if processed < received:
             # if processed % 100 == 0:
             #    logging.debug(f"action: waiting_local_processing | type:{self.aggregator_type} | client_id:{client_id} | processed:{processed} | received:{received}")
             return False

        # 3. Check if all peers have processed their part
        # Sum of processed chunks from all aggregators (including self)
        total_processed_global = self.working_state.get_total_processed_global(client_id, table_type)
        
        if total_processed_global < global_total:
             # if total_processed_global % 100 == 0:
             #    logging.debug(f"action: waiting_global_processing | type:{self.aggregator_type} | client_id:{client_id} | global_processed:{total_processed_global} | global_expected:{global_total}")
             return False
             
        # 4. Check accumulation consistency (processed vs accumulated)
        total_accumulated = self.working_state.get_total_accumulated(client_id, table_type)
        # Note: total_accumulated tracks what THIS aggregator has accumulated. 
        # But get_total_processed_global tracks what ALL aggregators have processed.
        # We should check if OUR processed matches OUR accumulated.
        
        # Actually, get_total_accumulated in working_state sums up accumulation counts from all aggregators if they send it?
        # Let's check working_state.increment_accumulated_chunks. It seems it tracks per aggregator.
        # So get_total_accumulated returns sum of accumulated chunks for all aggregators.
        
        if total_processed_global != total_accumulated:
             # logging.debug(f"action: waiting_accumulation_consistency | type:{self.aggregator_type} | client_id:{client_id} | global_processed:{total_processed_global} | global_accumulated:{total_accumulated}")
             return False

        logging.info(f"action: symmetric_termination_condition_met | type:{self.aggregator_type} | client_id:{client_id} | global_total:{global_total}")
        return True

    def _send_end_message(self, client_id, table_type):
        total_processed = self.working_state.get_total_processed(client_id, table_type)
        logging.info(
            f"action: sending_end_message | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type.name} | total_chunks:{total_processed} | expected_chunks:{self.working_state.get_chunks_to_receive(client_id, table_type)}"
        )

        logging.debug(
            f"action: publish_final_results_trigger | type:{self.aggregator_type} | client_id:{client_id}"
        )
        self.publish_final_results(client_id, table_type)

        # Send END message with OUR ID
        try:
            # We send the count of chunks WE processed/generated. 
            # Actually, for the next stage (Maximizer/Joiner), they need to know how many items to expect.
            # If we are sending results (e.g. Products), we send chunks of results.
            # The count in MessageEnd should be the number of RESULT chunks we sent?
            # Or the number of input chunks we processed?
            
            # The protocol usually expects "total_items" or "total_chunks".
            # If downstream waits for "total_expected", and we send "total_processed", it might be confusing if they sum it up.
            # But wait, the Maximizer will sum up "count" from all aggregators.
            # So we should send the number of items/chunks WE produced for the next stage.
            
            # However, `publish_final_results` sends data but doesn't return the count easily here.
            # But wait, `publish_final_results` sends ONE chunk per queue usually, or multiple.
            # Let's look at `publish_final_products`: it iterates and sends chunks.
            
            # For simplicity in this refactor, let's assume we send 1 END message per aggregator.
            # The count in MessageEnd is used by the receiver.
            # If the receiver sums them up, it gets a total.
            # What does the receiver use this total for?
            # Maximizer uses it to know if it received all data? No, it uses it to know "total_expected".
            
            # If we change the logic so Maximizer waits for N end messages, the "count" field becomes "payload size" or "items produced".
            # Let's send 0 for now if it's not strictly used for verification, OR send the number of result chunks we sent.
            # Given we don't track result chunks sent count easily here without modifying publish_final_results, 
            # and the previous logic sent "total_processed" (input chunks), let's stick to sending "total_processed" 
            # BUT downstream must know this is "input chunks processed by this aggregator" if they want to verify.
            
            # Actually, the previous logic sent `total_processed` which was the GLOBAL total processed (since it was leader).
            # Now each aggregator sends its OWN processed count.
            # So Sum(OwnProcessed) = GlobalTotalProcessed = GlobalTotalExpected.
            # This maintains consistency!
            
            my_processed = self.working_state.get_processed_for_aggregator(client_id, table_type, self.aggregator_id)
            end_msg = MessageEnd(client_id, table_type, my_processed, str(self.aggregator_id))
            
            for queue in self.middleware_queue_sender.values():
                queue.send(end_msg.encode())
            logging.info(
                f"action: sent_end_to_next_stage | type:{self.aggregator_type} | cli_id:{client_id} | my_processed:{my_processed}"
            )
            # Publish coordination END
            try:
                payload = {
                    "type": MSG_WORKER_END,
                    "id": str(self.aggregator_id),
                    "client_id": client_id,
                    "stage": self.stage,
                    "shard": DEFAULT_SHARD,
                    "expected": self.working_state.get_chunks_to_receive(client_id, table_type),
                    "chunks": my_processed,
                    "sender": str(self.aggregator_id),
                }
                rk = f"coordination.barrier.{self.stage}.{DEFAULT_SHARD}"
                self.middleware_coordination.send(json.dumps(payload).encode("utf-8"), routing_key=rk)
                logging.debug(f"action: coordination_end_sent | stage:{self.stage} | cli_id:{client_id} | chunks:{my_processed}")
            except Exception as e:
                logging.error(f"action: coordination_end_send_error | stage:{self.stage} | cli_id:{client_id} | error:{e}")
        except Exception as e:
            logging.error(f"action: error_sending_end_message | error:{e}")

        stats_end = AggregatorStatsEndMessage(self.aggregator_id, client_id, table_type)
        self.middleware_stats_exchange.send(stats_end.encode())
        self.delete_client_data(stats_end)

    def delete_client_data(self, stats_end: AggregatorStatsEndMessage):
        client_id = stats_end.client_id
        table_type = stats_end.table_type
        accumulator_key = self._accumulator_key()
        
        self.working_state.delete_client_data(client_id, table_type, accumulator_key)

        logging.info(
            f"action: cleanup_state | client_id:{client_id} | table_type:{table_type} | aggregator_id:{self.aggregator_id}"
        )

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
            count = int(row.final_amount)
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

    def _build_purchases_payload(self, aggregated_chunk: ProcessChunk):
        return {
            "purchases": [
                [int(row.store_id), int(row.user_id), int(row.final_amount)]
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
                rows.append(TransactionsProcessRow("", int(store_id), int(user_id), int(count), marker_date))
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

        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
        rows_by_queue = defaultdict(list)

        for (item_id, year, month), totals in data.items():
            shard = self.id_to_shard.get(item_id)
            if shard is None:
                logging.warning(
                    f"action: publish_final_products_missing_shard | client_id:{client_id} | item_id:{item_id}"
                )
                continue

            created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
            rows_by_queue[shard.queue_name].append(
                TransactionItemsProcessRow(
                    "",
                    item_id,
                    totals["quantity"],
                    totals["subtotal"],
                    created_at,
                )
            )

        for queue_name, rows in rows_by_queue.items():
            chunk = ProcessChunk(header, rows).serialize()
            self.middleware_queue_sender[queue_name].send(chunk)
            logging.info(
                f"action: publish_final_products | client_id:{client_id} | queue:{queue_name} | rows:{len(rows)}"
            )

    def _publish_final_purchases(self, client_id):
        data = self.working_state.global_accumulator[client_id].get("purchases")
        if not data:
            return

        from utils.processing.process_chunk import ProcessChunkHeader

        placeholder_date = datetime.date(2024, 1, 1)

        rows_by_queue = defaultdict(list)

        for store_id, users in data.items():
            shard = self.id_to_shard.get(store_id)
            if shard is None:
                logging.warning(
                    f"action: publish_final_purchases_missing_shard | client_id:{client_id} | store_id:{store_id}"
                )
                continue
            for user_id, count in users.items():
                rows_by_queue[shard.queue_name].append(
                    PurchasesPerUserStoreRow(
                        store_id=store_id,
                        store_name="",
                        user_id=user_id,
                        user_birthdate=placeholder_date,
                        purchases_made=count,
                    )
                )

        for queue_name, rows in rows_by_queue.items():
            header = ProcessChunkHeader(
                client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE
            )
            chunk = ProcessChunk(header, rows)
            chunk_data = chunk.serialize()
            logging.info(
                f"action: publish_final_purchases_chunk | client_id:{client_id} | queue:{queue_name} "
                f"| rows:{len(rows)} | bytes:{len(chunk_data)}"
            )
            self.middleware_queue_sender[queue_name].send(chunk_data)
            logging.info(
                f"action: publish_final_purchases | client_id:{client_id} | queue:{queue_name} | rows:{len(rows)}"
            )

    def _publish_final_tpv(self, client_id):
        data = self.working_state.global_accumulator[client_id].get("tpv")
        if not data:
            return

        from utils.processing.process_chunk import ProcessChunkHeader

        rows = []
        for (year, semester, store_id), total in data.items():
            year_half = YearHalf(year, semester)
            rows.append(TPVProcessRow(store_id=store_id, tpv=total, year_half=year_half))

        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TPV)
        chunk = ProcessChunk(header, rows)
        self.middleware_queue_sender["to_join_with_stores_tvp"].send(chunk.serialize())
        logging.info(
            f"action: publish_final_tpv | client_id:{client_id} | rows:{len(rows)}"
        )

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

            if self.id_to_shard and item_id not in self.id_to_shard:
                logging.warning(
                    f"action: apply_products_missing_shard | client_id:{chunk.header.client_id} | item_id:{item_id}"
                )
                continue

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
            row = TransactionsProcessRow(
                transaction_id="",
                store_id=store_id,
                user_id=user_id,
                final_amount=float(count),
                created_at=marker_date,
            )
            rows.append(row)

        from utils.processing.process_chunk import ProcessChunkHeader

        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TRANSACTIONS)
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

        rows = []
        for (year, semester, store_id), total_tpv in chunk_accumulator.items():
            year_half = YearHalf(year, semester)
            row = TPVProcessRow(store_id=store_id, tpv=total_tpv, year_half=year_half)
            rows.append(row)

        from utils.processing.process_chunk import ProcessChunkHeader

        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TPV)
        return ProcessChunk(header, rows)
