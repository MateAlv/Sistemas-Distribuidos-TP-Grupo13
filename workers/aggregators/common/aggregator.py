import datetime
import logging
from collections import defaultdict, deque
from types import SimpleNamespace

from utils.file_utils.process_table import (
    TransactionItemsProcessRow,
    TransactionsProcessRow,
    PurchasesPerUserStoreRow,
    TPVProcessRow,
    YearHalf,
)
from utils.file_utils.process_chunk import ProcessChunk
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import DateTime
from utils.file_utils.end_messages import MessageEnd
from utils.file_utils.table_type import TableType
from middleware.middleware_interface import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    TIMEOUT,
)
from .aggregator_stats_messages import (
    AggregatorStatsMessage,
    AggregatorStatsEndMessage,
    AggregatorDataMessage,
)


class Aggregator:
    def __init__(self, agg_type: str, agg_id: int = 1):
        logging.getLogger("pika").setLevel(logging.CRITICAL)

        self._running = True

        self.aggregator_type = agg_type
        self.aggregator_id = agg_id
        self.middleware_queue_sender = {}

        # Estado distribuido entre hermanos
        self.end_message_received = {}
        self.chunks_received_per_client = {}
        self.chunks_processed_per_client = {}
        self.accumulated_chunks_per_client = {}
        self.chunks_to_receive = {}
        self.already_sent_stats = {}

        # Acumuladores globales
        self.global_accumulator = {}

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

        if self.aggregator_type == "PRODUCTS":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_1+2")
            self.middleware_queue_sender["to_max_1_3"] = MessageMiddlewareQueue("rabbitmq", "to_max_1_3")
            self.middleware_queue_sender["to_max_4_6"] = MessageMiddlewareQueue("rabbitmq", "to_max_4_6")
            self.middleware_queue_sender["to_max_7_8"] = MessageMiddlewareQueue("rabbitmq", "to_max_7_8")
        elif self.aggregator_type == "PURCHASES":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_4")
            self.middleware_queue_sender["to_top_1_3"] = MessageMiddlewareQueue("rabbitmq", "to_top_1_3")
            self.middleware_queue_sender["to_top_4_6"] = MessageMiddlewareQueue("rabbitmq", "to_top_4_6")
            self.middleware_queue_sender["to_top_7_10"] = MessageMiddlewareQueue("rabbitmq", "to_top_7_10")
        elif self.aggregator_type == "TPV":
            self.middleware_queue_receiver = MessageMiddlewareQueue("rabbitmq", "to_agg_3")
            self.middleware_queue_sender["to_join_with_stores_tvp"] = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        else:
            raise ValueError(f"Tipo de agregador inválido: {self.aggregator_type}")

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    def shutdown(self, signum, frame):
        logging.info(
            f"action: shutdown_signal_received | type:{self.aggregator_type} | agg_id:{self.aggregator_id}"
        )
        try:
            self._running = False
            self.middleware_queue_receiver.stop_consuming()
            self.middleware_queue_receiver.close()
            for sender in self.middleware_queue_sender.values():
                sender.stop_consuming()
                sender.close()
            self.middleware_stats_exchange.stop_consuming()
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
            self.middleware_stats_exchange.close()
            self.middleware_data_exchange.stop_consuming()
            self.middleware_data_exchange.close()
            logging.info(
                f"action: shutdown_completed | type:{self.aggregator_type} | agg_id:{self.aggregator_id}"
            )
        except Exception as e:
            logging.error(
                f"action: shutdown_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
            )

    def run(self):
        logging.info(
            f"Agregador iniciado. Tipo: {self.aggregator_type}, ID: {self.aggregator_id}"
        )

        data_results = deque()
        stats_results = deque()
        data_chunks = deque()

        def data_callback(msg):
            data_results.append(msg)

        def stats_callback(msg):
            stats_results.append(msg)

        def chunk_callback(msg):
            data_chunks.append(msg)

        def data_stop():
            self.middleware_data_exchange.stop_consuming()

        def stats_stop():
                self.middleware_stats_exchange.stop_consuming()

        def chunk_stop():
            self.middleware_queue_receiver.stop_consuming()

        while self._running:
            try:
                self.middleware_queue_receiver.connection.call_later(TIMEOUT, chunk_stop)
                self.middleware_queue_receiver.start_consuming(chunk_callback)
            except Exception as e:
                logging.error(
                    f"action: data_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            try:
                self.middleware_data_exchange.connection.call_later(TIMEOUT, data_stop)
                self.middleware_data_exchange.start_consuming(data_callback)
            except Exception as e:
                logging.error(
                    f"action: data_exchange_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            try:
                self.middleware_stats_exchange.connection.call_later(TIMEOUT, stats_stop)
                self.middleware_stats_exchange.start_consuming(stats_callback)
            except Exception as e:
                logging.error(
                    f"action: stats_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            while data_results:
                raw_data = data_results.popleft()
                self._process_data_message(raw_data)

            while stats_results:
                raw_stats = stats_results.popleft()
                self._process_stats_message(raw_stats)

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

    # ------------------------------------------------------------------
    # Procesamiento de mensajes
    # ------------------------------------------------------------------
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

        self._apply_remote_aggregation(data)
        self._increment_accumulated_chunks(data.client_id, data.table_type, data.aggregator_id)

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

        logging.info(
            f"action: stats_received | type:{self.aggregator_type} | agg_id:{stats.aggregator_id} "
            f"| cli_id:{stats.client_id} | file_type:{stats.table_type} "
            f"| chunks_received:{stats.chunks_received} | chunks_processed:{stats.chunks_processed}"
        )

        self._set_aggregator_value(
            self.chunks_received_per_client,
            stats.client_id,
            stats.table_type,
            stats.aggregator_id,
            stats.chunks_received,
        )
        self._set_aggregator_value(
            self.chunks_processed_per_client,
            stats.client_id,
            stats.table_type,
            stats.aggregator_id,
            stats.chunks_processed,
        )
        self._ensure_dict_entry(self.end_message_received, stats.client_id, stats.table_type, default=False)
        self._ensure_dict_entry(self.chunks_to_receive, stats.client_id, stats.table_type)

        self.end_message_received[stats.client_id][stats.table_type] = True
        self.chunks_to_receive[stats.client_id][stats.table_type] = stats.total_expected

        # Enviar mis propios stats si ya procesé algo
        self._maybe_send_stats(stats.client_id, stats.table_type)

        if self._can_send_end_message(stats.client_id, stats.table_type):
            self._send_end_message(stats.client_id, stats.table_type)

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

        self._ensure_dict_entry(self.end_message_received, client_id, table_type, default=False)
        self._ensure_dict_entry(self.chunks_to_receive, client_id, table_type)
        self.end_message_received[client_id][table_type] = True
        self.chunks_to_receive[client_id][table_type] = total_expected

        self._maybe_send_stats(client_id, table_type)

        if self._can_send_end_message(client_id, table_type):
            self._send_end_message(client_id, table_type)

    def _handle_data_chunk(self, raw_msg: bytes):
        chunk = ProcessBatchReader.from_bytes(raw_msg)
        client_id = chunk.client_id()
        table_type = chunk.table_type()

        logging.info(
            f"action: aggregate | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type} | rows_in:{len(chunk.rows)}"
        )

        self._increment_aggregator_value(
            self.chunks_received_per_client,
            client_id,
            table_type,
            self.aggregator_id,
            1,
        )

        has_output = False
        payload = None

        if self.aggregator_type == "PRODUCTS":
            aggregated = self.apply_products(chunk)
            if aggregated:
                rows_1_3, rows_4_6, rows_7_8 = aggregated
                self.accumulate_products(client_id, rows_1_3, rows_4_6, rows_7_8)
                payload = self._build_products_payload(rows_1_3, rows_4_6, rows_7_8)
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

        if has_output:
            self._increment_aggregator_value(
                self.chunks_processed_per_client,
                client_id,
                table_type,
                self.aggregator_id,
                1,
            )
            self._increment_accumulated_chunks(client_id, table_type, self.aggregator_id)

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
                except Exception as e:
                    logging.error(f"action: error_sending_data_message | error:{e}")

        self._maybe_send_stats(client_id, table_type)

        if self._can_send_end_message(client_id, table_type):
            self._send_end_message(client_id, table_type)

    # ------------------------------------------------------------------
    # Helpers para stats
    # ------------------------------------------------------------------
    def _ensure_dict_entry(self, dictionary, client_id, table_type, default=0):
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = default

    def _ensure_client_table_entry(self, dictionary, client_id, table_type):
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = {}

    def _get_aggregator_value(self, dictionary, client_id, table_type, aggregator_id):
        return dictionary.get(client_id, {}).get(table_type, {}).get(aggregator_id, 0)

    def _set_aggregator_value(self, dictionary, client_id, table_type, aggregator_id, value):
        self._ensure_client_table_entry(dictionary, client_id, table_type)
        dictionary[client_id][table_type][aggregator_id] = value

    def _increment_aggregator_value(self, dictionary, client_id, table_type, aggregator_id, delta):
        current = self._get_aggregator_value(dictionary, client_id, table_type, aggregator_id)
        self._set_aggregator_value(dictionary, client_id, table_type, aggregator_id, current + delta)

    def _sum_counts(self, dictionary, client_id, table_type):
        return sum(dictionary.get(client_id, {}).get(table_type, {}).values())

    def _increment_accumulated_chunks(self, client_id, table_type, aggregator_id):
        self._increment_aggregator_value(
            self.accumulated_chunks_per_client,
            client_id,
            table_type,
            aggregator_id,
            1,
        )

    def _maybe_send_stats(self, client_id, table_type):
        if client_id not in self.chunks_to_receive or table_type not in self.chunks_to_receive[client_id]:
            return

        total_expected = self.chunks_to_receive[client_id][table_type]
        received = self._get_aggregator_value(
            self.chunks_received_per_client, client_id, table_type, self.aggregator_id
        )
        processed = self._get_aggregator_value(
            self.chunks_processed_per_client, client_id, table_type, self.aggregator_id
        )

        last_sent = self.already_sent_stats.get((client_id, table_type))
        if last_sent == (received, processed):
            return

        stats_msg = AggregatorStatsMessage(
            self.aggregator_id,
            client_id,
            table_type,
            total_expected,
            received,
            processed,
        )
        logging.debug(
            f"action: aggregator_stats_sent | type:{self.aggregator_type} | agg_id:{self.aggregator_id} "
            f"| client_id:{client_id} | table_type:{table_type} | received:{received} | processed:{processed} | expected:{total_expected}"
        )
        self.middleware_stats_exchange.send(stats_msg.encode())
        self.already_sent_stats[(client_id, table_type)] = (received, processed)

    def _leader_id(self, client_id, table_type):
        aggregators = self.chunks_received_per_client.get(client_id, {}).get(table_type, {}).keys()
        return min(aggregators) if aggregators else self.aggregator_id

    def _can_send_end_message(self, client_id, table_type):
        if client_id not in self.chunks_to_receive or table_type not in self.chunks_to_receive[client_id]:
            return False

        total_expected = self.chunks_to_receive[client_id][table_type]
        total_received = self._sum_counts(self.chunks_received_per_client, client_id, table_type)
        if total_received < total_expected:
            logging.debug(
                f"action: can_send_end_waiting_more_chunks | type:{self.aggregator_type} | client_id:{client_id} "
                f"| table_type:{table_type} | expected:{total_expected} | received:{total_received}"
            )
            return False

        total_processed = self._sum_counts(self.chunks_processed_per_client, client_id, table_type)
        total_accumulated = self._sum_counts(self.accumulated_chunks_per_client, client_id, table_type)
        if total_processed != total_accumulated:
            logging.debug(
                f"action: can_send_end_waiting_accumulation | type:{self.aggregator_type} | client_id:{client_id} "
                f"| table_type:{table_type} | processed:{total_processed} | accumulated:{total_accumulated}"
            )
            return False

        leader_id = self._leader_id(client_id, table_type)
        if self.aggregator_id != leader_id:
            logging.debug(
                f"action: can_send_end_not_leader | type:{self.aggregator_type} | client_id:{client_id} "
                f"| table_type:{table_type} | leader:{leader_id} | self:{self.aggregator_id}"
            )
            return False

        return True

    def _send_end_message(self, client_id, table_type):
        total_processed = self._sum_counts(self.chunks_processed_per_client, client_id, table_type)
        logging.info(
            f"action: sending_end_message | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type.name} | total_chunks:{total_processed} | expected_chunks:{self.chunks_to_receive[client_id][table_type]}"
        )

        logging.debug(
            f"action: publish_final_results_trigger | type:{self.aggregator_type} | client_id:{client_id}"
        )
        self.publish_final_results(client_id, table_type)

        try:
            end_msg = MessageEnd(client_id, table_type, total_processed)
            for queue in self.middleware_queue_sender.values():
                queue.send(end_msg.encode())
            logging.info(
                f"action: sent_end_to_next_stage | type:{self.aggregator_type} | cli_id:{client_id}"
            )
        except Exception as e:
            logging.error(f"action: error_sending_end_message | error:{e}")

        stats_end = AggregatorStatsEndMessage(self.aggregator_id, client_id, table_type)
        self.middleware_stats_exchange.send(stats_end.encode())
        self.delete_client_data(stats_end)

    def delete_client_data(self, stats_end: AggregatorStatsEndMessage):
        client_id = stats_end.client_id
        table_type = stats_end.table_type

        for dictionary in [
            self.end_message_received,
            self.chunks_received_per_client,
            self.chunks_processed_per_client,
            self.accumulated_chunks_per_client,
            self.chunks_to_receive,
        ]:
            if client_id in dictionary and table_type in dictionary[client_id]:
                del dictionary[client_id][table_type]
                if not dictionary[client_id]:
                    del dictionary[client_id]

        if (client_id, table_type) in self.already_sent_stats:
            del self.already_sent_stats[(client_id, table_type)]

        accumulator_key = self._accumulator_key()
        if (
            client_id in self.global_accumulator
            and accumulator_key in self.global_accumulator[client_id]
        ):
            del self.global_accumulator[client_id][accumulator_key]
            if not self.global_accumulator[client_id]:
                del self.global_accumulator[client_id]

        logging.info(
            f"action: cleanup_state | client_id:{client_id} | table_type:{table_type} | aggregator_id:{self.aggregator_id}"
        )

    # ------------------------------------------------------------------
    # Acumuladores y publicación final
    # ------------------------------------------------------------------
    def _ensure_global_entry(self, client_id):
        if client_id not in self.global_accumulator:
            self.global_accumulator[client_id] = {}

    def _accumulator_key(self):
        if self.aggregator_type == "PRODUCTS":
            return "products"
        if self.aggregator_type == "PURCHASES":
            return "purchases"
        if self.aggregator_type == "TPV":
            return "tpv"
        return "unknown"

    def accumulate_products(self, client_id, rows_1_3, rows_4_6, rows_7_8):
        self._ensure_global_entry(client_id)
        data = self.global_accumulator[client_id].setdefault(
            "products",
            {
                "1_3": defaultdict(lambda: {"quantity": 0, "subtotal": 0.0}),
                "4_6": defaultdict(lambda: {"quantity": 0, "subtotal": 0.0}),
                "7_8": defaultdict(lambda: {"quantity": 0, "subtotal": 0.0}),
            },
        )

        for row in rows_1_3:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            data["1_3"][key]["quantity"] += row.quantity
            data["1_3"][key]["subtotal"] += row.subtotal

        for row in rows_4_6:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            data["4_6"][key]["quantity"] += row.quantity
            data["4_6"][key]["subtotal"] += row.subtotal

        for row in rows_7_8:
            key = (row.item_id, row.created_at.date.year, row.created_at.date.month)
            data["7_8"][key]["quantity"] += row.quantity
            data["7_8"][key]["subtotal"] += row.subtotal

    def accumulate_purchases(self, client_id, aggregated_chunk: ProcessChunk):
        self._ensure_global_entry(client_id)
        data = self.global_accumulator[client_id].setdefault(
            "purchases", defaultdict(lambda: defaultdict(int))
        )

        for row in aggregated_chunk.rows:
            store_id = int(row.store_id)
            user_id = int(row.user_id)
            count = int(row.final_amount)
            data[store_id][user_id] += count

    def accumulate_tpv(self, client_id, aggregated_chunk: ProcessChunk):
        self._ensure_global_entry(client_id)
        data = self.global_accumulator[client_id].setdefault("tpv", defaultdict(float))

        for row in aggregated_chunk.rows:
            if isinstance(row, TPVProcessRow):
                key = (row.year_half.year, row.year_half.half, int(row.store_id))
                data[key] += float(row.tpv)
            else:
                year = row.created_at.year
                semester = 1 if row.created_at.month <= 6 else 2
                key = (year, semester, int(row.store_id))
                data[key] += float(row.final_amount)

    def _build_products_payload(self, rows_1_3, rows_4_6, rows_7_8):
        return {
            "products_1_3": [
                [int(row.item_id), int(row.created_at.date.year), int(row.created_at.date.month), int(row.quantity), float(row.subtotal)]
                for row in rows_1_3
            ],
            "products_4_6": [
                [int(row.item_id), int(row.created_at.date.year), int(row.created_at.date.month), int(row.quantity), float(row.subtotal)]
                for row in rows_4_6
            ],
            "products_7_8": [
                [int(row.item_id), int(row.created_at.date.year), int(row.created_at.date.month), int(row.quantity), float(row.subtotal)]
                for row in rows_7_8
            ],
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
            rows_1_3, rows_4_6, rows_7_8 = [], [], []
            for item_id, year, month, quantity, subtotal in payload.get("products_1_3", []):
                created_at = DateTime(datetime.date(int(year), int(month), 1), datetime.time(0, 0))
                rows_1_3.append(TransactionItemsProcessRow("", int(item_id), int(quantity), float(subtotal), created_at))
            for item_id, year, month, quantity, subtotal in payload.get("products_4_6", []):
                created_at = DateTime(datetime.date(int(year), int(month), 1), datetime.time(0, 0))
                rows_4_6.append(TransactionItemsProcessRow("", int(item_id), int(quantity), float(subtotal), created_at))
            for item_id, year, month, quantity, subtotal in payload.get("products_7_8", []):
                created_at = DateTime(datetime.date(int(year), int(month), 1), datetime.time(0, 0))
                rows_7_8.append(TransactionItemsProcessRow("", int(item_id), int(quantity), float(subtotal), created_at))
            if rows_1_3 or rows_4_6 or rows_7_8:
                self.accumulate_products(client_id, rows_1_3, rows_4_6, rows_7_8)

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
        if client_id not in self.global_accumulator:
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
        data = self.global_accumulator[client_id].get("products")
        if not data:
            return

        from utils.file_utils.process_chunk import ProcessChunkHeader

        header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)

        def send_rows(key, queue_name):
            bucket = data.get(key)
            if not bucket:
                return
            rows = []
            for (item_id, year, month), totals in bucket.items():
                created_at = DateTime(datetime.date(year, month, 1), datetime.time(0, 0))
                rows.append(
                    TransactionItemsProcessRow(
                        "",
                        item_id,
                        totals["quantity"],
                        totals["subtotal"],
                        created_at,
                    )
                )
            if rows:
                chunk = ProcessChunk(header, rows).serialize()
                self.middleware_queue_sender[queue_name].send(chunk)
                logging.info(
                    f"action: publish_final_products | client_id:{client_id} | bucket:{key} | rows:{len(rows)}"
                )

        send_rows("1_3", "to_max_1_3")
        send_rows("4_6", "to_max_4_6")
        send_rows("7_8", "to_max_7_8")

    def _publish_final_purchases(self, client_id):
        data = self.global_accumulator[client_id].get("purchases")
        if not data:
            return

        from utils.file_utils.process_chunk import ProcessChunkHeader

        placeholder_date = datetime.date(2024, 1, 1)

        def build_rows(stores):
            rows = []
            for store_id in stores:
                for user_id, count in data[store_id].items():
                    rows.append(
                        PurchasesPerUserStoreRow(
                            store_id=store_id,
                            store_name="",
                            user_id=user_id,
                            user_birthdate=placeholder_date,
                            purchases_made=count,
                        )
                    )
            return rows

        groups = {
            "to_top_1_3": [store for store in data if 1 <= store <= 3],
            "to_top_4_6": [store for store in data if 4 <= store <= 6],
            "to_top_7_10": [store for store in data if 7 <= store <= 10],
        }

        for queue_name, stores in groups.items():
            if not stores:
                continue
            rows = build_rows(stores)
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, rows)
            self.middleware_queue_sender[queue_name].send(chunk.serialize())
            logging.info(
                f"action: publish_final_purchases | client_id:{client_id} | queue:{queue_name} | rows:{len(rows)}"
            )

    def _publish_final_tpv(self, client_id):
        data = self.global_accumulator[client_id].get("tpv")
        if not data:
            return

        from utils.file_utils.process_chunk import ProcessChunkHeader

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

    # ------------------------------------------------------------------
    # Lógica de agregación (sin cambios funcionales)
    # ------------------------------------------------------------------
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

        rows_1_3 = []
        rows_4_6 = []
        rows_7_8 = []

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

            if 1 <= item_id <= 3:
                rows_1_3.append(new_row)
            elif 4 <= item_id <= 6:
                rows_4_6.append(new_row)
            elif 7 <= item_id <= 8:
                rows_7_8.append(new_row)

        return rows_1_3, rows_4_6, rows_7_8

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

        from utils.file_utils.process_chunk import ProcessChunkHeader

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

        from utils.file_utils.process_chunk import ProcessChunkHeader

        header = ProcessChunkHeader(client_id=chunk.header.client_id, table_type=TableType.TPV)
        return ProcessChunk(header, rows)
