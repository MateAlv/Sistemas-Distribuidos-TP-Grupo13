from collections import defaultdict, deque
import datetime
import logging

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
from middleware.middleware_interface import MessageMiddlewareQueue, MessageMiddlewareExchange, TIMEOUT
from .aggregator_stats_messages import AggregatorStatsMessage, AggregatorStatsEndMessage


class Aggregator:
    def __init__(self, agg_type: str, agg_id: int = 1):
        logging.getLogger("pika").setLevel(logging.CRITICAL)

        self._running = True

        self.aggregator_type = agg_type
        self.aggregator_id = agg_id
        self.middleware_queue_sender = {}

        # Per-client tracking (global across all aggregators through stats)
        self.end_message_received = {}
        self.chunks_received_per_client = {}
        self.chunks_processed_per_client = {}
        self.chunks_to_receive = {}
        self.initial_stats_sent = {}

        self.middleware_stats_exchange = MessageMiddlewareExchange(
            "rabbitmq", f"end_exchange_aggregator_{self.aggregator_type}", [""], "fanout"
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
            self.middleware_stats_exchange.close()
            logging.info(
                f"action: shutdown_completed | type:{self.aggregator_type} | agg_id:{self.aggregator_id}"
            )
        except Exception as e:
            logging.error(
                f"action: shutdown_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
            )

    def run(self):
        logging.info(f"Agregador iniciado. Tipo: {self.aggregator_type}, ID: {self.aggregator_id}")
        results = deque()
        stats_results = deque()

        def callback(msg):
            results.append(msg)

        def stats_callback(msg):
            stats_results.append(msg)

        def stop():
            self.middleware_queue_receiver.stop_consuming()

        def stats_stop():
            self.middleware_stats_exchange.stop_consuming()

        while self._running:
            try:
                self.middleware_stats_exchange.connection.call_later(TIMEOUT, stats_stop)
                self.middleware_stats_exchange.start_consuming(stats_callback)
            except Exception as e:
                logging.error(
                    f"action: stats_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            try:
                self.middleware_queue_receiver.connection.call_later(TIMEOUT, stop)
                self.middleware_queue_receiver.start_consuming(callback)
            except Exception as e:
                logging.error(
                    f"action: data_consume_error | type:{self.aggregator_type} | agg_id:{self.aggregator_id} | error:{e}"
                )

            while stats_results:
                raw_stats = stats_results.popleft()
                self._process_stats_message(raw_stats)

            while results:
                msg = results.popleft()
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
    # Message Handlers
    # ------------------------------------------------------------------
    def _process_stats_message(self, raw_msg: bytes):
        try:
            if raw_msg.startswith(b"AGG_STATS_END"):
                stats_end = AggregatorStatsEndMessage.decode(raw_msg)
                if stats_end.aggregator_id == self.aggregator_id:
                    return
                logging.info(
                    f"action: stats_end_received | type:{self.aggregator_type} | agg_id:{stats_end.aggregator_id} "
                    f"| cli_id:{stats_end.client_id} | table_type:{stats_end.table_type}"
                )
                self.delete_client_data(stats_end)
                return

            stats = AggregatorStatsMessage.decode(raw_msg)
            if stats.aggregator_id == self.aggregator_id:
                return

            logging.info(
                f"action: stats_received | type:{self.aggregator_type} | agg_id:{stats.aggregator_id} "
                f"| cli_id:{stats.client_id} | file_type:{stats.table_type} "
                f"| chunks_received:{stats.chunks_received} | chunks_processed:{stats.chunks_processed}"
            )

            self._ensure_dict_entry(self.chunks_received_per_client, stats.client_id, stats.table_type)
            self._ensure_dict_entry(self.chunks_processed_per_client, stats.client_id, stats.table_type)
            self._ensure_dict_entry(self.end_message_received, stats.client_id, stats.table_type, default=False)
            self._ensure_dict_entry(self.chunks_to_receive, stats.client_id, stats.table_type)

            self.end_message_received[stats.client_id][stats.table_type] = True
            self.chunks_to_receive[stats.client_id][stats.table_type] = stats.total_expected
            self.chunks_received_per_client[stats.client_id][stats.table_type] += stats.chunks_received
            self.chunks_processed_per_client[stats.client_id][stats.table_type] += stats.chunks_processed

            if self._can_send_end_message(stats.client_id, stats.table_type):
                self._send_end_message(stats.client_id, stats.table_type)

        except Exception as e:
            logging.error(f"action: error_decoding_stats_message | error:{e}")

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
        self._ensure_dict_entry(self.chunks_received_per_client, client_id, table_type)
        self._ensure_dict_entry(self.chunks_processed_per_client, client_id, table_type)

        self.end_message_received[client_id][table_type] = True
        self.chunks_to_receive[client_id][table_type] = total_expected

        stats_key = (client_id, table_type)
        if not self.initial_stats_sent.get(stats_key):
            stats_msg = AggregatorStatsMessage(
                self.aggregator_id,
                client_id,
                table_type,
                total_expected,
                self.chunks_received_per_client[client_id][table_type],
                self.chunks_processed_per_client[client_id][table_type],
            )
            self.middleware_stats_exchange.send(stats_msg.encode())
            self.initial_stats_sent[stats_key] = True

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

        self._ensure_dict_entry(self.chunks_received_per_client, client_id, table_type)
        self._ensure_dict_entry(self.chunks_processed_per_client, client_id, table_type)
        self.chunks_received_per_client[client_id][table_type] += 1

        has_output = False

        if self.aggregator_type == "PRODUCTS":
            aggregated = self.apply_products(chunk)
            if aggregated:
                rows_1_3, rows_4_6, rows_7_8 = aggregated
                has_output = self._publish_products_results(client_id, rows_1_3, rows_4_6, rows_7_8)
        elif self.aggregator_type == "PURCHASES":
            aggregated_chunk = self.apply_purchases(chunk)
            if aggregated_chunk:
                has_output = self._publish_purchases_results(client_id, aggregated_chunk)
        elif self.aggregator_type == "TPV":
            aggregated_chunk = self.apply_tpv(chunk)
            if aggregated_chunk:
                has_output = self._publish_tpv_results(aggregated_chunk)

        if has_output:
            self.chunks_processed_per_client[client_id][table_type] += 1

        if (
            client_id in self.end_message_received
            and self.end_message_received[client_id].get(table_type, False)
            and client_id in self.chunks_to_receive
            and table_type in self.chunks_to_receive[client_id]
        ):
            total_expected = self.chunks_to_receive[client_id][table_type]
            stats_key = (client_id, table_type)

            if not self.initial_stats_sent.get(stats_key):
                stats_msg = AggregatorStatsMessage(
                    self.aggregator_id,
                    client_id,
                    table_type,
                    total_expected,
                    self.chunks_received_per_client[client_id][table_type],
                    self.chunks_processed_per_client[client_id][table_type],
                )
                self.middleware_stats_exchange.send(stats_msg.encode())
                self.initial_stats_sent[stats_key] = True
            else:
                stats_msg = AggregatorStatsMessage(
                    self.aggregator_id,
                    client_id,
                    table_type,
                    total_expected,
                    1,
                    1 if has_output else 0,
                )
                self.middleware_stats_exchange.send(stats_msg.encode())

        if self._can_send_end_message(client_id, table_type):
            self._send_end_message(client_id, table_type)

    # ------------------------------------------------------------------
    # Publishing helpers
    # ------------------------------------------------------------------
    def _publish_products_results(self, client_id, rows_1_3, rows_4_6, rows_7_8):
        from utils.file_utils.process_chunk import ProcessChunkHeader

        sent = False
        if rows_1_3:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk_data = ProcessChunk(header, rows_1_3).serialize()
            queue = self.middleware_queue_sender["to_max_1_3"]
            queue.send(chunk_data)
            logging.info(
                f"action: publish_products_1_3 | client_id:{client_id} | rows:{len(rows_1_3)} | queue:to_max_1_3"
            )
            sent = True
        if rows_4_6:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk_data = ProcessChunk(header, rows_4_6).serialize()
            queue = self.middleware_queue_sender["to_max_4_6"]
            queue.send(chunk_data)
            logging.info(
                f"action: publish_products_4_6 | client_id:{client_id} | rows:{len(rows_4_6)} | queue:to_max_4_6"
            )
            sent = True
        if rows_7_8:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.TRANSACTION_ITEMS)
            chunk_data = ProcessChunk(header, rows_7_8).serialize()
            queue = self.middleware_queue_sender["to_max_7_8"]
            queue.send(chunk_data)
            logging.info(
                f"action: publish_products_7_8 | client_id:{client_id} | rows:{len(rows_7_8)} | queue:to_max_7_8"
            )
            sent = True
        return sent

    def _publish_purchases_results(self, client_id, aggregated_chunk: ProcessChunk):
        from utils.file_utils.process_chunk import ProcessChunkHeader

        stores_1_3 = []
        stores_4_6 = []
        stores_7_10 = []
        placeholder_date = datetime.date(2024, 1, 1)

        for row in aggregated_chunk.rows:
            store_id = int(row.store_id)
            user_id = int(row.user_id)
            count = int(row.final_amount)
            purchases_row = PurchasesPerUserStoreRow(
                store_id=store_id,
                store_name="",
                user_id=user_id,
                user_birthdate=placeholder_date,
                purchases_made=count,
            )
            if 1 <= store_id <= 3:
                stores_1_3.append(purchases_row)
            elif 4 <= store_id <= 6:
                stores_4_6.append(purchases_row)
            elif 7 <= store_id <= 10:
                stores_7_10.append(purchases_row)

        sent = False
        if stores_1_3:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_1_3)
            queue = self.middleware_queue_sender["to_top_1_3"]
            queue.send(chunk.serialize())
            logging.info(
                f"action: publish_purchases_1_3 | client_id:{client_id} | rows:{len(stores_1_3)} | queue:to_top_1_3"
            )
            sent = True
        if stores_4_6:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_4_6)
            queue = self.middleware_queue_sender["to_top_4_6"]
            queue.send(chunk.serialize())
            logging.info(
                f"action: publish_purchases_4_6 | client_id:{client_id} | rows:{len(stores_4_6)} | queue:to_top_4_6"
            )
            sent = True
        if stores_7_10:
            header = ProcessChunkHeader(client_id=client_id, table_type=TableType.PURCHASES_PER_USER_STORE)
            chunk = ProcessChunk(header, stores_7_10)
            queue = self.middleware_queue_sender["to_top_7_10"]
            queue.send(chunk.serialize())
            logging.info(
                f"action: publish_purchases_7_10 | client_id:{client_id} | rows:{len(stores_7_10)} | queue:to_top_7_10"
            )
            sent = True

        if not sent:
            logging.info(f"action: purchases_chunk_no_output | client_id:{client_id}")
        return sent

    def _publish_tpv_results(self, aggregated_chunk: ProcessChunk):
        if not aggregated_chunk.rows:
            logging.info(
                f"action: tpv_chunk_no_output | client_id:{aggregated_chunk.header.client_id}"
            )
            return False
        queue = self.middleware_queue_sender["to_join_with_stores_tvp"]
        chunk_data = aggregated_chunk.serialize()
        queue.send(chunk_data)
        logging.info(
            f"action: publish_tpv_chunk | result: success | client_id:{aggregated_chunk.header.client_id} "
            f"| rows:{len(aggregated_chunk.rows)} | queue:to_join_with_stores_tvp"
        )
        return True

    # ------------------------------------------------------------------
    # Stats helpers and cleanup
    # ------------------------------------------------------------------
    def _ensure_dict_entry(self, dictionary, client_id, table_type, default=0):
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = default

    def _can_send_end_message(self, client_id, table_type):
        if self.aggregator_id != 1:
            return False
        if client_id not in self.end_message_received or not self.end_message_received[client_id].get(table_type):
            return False
        if client_id not in self.chunks_to_receive or table_type not in self.chunks_to_receive[client_id]:
            return False
        expected = self.chunks_to_receive[client_id][table_type]
        received = self.chunks_received_per_client.get(client_id, {}).get(table_type, 0)
        return received >= expected and expected > 0

    def _send_end_message(self, client_id, table_type):
        total_expected = self.chunks_to_receive.get(client_id, {}).get(table_type, 0)
        total_processed = self.chunks_processed_per_client.get(client_id, {}).get(table_type, 0)
        logging.info(
            f"action: sending_end_message | type:{self.aggregator_type} | cli_id:{client_id} "
            f"| file_type:{table_type.name} | total_chunks:{total_processed} | expected_chunks:{total_expected}"
        )

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

        try:
            if client_id in self.end_message_received and table_type in self.end_message_received[client_id]:
                del self.end_message_received[client_id][table_type]
                if not self.end_message_received[client_id]:
                    del self.end_message_received[client_id]

            if client_id in self.chunks_received_per_client and table_type in self.chunks_received_per_client[client_id]:
                del self.chunks_received_per_client[client_id][table_type]
                if not self.chunks_received_per_client[client_id]:
                    del self.chunks_received_per_client[client_id]

            if client_id in self.chunks_processed_per_client and table_type in self.chunks_processed_per_client[client_id]:
                del self.chunks_processed_per_client[client_id][table_type]
                if not self.chunks_processed_per_client[client_id]:
                    del self.chunks_processed_per_client[client_id]

            if client_id in self.chunks_to_receive and table_type in self.chunks_to_receive[client_id]:
                del self.chunks_to_receive[client_id][table_type]
                if not self.chunks_to_receive[client_id]:
                    del self.chunks_to_receive[client_id]

            stats_key = (client_id, table_type)
            if stats_key in self.initial_stats_sent:
                del self.initial_stats_sent[stats_key]

            logging.info(
                f"action: cleanup_state | client_id:{client_id} | table_type:{table_type} | aggregator_id:{self.aggregator_id}"
            )
        except KeyError:
            pass

    # ------------------------------------------------------------------
    # Aggregation logic (unchanged)
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
