from collections import defaultdict
from utils.tolerance.working_state import WorkingState
import logging
import threading

def default_product_value():
    return {"quantity": 0, "subtotal": 0.0}

def default_purchase_value():
    return defaultdict(int)

class AggregatorWorkingState(WorkingState):
    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self.end_message_received = {}
        self.chunks_received_per_client = {}
        self.chunks_processed_per_client = {}
        self.accumulated_chunks_per_client = {}
        self.chunks_to_receive = {}
        self.already_sent_stats = {}
        self.last_stats_sent_time = {}
        self.global_accumulator = {}
        self.processed_ids = set()
        # Publish/idempotency flags
        self.results_sent = set()  # (client_id, table_type)
        self.end_sent = set()      # (client_id, table_type)
        # Track processed ids per client for pruning
        self.processed_ids_per_client = {}

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
    
    def force_delete_client_stats_data(self, client_id):
        logging.info(f"action: deleting_client_stats_data | cli_id:{client_id}")
        try:
            with self._lock:
                if client_id == -1:
                    self.end_message_received = {}
                    self.chunks_received_per_client = {}
                    self.chunks_processed_per_client = {}
                    self.accumulated_chunks_per_client = {}
                    self.chunks_to_receive = {}
                    self.already_sent_stats = {}
                    self.last_stats_sent_time = {}
                    self.global_accumulator = {}
                    self.processed_ids = set()
                else:
                    if client_id in self.end_message_received:
                        del self.end_message_received[client_id]
                    if client_id in self.chunks_received_per_client:
                        del self.chunks_received_per_client[client_id]
                    if client_id in self.chunks_processed_per_client:
                        del self.chunks_processed_per_client[client_id]
                    if client_id in self.accumulated_chunks_per_client:
                        del self.accumulated_chunks_per_client[client_id]
                    if client_id in self.chunks_to_receive:
                        del self.chunks_to_receive[client_id]
                    keys_to_delete = [key for key in self.already_sent_stats if key[0] == client_id]
                    for key in keys_to_delete:
                        del self.already_sent_stats[key]
                    keys_to_delete = [key for key in self.last_stats_sent_time if key[0] == client_id]
                    for key in keys_to_delete:
                        del self.last_stats_sent_time[key]
                    if client_id in self.global_accumulator:
                        del self.global_accumulator[client_id]

                logging.info(f"action: client_stats_data_deleted | cli_id:{client_id}")
        except KeyError:
            pass

    def _get_aggregator_value(self, dictionary, client_id, table_type, aggregator_id):
        return dictionary.get(client_id, {}).get(table_type, {}).get(aggregator_id, 0)

    def _set_aggregator_value(self, dictionary, client_id, table_type, aggregator_id, value):
        self._ensure_client_table_entry(dictionary, client_id, table_type)
        dictionary[client_id][table_type][aggregator_id] = value

    def _increment_aggregator_value(self, dictionary, client_id, table_type, aggregator_id, delta):
        with self._lock:
            current = self._get_aggregator_value(dictionary, client_id, table_type, aggregator_id)
            self._set_aggregator_value(dictionary, client_id, table_type, aggregator_id, current + delta)

    def _sum_counts(self, dictionary, client_id, table_type):
        with self._lock:
            return sum(dictionary.get(client_id, {}).get(table_type, {}).values())

    def _ensure_global_entry(self, client_id):
        if client_id not in self.global_accumulator:
            self.global_accumulator[client_id] = {}

    # Public methods to replace direct access
    def mark_end_message_received(self, client_id, table_type):
        with self._lock:
            self._ensure_dict_entry(self.end_message_received, client_id, table_type, default=False)
            self.end_message_received[client_id][table_type] = True

    def is_end_message_received(self, client_id, table_type):
        with self._lock:
            return self.end_message_received.get(client_id, {}).get(table_type, False)

    def set_chunks_to_receive(self, client_id, table_type, total_expected):
        with self._lock:
            self._ensure_dict_entry(self.chunks_to_receive, client_id, table_type)
            self.chunks_to_receive[client_id][table_type] = total_expected

    def get_chunks_to_receive(self, client_id, table_type):
        with self._lock:
            return self.chunks_to_receive.get(client_id, {}).get(table_type)

    def update_chunks_received(self, client_id, table_type, aggregator_id, value):
        with self._lock:
        # Get current value and only update if the new value is different (from another aggregator)
            current = self._get_aggregator_value(self.chunks_received_per_client, client_id, table_type, aggregator_id)
            if value != current:
                self._set_aggregator_value(self.chunks_received_per_client, client_id, table_type, aggregator_id, value)

    def update_chunks_processed(self, client_id, table_type, aggregator_id, value):
        # Get current value and only update if the new value is different (from another aggregator)
        with self._lock:
            current = self._get_aggregator_value(self.chunks_processed_per_client, client_id, table_type, aggregator_id)
            if value != current:
                self._set_aggregator_value(self.chunks_processed_per_client, client_id, table_type, aggregator_id, value)

    def increment_chunks_received(self, client_id, table_type, aggregator_id, delta=1):
        self._increment_aggregator_value(self.chunks_received_per_client, client_id, table_type, aggregator_id, delta)

    def increment_chunks_processed(self, client_id, table_type, aggregator_id, delta=1):
        self._increment_aggregator_value(self.chunks_processed_per_client, client_id, table_type, aggregator_id, delta)

    def increment_accumulated_chunks(self, client_id, table_type, aggregator_id, delta=1):
        self._increment_aggregator_value(self.accumulated_chunks_per_client, client_id, table_type, aggregator_id, delta)

    def get_total_received(self, client_id, table_type):
        return self._sum_counts(self.chunks_received_per_client, client_id, table_type)

    def get_total_processed(self, client_id, table_type):
        return self._sum_counts(self.chunks_processed_per_client, client_id, table_type)

    def get_total_accumulated(self, client_id, table_type):
        return self._sum_counts(self.accumulated_chunks_per_client, client_id, table_type)

    def get_received_for_aggregator(self, client_id, table_type, aggregator_id):
        return self._get_aggregator_value(self.chunks_received_per_client, client_id, table_type, aggregator_id)

    def get_processed_for_aggregator(self, client_id, table_type, aggregator_id):
        return self._get_aggregator_value(self.chunks_processed_per_client, client_id, table_type, aggregator_id)

    def set_global_total_expected(self, client_id, table_type, total):
        with self._lock:
            self._ensure_dict_entry(self.chunks_to_receive, client_id, table_type)
            self.chunks_to_receive[client_id][table_type] = total

    def get_global_total_expected(self, client_id, table_type):
        return self.chunks_to_receive.get(client_id, {}).get(table_type)

    def get_total_processed_global(self, client_id, table_type):
        return self._sum_counts(self.chunks_processed_per_client, client_id, table_type)

    def get_leader_id(self, client_id, table_type, default_id):
        with self._lock:
            aggregators = self.chunks_received_per_client.get(client_id, {}).get(table_type, {}).keys()
            return min(aggregators) if aggregators else default_id

    def was_stats_sent(self, client_id, table_type, current_stats):
        with self._lock:
            return self.already_sent_stats.get((client_id, table_type)) == current_stats

    def mark_stats_sent(self, client_id, table_type, current_stats):
        with self._lock:
            self.already_sent_stats[(client_id, table_type)] = current_stats

    def is_processed(self, message_id):
        return message_id in self.processed_ids

    def mark_processed(self, message_id, client_id=None):
        self.processed_ids.add(message_id)
        if client_id is not None:
            self.processed_ids_per_client.setdefault(client_id, set()).add(message_id)

    def get_product_accumulator(self, client_id):
        with self._lock:
            self._ensure_global_entry(client_id)
            return self.global_accumulator[client_id].setdefault(
                "products",
                defaultdict(default_product_value),
            )

    def get_purchase_accumulator(self, client_id):
        with self._lock:
            self._ensure_global_entry(client_id)
            return self.global_accumulator[client_id].setdefault(
                "purchases", defaultdict(default_purchase_value)
            )

    def get_tpv_accumulator(self, client_id):
        with self._lock:
            self._ensure_global_entry(client_id)
            return self.global_accumulator[client_id].setdefault("tpv", defaultdict(float))

    def get_last_stats_sent_time(self, client_id, table_type):
        with self._lock:
            return self.last_stats_sent_time.get((client_id, table_type), 0.0)

    def set_last_stats_sent_time(self, client_id, table_type, time_val):
        with self._lock:
            self.last_stats_sent_time[(client_id, table_type)] = time_val

    def delete_client_data(self, client_id, table_type, accumulator_key):
        with self._lock:
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

            if (client_id, table_type) in self.last_stats_sent_time:
                del self.last_stats_sent_time[(client_id, table_type)]

            if (
                client_id in self.global_accumulator
                and accumulator_key in self.global_accumulator[client_id]
            ):
                if not self.global_accumulator[client_id]:
                    del self.global_accumulator[client_id]

        # Clear publish flags
        self.results_sent = {k for k in self.results_sent if k[0] != client_id or k[1] != table_type}
        self.end_sent = {k for k in self.end_sent if k[0] != client_id or k[1] != table_type}
        # Prune processed ids for this client if tracked
        if client_id in self.processed_ids_per_client:
            to_remove = self.processed_ids_per_client[client_id]
            self.processed_ids -= to_remove
            del self.processed_ids_per_client[client_id]

    # Publish flags helpers
    def mark_results_sent(self, client_id, table_type):
        self.results_sent.add((client_id, table_type))

    def results_already_sent(self, client_id, table_type):
        return (client_id, table_type) in self.results_sent

    def mark_end_sent(self, client_id, table_type):
        self.end_sent.add((client_id, table_type))

    def end_already_sent(self, client_id, table_type):
        return (client_id, table_type) in self.end_sent

    def get_active_clients_and_tables(self):
        with self._lock:
            active = []
            for client_id, tables in self.chunks_received_per_client.items():
                for table_type in tables.keys():
                    active.append((client_id, table_type))
            return active
