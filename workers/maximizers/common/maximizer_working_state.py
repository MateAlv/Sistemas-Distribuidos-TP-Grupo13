from collections import defaultdict
from utils.tolerance.working_state import WorkingState

def default_top3_value():
    return defaultdict(list)

class MaximizerWorkingState(WorkingState):
    def __init__(self):
        super().__init__()
        self.clients_end_processed = set()
        self.processed_ids = set()
        
        # MAX type state
        self.sellings_max = defaultdict(dict)   # client_id -> {(item_id, month): quantity}
        self.profit_max = defaultdict(dict)     # client_id -> {(item_id, month): subtotal}
        self.partial_ranges_seen = defaultdict(set)  # client_id -> {range_id}
        self.partial_end_counts = defaultdict(int)   # client_id -> cantidad de END recibidos
        
        # TOP3 type state
        self.top3_by_store = defaultdict(default_top3_value)  # client_id -> store_id -> heap[(count, user_id)]
        self.partial_top3_finished = defaultdict(int)  # client_id -> cantidad de END recibidos

    def is_processed(self, message_id):
        return message_id in self.processed_ids
    
    def delete_all_data(self, maximizer_type, is_absolute):
        if maximizer_type == "MAX":
            self.sellings_max.clear()
            self.profit_max.clear()
            if is_absolute:
                self.partial_ranges_seen.clear()
                self.partial_end_counts.clear()
        elif maximizer_type == "TOP3":
            self.top3_by_store.clear()
            if is_absolute:
                self.partial_top3_finished.clear()

    def mark_processed(self, message_id):
        self.processed_ids.add(message_id)

    def is_client_end_processed(self, client_id):
        return client_id in self.clients_end_processed

    def mark_client_end_processed(self, client_id):
        self.clients_end_processed.add(client_id)

    def unmark_client_end_processed(self, client_id):
        self.clients_end_processed.discard(client_id)

    # MAX methods
    def get_sellings_max(self, client_id):
        return self.sellings_max[client_id]

    def get_profit_max(self, client_id):
        return self.profit_max[client_id]

    def get_partial_ranges_seen(self, client_id):
        return self.partial_ranges_seen[client_id]

    def add_partial_range_seen(self, client_id, range_id):
        self.partial_ranges_seen[client_id].add(range_id)

    def get_partial_end_count(self, client_id):
        return self.partial_end_counts[client_id]

    def increment_partial_end_count(self, client_id):
        self.partial_end_counts[client_id] += 1
        return self.partial_end_counts[client_id]

    # TOP3 methods
    def get_top3_by_store(self, client_id):
        return self.top3_by_store[client_id]

    def get_partial_top3_finished_count(self, client_id):
        return self.partial_top3_finished[client_id]

    def increment_partial_top3_finished(self, client_id):
        self.partial_top3_finished[client_id] += 1
        return self.partial_top3_finished[client_id]

    def delete_client_data(self, client_id, maximizer_type, is_absolute):
        if maximizer_type == "MAX":
            if client_id in self.sellings_max:
                del self.sellings_max[client_id]
            if client_id in self.profit_max:
                del self.profit_max[client_id]
            if is_absolute:
                self.partial_ranges_seen.pop(client_id, None)
                self.partial_end_counts.pop(client_id, None)
        elif maximizer_type == "TOP3":
            self.top3_by_store.pop(client_id, None)
            if is_absolute:
                self.partial_top3_finished.pop(client_id, None)
