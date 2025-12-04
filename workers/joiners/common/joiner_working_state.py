from utils.tolerance.working_state import WorkingState
import pickle

class JoinerMainWorkingState(WorkingState):
    def __init__(self):
        super().__init__()
        self.data = {}
        self.joiner_data_chunks = {}
        self.joiner_results = {}
        self.client_end_messages_received = []
        self.completed_clients = []
        self._pending_end_messages = []
        self.ready_to_join = {}
        self.processed_ids_main = set()
        
        # Multi-Sender Tracking
        self.finished_senders = {} # client_id -> set(sender_id)
        self.total_expected_chunks = {} # client_id -> total_chunks

    def is_processed(self, message_id):
        return message_id in self.processed_ids_main

    def add_processed_id(self, message_id):
        self.processed_ids_main.add(message_id)

    def add_data(self, client_id, rows):
        if client_id not in self.data:
            self.data[client_id] = []
        self.data[client_id].extend(rows)

    def add_chunk(self, client_id, chunk):
        if client_id not in self.joiner_data_chunks:
            self.joiner_data_chunks[client_id] = []
        self.joiner_data_chunks[client_id].append(chunk)

    def get_chunks(self, client_id):
        return self.joiner_data_chunks.get(client_id, [])

    def add_result(self, client_id, message_id, row):
        if client_id not in self.joiner_results:
            self.joiner_results[client_id] = {}
        if message_id not in self.joiner_results[client_id]:
            self.joiner_results[client_id][message_id] = []
        self.joiner_results[client_id][message_id].append(row)

    def get_results(self, client_id):
        return self.joiner_results.get(client_id, {})

    def mark_end_message_received(self, client_id):
        if client_id not in self.client_end_messages_received:
            self.client_end_messages_received.append(client_id)

    def is_end_message_received(self, client_id):
        return client_id in self.client_end_messages_received

    def mark_client_completed(self, client_id):
        self.completed_clients.append(client_id)

    def is_client_completed(self, client_id):
        return client_id in self.completed_clients

    def add_pending_end_message(self, client_id):
        self._pending_end_messages.append(client_id)

    def get_pending_end_messages(self):
        return self._pending_end_messages

    def clear_pending_end_messages(self):
        self._pending_end_messages.clear()

    def set_ready_to_join(self, client_id):
        self.ready_to_join[client_id] = True

    def is_ready_flag_set(self, client_id):
        return self.ready_to_join.get(client_id, False)

    def has_chunks(self, client_id):
        return client_id in self.joiner_data_chunks

    def clean_client_data(self, client_id):
        if client_id in self.joiner_data_chunks:
            del self.joiner_data_chunks[client_id]
        if client_id in self.data:
            del self.data[client_id]
        if client_id in self.joiner_results:
            del self.joiner_results[client_id]

    def delete_client_data(self, client_id: int):
        """Delete all data for a specific client (used by force-end)."""
        # Remove from all dictionaries and lists
        if client_id in self.joiner_data_chunks:
            del self.joiner_data_chunks[client_id]
        if client_id in self.data:
            del self.data[client_id]
        if client_id in self.joiner_results:
            del self.joiner_results[client_id]
        if client_id in self.ready_to_join:
            del self.ready_to_join[client_id]
        if client_id in self.finished_senders:
            del self.finished_senders[client_id]
        if client_id in self.total_expected_chunks:
            del self.total_expected_chunks[client_id]
        
        # Remove from lists
        if client_id in self.client_end_messages_received:
            self.client_end_messages_received.remove(client_id)
        if client_id in self.completed_clients:
            self.completed_clients.remove(client_id)
        if client_id in self._pending_end_messages:
            self._pending_end_messages.remove(client_id)
        
        # Note: processed_ids_main is not cleaned since it doesn't track client_id

    def reset(self):
        self.data.clear()
        self.joiner_data_chunks.clear()
        self.joiner_results.clear()
        self.client_end_messages_received.clear()
        self.completed_clients.clear()
        self._pending_end_messages.clear()
        self.ready_to_join.clear()

    def to_bytes(self):
        state = {
            "data": self.data,
            "joiner_data_chunks": self.joiner_data_chunks,
            "joiner_results": self.joiner_results,
            "client_end_messages_received": self.client_end_messages_received,
            "completed_clients": self.completed_clients,
            "_pending_end_messages": self._pending_end_messages,
            "ready_to_join": self.ready_to_join,
            "processed_ids_main": self.processed_ids_main,
            "finished_senders": self.finished_senders,
            "total_expected_chunks": self.total_expected_chunks
        }
        return pickle.dumps(state)

    @classmethod
    def from_bytes(cls, data):
        instance = cls()
        state = pickle.loads(data)
        instance.data = state.get("data", {})
        instance.joiner_data_chunks = state.get("joiner_data_chunks", {})
        instance.joiner_results = state.get("joiner_results", {})
        instance.client_end_messages_received = state.get("client_end_messages_received", [])
        instance.completed_clients = state.get("completed_clients", [])
        instance._pending_end_messages = state.get("_pending_end_messages", [])
        instance.ready_to_join = state.get("ready_to_join", {})
        instance.processed_ids_main = state.get("processed_ids_main", set())
        
        # Recover multi-sender state
        instance.finished_senders = state.get("finished_senders", {})
        instance.total_expected_chunks = state.get("total_expected_chunks", {})
        
        return instance

    # Multi-Sender Tracking Methods
    def is_sender_finished(self, client_id, sender_id):
        if client_id not in self.finished_senders:
            return False
        return sender_id in self.finished_senders[client_id]

    def mark_sender_finished(self, client_id, sender_id):
        if client_id not in self.finished_senders:
            self.finished_senders[client_id] = set()
        self.finished_senders[client_id].add(sender_id)

    def get_finished_senders_count(self, client_id):
        if client_id not in self.finished_senders:
            return 0
        return len(self.finished_senders[client_id])

    def add_expected_chunks(self, client_id, count):
        if client_id not in self.total_expected_chunks:
            self.total_expected_chunks[client_id] = 0
        self.total_expected_chunks[client_id] += count

    def get_total_expected_chunks(self, client_id):
        return self.total_expected_chunks.get(client_id, 0)


class JoinerJoinWorkingState(WorkingState):
    def __init__(self):
        super().__init__()
        self.joiner_data = {}
        self.processed_ids_join = set()

    def is_processed(self, message_id):
        return message_id in self.processed_ids_join

    def add_processed_id(self, message_id):
        self.processed_ids_join.add(message_id)

    def add_join_data(self, client_id, item_id, item_name):
        if client_id not in self.joiner_data:
            self.joiner_data[client_id] = {}
        self.joiner_data[client_id][item_id] = item_name

    def get_join_data(self, client_id, item_id):
        return self.joiner_data.get(client_id, {}).get(item_id)
    
    def get_all_join_data(self, client_id):
        return self.joiner_data.get(client_id, {})

    def get_join_data_count(self, client_id):
        return len(self.joiner_data.get(client_id, {}))

    def delete_client_data(self, client_id: int):
        """Delete all join data for a specific client (used by force-end)."""
        if client_id in self.joiner_data:
            del self.joiner_data[client_id]
        # Note: processed_ids_join is not cleaned since it doesn't track client_id

    def reset(self):
        self.joiner_data.clear()

    def to_bytes(self):
        state = {
            "joiner_data": self.joiner_data,
            "processed_ids_join": self.processed_ids_join
        }
        return pickle.dumps(state)

    @classmethod
    def from_bytes(cls, data):
        instance = cls()
        state = pickle.loads(data)
        instance.joiner_data = state.get("joiner_data", {})
        instance.processed_ids_join = state.get("processed_ids_join", set())
        return instance
