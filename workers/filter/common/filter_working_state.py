import logging

from utils.tolerance.working_state import WorkingState
from .filter_stats_messages import FilterStatsMessage

def _ensure_dict_entry(dictionary, client_id, table_type, default=0):
    if client_id not in dictionary:
        dictionary[client_id] = {}
    if table_type not in dictionary[client_id]:
        dictionary[client_id][table_type] = default

class FilterWorkingState(WorkingState):
    def __init__(self):
        super().__init__()
        # Track stats per filter_id, similar to aggregators
        # Structure: {client_id: {table_type: {filter_id: chunks_received}}}
        self.chunks_received_per_filter = {}
        self.chunks_not_sent_per_filter = {}
        self.number_of_chunks_to_receive = {}
        # mensajes de fin recibidos por cliente y tipo de tabla
        self.end_message_received = {}
        # estadísticas ya enviadas por cliente y tipo de tabla
        self.already_sent_stats = {}
        # processed message ids
        self.processed_ids = set()

    def _ensure_filter_entry(self, dictionary, client_id, table_type, filter_id, default=0):
        """Ensure nested dict structure exists for filter tracking"""
        if client_id not in dictionary:
            dictionary[client_id] = {}
        if table_type not in dictionary[client_id]:
            dictionary[client_id][table_type] = {}
        if filter_id not in dictionary[client_id][table_type]:
            dictionary[client_id][table_type][filter_id] = default

    def _sum_all_filters(self, dictionary, client_id, table_type):
        """Sum values across all filters for a given client/table"""
        return sum(dictionary.get(client_id, {}).get(table_type, {}).values())

    def get_total_chunks_received(self, client_id, table_type):
        """Obtiene el número total de chunks recibidos para un cliente y tipo de tabla (suma de todos los filtros)"""
        return self._sum_all_filters(self.chunks_received_per_filter, client_id, table_type)

    def get_total_not_sent_chunks(self, client_id, table_type):
        """Obtiene el número de chunks no enviados para un cliente y tipo de tabla (suma de todos los filtros)"""
        return self._sum_all_filters(self.chunks_not_sent_per_filter, client_id, table_type)

    def get_own_chunks_received(self, client_id, table_type, filter_id):
        """Obtiene el número de chunks recibidos SOLO para este filtro específico"""
        return self.chunks_received_per_filter.get(client_id, {}).get(table_type, {}).get(filter_id, 0)

    def get_own_chunks_not_sent(self, client_id, table_type, filter_id):
        """Obtiene el número de chunks no enviados SOLO para este filtro específico"""
        return self.chunks_not_sent_per_filter.get(client_id, {}).get(table_type, {}).get(filter_id, 0)

    def get_total_chunks_to_receive(self, client_id, table_type):
        """Obtiene el número total de chunks esperados para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_to_receive, client_id, table_type)
        return self.number_of_chunks_to_receive[client_id][table_type]

    def should_send_stats(self, client_id, table_type, current_received, current_not_sent):
        """Verifica si las estadísticas deben enviarse (no se enviaron o los valores cambiaron)"""
        key = (client_id, table_type)
        if key not in self.already_sent_stats:
            return True  # Nunca se enviaron
        
        last_received, last_not_sent = self.already_sent_stats[key]
        # Solo enviar si los valores cambiaron
        return (current_received != last_received) or (current_not_sent != last_not_sent)

    def mark_stats_sent(self, client_id, table_type, chunks_received, chunks_not_sent):
        """Marca las estadísticas como enviadas con los valores actuales"""
        self.already_sent_stats[(client_id, table_type)] = (chunks_received, chunks_not_sent)

    def end_is_received(self, client_id, table_type):
        """Verifica si se recibió el mensaje de fin para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.end_message_received, client_id, table_type, default=False)
        return self.end_message_received[client_id][table_type]

    def end_received(self, client_id, table_type):
        """Marca que se recibió el mensaje de fin para un cliente y tipo de tabla"""
        if client_id not in self.end_message_received:
            self.end_message_received[client_id] = {}
        self.end_message_received[client_id][table_type] = True

    def can_send_end_message(self, client_id, table_type, total_expected, filter_id):
        """Verifica si se puede enviar el mensaje de fin para un cliente y tipo de tabla"""
        if client_id not in self.end_message_received:
            self.end_message_received[client_id] = {}
        
        total_processed = self.get_total_chunks_received(client_id, table_type) + self.get_total_not_sent_chunks(client_id, table_type)
        total_received = self.get_total_chunks_received(client_id, table_type)
        total_not_sent = self.get_total_not_sent_chunks(client_id, table_type)
        logging.debug(f"Count: {total_processed}/{total_expected} (received:{total_received}, not_sent:{total_not_sent}) | cli_id:{client_id}")
        # Cualquier instancia puede propagar END una vez que el total global coincide.
        return total_expected == total_processed

    def increase_received_chunks(self, client_id, table_type, filter_id, count):
        """Incrementa el número de chunks recibidos para un filtro específico"""
        self._ensure_filter_entry(self.chunks_received_per_filter, client_id, table_type, filter_id, default=0)
        self.chunks_received_per_filter[client_id][table_type][filter_id] += count

    def increase_not_sent_chunks(self, client_id, table_type, filter_id, count):
        """Incrementa el número de chunks no enviados para un filtro específico"""
        self._ensure_filter_entry(self.chunks_not_sent_per_filter, client_id, table_type, filter_id, default=0)
        self.chunks_not_sent_per_filter[client_id][table_type][filter_id] += count

    def set_total_chunks_expected(self, client_id, table_type, total_expected):
        """Establece el número total de chunks esperados para un cliente y tipo de tabla"""
        if client_id not in self.number_of_chunks_to_receive:
            self.number_of_chunks_to_receive[client_id] = {}
        self.number_of_chunks_to_receive[client_id][table_type] = total_expected

    def update_stats_received(self, client_id, table_type, stats: FilterStatsMessage):
        """Actualiza las estadísticas recibidas de otro filtro (tracking per-filter)"""
        _ensure_dict_entry(self.number_of_chunks_to_receive, client_id, table_type, default=False)
        self.number_of_chunks_to_receive[client_id][table_type] = stats.total_expected

        # Track this filter's stats separately
        filter_id = stats.filter_id
        self._ensure_filter_entry(self.chunks_received_per_filter, client_id, table_type, filter_id, default=0)
        self._ensure_filter_entry(self.chunks_not_sent_per_filter, client_id, table_type, filter_id, default=0)
        
        # Update only if the value changed (prevents resetting to 0)
        current_received = self.chunks_received_per_filter[client_id][table_type][filter_id]
        current_not_sent = self.chunks_not_sent_per_filter[client_id][table_type][filter_id]
        
        if stats.chunks_received != current_received:
            self.chunks_received_per_filter[client_id][table_type][filter_id] = stats.chunks_received
        if stats.chunks_not_sent != current_not_sent:
            self.chunks_not_sent_per_filter[client_id][table_type][filter_id] = stats.chunks_not_sent

    def delete_client_stats_data(self, stats_end):
        """Limpia datos del cliente después de procesar"""
        logging.info(f"action: deleting_client_stats_data | cli_id:{stats_end.client_id}")
        try:
            if stats_end.client_id in self.end_message_received:
                if stats_end.table_type in self.end_message_received[stats_end.client_id]:
                    del self.end_message_received[stats_end.client_id][stats_end.table_type]
                if not self.end_message_received[stats_end.client_id]:
                    del self.end_message_received[stats_end.client_id]

            if stats_end.client_id in self.number_of_chunks_to_receive:
                if stats_end.table_type in self.number_of_chunks_to_receive[stats_end.client_id]:
                    del self.number_of_chunks_to_receive[stats_end.client_id][stats_end.table_type]
                if not self.number_of_chunks_to_receive[stats_end.client_id]:
                    del self.number_of_chunks_to_receive[stats_end.client_id]

            if stats_end.client_id in self.chunks_received_per_filter:
                if stats_end.table_type in self.chunks_received_per_filter[stats_end.client_id]:
                    del self.chunks_received_per_filter[stats_end.client_id][stats_end.table_type]
                if not self.chunks_received_per_filter[stats_end.client_id]:
                    del self.chunks_received_per_filter[stats_end.client_id]

            if stats_end.client_id in self.chunks_not_sent_per_filter:
                if stats_end.table_type in self.chunks_not_sent_per_filter[stats_end.client_id]:
                    del self.chunks_not_sent_per_filter[stats_end.client_id][stats_end.table_type]
                if not self.chunks_not_sent_per_filter[stats_end.client_id]:
                    del self.chunks_not_sent_per_filter[stats_end.client_id]

            if stats_end.client_id in self.number_of_chunks_to_receive:
                if stats_end.table_type in self.number_of_chunks_to_receive[stats_end.client_id]:
                    del self.number_of_chunks_to_receive[stats_end.client_id][stats_end.table_type]
                if not self.number_of_chunks_to_receive[stats_end.client_id]:
                    del self.number_of_chunks_to_receive[stats_end.client_id]

            if (stats_end.client_id, stats_end.table_type) in self.already_sent_stats:
                del self.already_sent_stats[(stats_end.client_id, stats_end.table_type)]

            logging.info(f"action: client_stats_data_deleted | cli_id:{stats_end.client_id}")
        except KeyError:
            pass

    def destroy(self):
        """Limpia todos los datos del estado de trabajo"""
        for attr in [
            "chunks_received_per_filter",
            "chunks_not_sent_per_filter",
            "number_of_chunks_to_receive",
            "end_message_received",
            "already_sent_stats",
            "processed_ids"
        ]:
            try:
                obj = getattr(self, attr, None)
                if isinstance(obj, (dict, list, set)) and hasattr(obj, "clear"):
                    obj.clear()
            except (OSError, RuntimeError, AttributeError):
                pass

    def is_processed(self, message_id):
        return message_id in self.processed_ids

    def mark_processed(self, message_id):
        self.processed_ids.add(message_id)
