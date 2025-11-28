import logging

from .filter_stats_messages import FilterStatsMessage

def _ensure_dict_entry(dictionary, client_id, table_type, default=0):
    if client_id not in dictionary:
        dictionary[client_id] = {}
    if table_type not in dictionary[client_id]:
        dictionary[client_id][table_type] = default

class FilterWorkingState:
    def __init__(self):
        # métricas por cliente
        self.number_of_chunks_received_per_client = {}
        self.number_of_chunks_not_sent_per_client = {}
        self.number_of_chunks_to_receive = {}
        # mensajes de fin recibidos por cliente y tipo de tabla
        self.end_message_received = {}
        # estadísticas ya enviadas por cliente y tipo de tabla
        self.already_sent_stats = {}

    def get_total_chunks_received(self, client_id, table_type):
        """Obtiene el número total de chunks recibidos para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type)
        return self.number_of_chunks_received_per_client[client_id][table_type]

    def get_total_not_sent_chunks(self, client_id, table_type):
        """Obtiene el número de chunks no enviados para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type)
        return self.number_of_chunks_not_sent_per_client[client_id][table_type]

    def get_total_chunks_to_receive(self, client_id, table_type):
        """Obtiene el número total de chunks esperados para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_to_receive, client_id, table_type)
        return self.number_of_chunks_to_receive[client_id][table_type]

    def stats_are_already_sent(self, client_id, table_type):
        """Verifica si las estadísticas ya fueron enviadas para un cliente y tipo de tabla"""
        return (client_id, table_type) in self.already_sent_stats

    def stats_sent(self, client_id, table_type):
        """Marca las estadísticas como enviadas para un cliente y tipo de tabla"""
        self.already_sent_stats[(client_id, table_type)] = True

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
        logging.debug(f"Count: {self.number_of_chunks_received_per_client[client_id][table_type]} | cli_id:{client_id}")
        return total_expected == self.number_of_chunks_received_per_client[client_id][table_type] and filter_id == 1

    def increase_received_chunks(self, client_id, table_type, count):
        """Incrementa el número de chunks recibidos para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type, default=0)
        self.number_of_chunks_received_per_client[client_id][table_type] += count

    def increase_not_sent_chunks(self, client_id, table_type, count):
        """Incrementa el número de chunks no enviados para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type, default=0)
        self.number_of_chunks_not_sent_per_client[client_id][table_type] += count

    def set_total_chunks_expected(self, client_id, table_type, total_expected):
        """Establece el número total de chunks esperados para un cliente y tipo de tabla"""
        if client_id not in self.number_of_chunks_to_receive:
            self.number_of_chunks_to_receive[client_id] = {}
        self.number_of_chunks_to_receive[client_id][table_type] = total_expected

    def update_stats_received(self, client_id, table_type, stats: FilterStatsMessage):
        """Actualiza el número de chunks recibidos para un cliente y tipo de tabla"""
        _ensure_dict_entry(self.number_of_chunks_to_receive, client_id, table_type, default=False)
        self.number_of_chunks_to_receive[client_id][table_type] = stats.total_expected

        _ensure_dict_entry(self.number_of_chunks_received_per_client, client_id, table_type, default=0)
        self.number_of_chunks_received_per_client[client_id][table_type] += stats.chunks_received

        _ensure_dict_entry(self.number_of_chunks_not_sent_per_client, client_id, table_type, default=0)
        self.number_of_chunks_not_sent_per_client[client_id][table_type] += stats.chunks_not_sent

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

            if stats_end.client_id in self.number_of_chunks_received_per_client:
                if stats_end.table_type in self.number_of_chunks_received_per_client[stats_end.client_id]:
                    del self.number_of_chunks_received_per_client[stats_end.client_id][stats_end.table_type]
                if not self.number_of_chunks_received_per_client[stats_end.client_id]:
                    del self.number_of_chunks_received_per_client[stats_end.client_id]

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
            "number_of_chunks_received_per_client",
            "number_of_chunks_not_sent_per_client",
            "number_of_chunks_to_receive",
            "end_message_received",
            "already_sent_stats"
        ]:
            try:
                obj = getattr(self, attr, None)
                if isinstance(obj, (dict, list, set)) and hasattr(obj, "clear"):
                    obj.clear()
            except (OSError, RuntimeError, AttributeError):
                pass