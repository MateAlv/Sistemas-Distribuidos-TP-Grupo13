import logging

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageQueryEnd
from utils.file_utils.table_type import ResultTableType
from utils.processing.process_table import TableProcessRow
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.results.result_table import Query3ResultRow
from .joiner import Joiner


class StoresTpvJoiner(Joiner):

    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_stores_tvp")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "stores_for_tpv_joiner")

    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.store_id] = row.store_name

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de stores).
        """
        client_id = chunk.client_id()
        rows = chunk.rows

        # Inicializar diccionario para este cliente si no existe
        if client_id not in self.joiner_data:
            self.joiner_data[client_id] = {}

        # Guardar mapping store_id → store_name
        for row in rows:
            if hasattr(row, 'store_id') and hasattr(row, 'store_name'):
                self.joiner_data[client_id][row.store_id] = row.store_name
                logging.debug(f"action: save_stores_join_data | type:{self.joiner_type} | store_id:{row.store_id} | name:{row.store_name}")
            else:
                logging.warning(f"action: invalid_stores_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_store_id:{hasattr(row, 'store_id')} | has_store_name:{hasattr(row, 'store_name')}")

        logging.info(f"action: saved_stores_join_data | type:{self.joiner_type} | client_id:{client_id} | stores_loaded:{len(self.joiner_data[client_id])}")
        return True

    def join_result(self, row: TableProcessRow, client_id):
        # Soporte para TPVProcessRow
        if hasattr(row, 'tpv') and hasattr(row, 'year_half'):
            # TPVProcessRow
            result = {
                "store_id": row.store_id,
                "store_name": self.joiner_data[client_id].get(row.store_id, "UNKNOWN"),
                "tpv": row.tpv,
                "year_half": row.year_half,
            }
        else:
            result = {
                "store_id": row.store_id,
                "store_name": self.joiner_data[client_id].get(row.store_id, "UNKNOWN"),
                "tpv": row.final_amount,
                "year_half": row.year_half_created_at,
            }
        return result

    def publish_results(self, client_id):
        joiner_results = self.joiner_results.get(client_id, [])
        query3_results = []
        for row in joiner_results:
            store_id = row["store_id"]
            store_name = row["store_name"]
            tpv = row["tpv"]
            year_half = row["year_half"]
            query3_result = Query3ResultRow(store_id, store_name, tpv, year_half)
            query3_results.append(query3_result)

        if query3_results:
            query3_header = ResultChunkHeader(client_id, ResultTableType.QUERY_3)
            query3_chunk = ResultChunk(query3_header, query3_results)

            # Enviar a cola específica del cliente
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            client_queue.send(query3_chunk.serialize())
            client_queue.close()
            logging.info(f"action: sent_result_message | type:{self.joiner_type} | client_id:{client_id}")
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")

    def send_end_query_msg(self, client_id):
        end_query_msg_3 = MessageQueryEnd(client_id, ResultTableType.QUERY_3, 1)
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        client_queue.send(end_query_msg_3.encode())
        client_queue.close()
        logging.info(f"action: sent_end_query_message | type:{self.joiner_type} | client_id:{client_id}")