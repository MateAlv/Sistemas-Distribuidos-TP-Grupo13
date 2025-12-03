import logging
import os
from collections import defaultdict

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageQueryEnd
from utils.file_utils.table_type import ResultTableType
from utils.processing.process_table import TableProcessRow
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.results.result_table import Query3ResultRow
from .joiner import Joiner


class StoresTpvJoiner(Joiner):

    def define_queues(self):
        # Recibe TPV agregados desde max_tpv (o directamente de aggs) en la cola global
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_absolute_tpv")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "stores_for_tpv_joiner")
        # Esperamos barreras de todos los shards de agg_tpv
        self.expected_shards = int(os.getenv("EXPECTED_INPUTS", "1"))
        self.received_shards = defaultdict(set)

    def save_data_join_fields(self, row, client_id):
        self.working_state_join.add_join_data(client_id, row.store_id, row.store_name)

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de stores).
        """
        client_id = chunk.client_id()
        rows = chunk.rows

        # Guardar mapping store_id → store_name
        for row in rows:
            if hasattr(row, 'store_id') and hasattr(row, 'store_name'):
                self.working_state_join.add_join_data(client_id, row.store_id, row.store_name)
                logging.debug(f"action: save_stores_join_data | type:{self.joiner_type} | store_id:{row.store_id} | store_name:{row.store_name}")
            else:
                logging.warning(f"action: invalid_stores_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_store_id:{hasattr(row, 'store_id')} | has_store_name:{hasattr(row, 'store_name')}")

        logging.info(f"action: saved_stores_join_data | type:{self.joiner_type} | client_id:{client_id} | stores_loaded:{self.working_state_join.get_join_data_count(client_id)}")
        return True

    def save_data(self, chunk) -> bool:
        """
        Guarda los datos para la tabla que debe joinearse.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        logging.debug(
            f"action: tpv_chunk_received | type:{self.joiner_type} | cli_id:{client_id} | rows:{len(rows)} | shards_seen:{len(self.received_shards.get(client_id, set()))}/{self.expected_shards}"
        )
        self.working_state_main.add_data(client_id, rows)
        self.working_state_main.add_chunk(client_id, chunk)
        return True

    def join_result(self, row: TableProcessRow, client_id):
        store_name = self.working_state_join.get_join_data(client_id, row.store_id)
        if store_name is None:
            store_name = "UNKNOWN"
            
        # Soporte para TPVProcessRow
        if hasattr(row, 'tpv') and hasattr(row, 'year_half'):
            # TPVProcessRow
            result = {
                "store_id": row.store_id,
                "store_name": store_name,
                "tpv": row.tpv,
                "year_half": row.year_half,
            }
        else:
            result = {
                "store_id": row.store_id,
                "store_name": store_name,
                "tpv": row.final_amount,
                "year_half": row.year_half_created_at,
            }
        return result

    def publish_results(self, client_id):
        joiner_results = self.working_state_main.get_results(client_id)
        query3_results = []
        for row in joiner_results:
            # Row is already aggregated by Maximizer
            query3_result = Query3ResultRow(row["store_id"], row["store_name"], row["tpv"], row["year_half"])
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

    def run(self):
        logging.info(f"Joiner iniciado. Tipo: {self.joiner_type}")
        self.handle_processing_recovery()
        self.data_handler_thread.start()
        self.join_data_handler_thread.start()

    def shutdown(self, signum=None, frame=None):
        # Delegate to base shutdown for consistent signal handling
        super().shutdown(signum, frame)

    def send_end_query_msg(self, client_id):
        end_query_msg_3 = MessageQueryEnd(client_id, ResultTableType.QUERY_3, 1)
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        client_queue.send(end_query_msg_3.encode())
        client_queue.close()
        logging.info(f"action: sent_end_query_message | type:{self.joiner_type} | client_id:{client_id}")
