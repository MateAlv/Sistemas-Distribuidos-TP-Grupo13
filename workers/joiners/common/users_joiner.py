import logging

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageQueryEnd
from utils.file_utils.table_type import ResultTableType
from utils.processing.process_table import TableProcessRow, PurchasesPerUserStoreRow
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.results.result_table import Query4ResultRow
from .joiner import Joiner


class UsersJoiner(Joiner):

    def define_queues(self):
        # Recibe del StoresTop3Joiner
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")
        # Recibe datos de users del servidor
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_users")
        # Envía resultados finales
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")

    def save_data_join_fields(self, row, client_id):
        # Guarda mapping user_id -> birthdate
        if hasattr(row, 'user_id') and hasattr(row, 'birthdate'):
            self.working_state_join.add_join_data(client_id, row.user_id, row.birthdate)
            logging.debug(f"action: save_user_data | user_id:{row.user_id} | birthdate:{row.birthdate}")

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos para la tabla base necesaria para el join (tabla de users).
        """
        client_id = chunk.client_id()
        rows = chunk.rows

        # Guardar mapping user_id → birthdate
        for row in rows:
            if hasattr(row, 'user_id') and hasattr(row, 'birthdate'):
                self.working_state_join.add_join_data(client_id, row.user_id, row.birthdate)
            else:
                logging.warning(f"action: invalid_users_join_row | type:{self.joiner_type} | row_type:{type(row)} | missing_fields | has_user_id:{hasattr(row, 'user_id')} | has_birthdate:{hasattr(row, 'birthdate')}")

        logging.info(f"action: saved_users_join_data | type:{self.joiner_type} | client_id:{client_id} | users_loaded:{self.working_state_join.get_join_data_count(client_id)}")
        return True

    def send_end_query_msg(self, client_id):
        # Envía END message final para Query 4
        try:
            end_query_msg = MessageQueryEnd(client_id, ResultTableType.QUERY_4, 1)
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            client_queue.send(end_query_msg.encode())
            client_queue.close()
            logging.info(f"action: sent_end_query_4 | client_id:{client_id}")
        except Exception as e:
            logging.error(f"action: error_sending_end_query_4 | error:{e}")

    def join_result(self, row: TableProcessRow, client_id):
        # Procesar PurchasesPerUserStoreRow del StoresTop3Joiner
        if isinstance(row, PurchasesPerUserStoreRow):
            user_id = row.user_id
            birthdate = self.working_state_join.get_join_data(client_id, user_id)

            if birthdate is None:
                logging.warning(f"action: user_not_found | user_id:{user_id} | using_placeholder")
                birthdate = "UNKNOWN"

            # Crear resultado final para Query 4
            result = Query4ResultRow(
                store_id=row.store_id,
                store_name=row.store_name,
                user_id=user_id,
                birthdate=birthdate,
                purchase_quantity=row.purchases_made
            )

            logging.debug(f"action: joined_user_data | store_id:{row.store_id} | store_name:{row.store_name} | user_id:{user_id} | birthdate:{birthdate} | purchases:{row.purchases_made}")
            return result
        else:
            logging.warning(f"action: unexpected_row_type | expected:PurchasesPerUserStoreRow | got:{type(row)}")
            return None

    def publish_results(self, client_id):
        # Envía resultados finales de Query 4
        joiner_results = self.working_state_main.get_results(client_id)

        # Filtrar resultados None
        query4_results = [result for result in joiner_results if result is not None]

        if query4_results:
            # Enviar a cola específica del cliente
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

            query4_header = ResultChunkHeader(client_id, ResultTableType.QUERY_4)
            query4_chunk = ResultChunk(query4_header, query4_results)

            client_queue.send(query4_chunk.serialize())
            client_queue.close()

            logging.info(f"action: sent_query4_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(query4_results)}")
        else:
            logging.info(f"action: no_query4_results_to_send | type:{self.joiner_type} | client_id:{client_id}")

    def send_force_end_msg(self, client_id):
        try:
            force_end_msg = MessageQueryEnd.force_end_message(client_id)
            client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
            client_queue.send(force_end_msg.encode())
            client_queue.close()
            logging.info(f"action: sent_force_end_query_4 | client_id:{client_id}")
        except Exception as e:
            logging.error(f"action: error_sending_force_end_query_4 | error:{e}")