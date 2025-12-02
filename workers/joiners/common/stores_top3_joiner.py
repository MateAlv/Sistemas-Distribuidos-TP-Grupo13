import logging

from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageEnd
from utils.file_utils.table_type import TableType
from utils.processing.process_chunk import ProcessChunk, ProcessChunkHeader
from utils.processing.process_table import TableProcessRow, PurchasesPerUserStoreRow
from .joiner import Joiner


class StoresTop3Joiner(Joiner):

    def define_queues(self):
        # Recibe del TOP3 absoluto
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_purchases_joiner")
        # Recibe datos de stores del servidor
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "stores_for_top3_joiner")
        # Envía al UsersJoiner
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_join_with_users")

    def save_data_join_fields(self, row, client_id):
        # Guarda mapping store_id -> store_name
        if hasattr(row, 'store_id') and hasattr(row, 'store_name'):
            self.working_state_join.add_join_data(client_id, row.store_id, row.store_name)
            logging.debug(f"action: save_store_data | store_id:{row.store_id} | store_name:{row.store_name}")

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

        logging.info(f"action: saved_stores_join_data | type:{self.joiner_type} | client_id:{client_id} | stores_loaded:{self.working_state_join.get_all_join_data(client_id)}")
        return True

    def join_result(self, row: TableProcessRow, client_id):
        logging.debug(f"action: starting_join_result | type:{self.joiner_type} | client_id:{client_id} | row_type:{type(row)}")

        # Procesar PurchasesPerUserStoreRow del TOP3 absoluto
        if isinstance(row, PurchasesPerUserStoreRow):
            try:
                store_id = row.store_id
                logging.debug(f"action: looking_up_store | type:{self.joiner_type} | client_id:{client_id} | store_id:{store_id}")

                store_name = self.working_state_join.get_join_data(client_id, store_id)
                if store_name is None:
                    logging.error(f"action: missing_joiner_data_for_client | type:{self.joiner_type} | client_id:{client_id} | store_id:{store_id}")
                    store_name = f"NO_JOINER_DATA_{store_id}"
                else:
                    logging.debug(f"action: store_lookup_result | type:{self.joiner_type} | client_id:{client_id} | store_id:{store_id} | store_name:{store_name}")

                # Crear nueva fila con store_name llenado
                joined_row = PurchasesPerUserStoreRow(
                    store_id=store_id,
                    store_name=store_name,
                    user_id=row.user_id,
                    user_birthdate=row.user_birthdate,
                    purchases_made=row.purchases_made
                )

                logging.debug(f"action: joined_store_data | store_id:{store_id} | store_name:{store_name} | user_id:{row.user_id} | purchases:{row.purchases_made}")
                return joined_row
            except Exception as join_error:
                logging.error(f"action: error_in_join_result | type:{self.joiner_type} | client_id:{client_id} | error:{join_error} | error_type:{type(join_error).__name__}")
                raise join_error
        else:
            logging.warning(f"action: unexpected_row_type | expected:PurchasesPerUserStoreRow | got:{type(row)}")
            return row

    def send_end_query_msg(self, client_id):
        # Envía END message al UsersJoiner
        try:
            end_msg = MessageEnd(client_id, TableType.PURCHASES_PER_USER_STORE, 1)
            self.data_sender.send(end_msg.encode())
            logging.info(f"action: sent_end_to_users_joiner | client_id:{client_id}")
        except Exception as e:
            logging.error(f"action: error_sending_end_to_users_joiner | error:{e}")

    def publish_results(self, client_id):
        # Envía PurchasesPerUserStoreRow con store_name llenado al UsersJoiner
        logging.debug(f"action: starting_publish_results | type:{self.joiner_type} | client_id:{client_id}")

        joiner_results = self.working_state_main.get_results(client_id)
        logging.debug(f"action: got_joiner_results | type:{self.joiner_type} | client_id:{client_id} | results_count:{len(joiner_results)}")

        if joiner_results:
            try:
                # Crear chunk con las filas que tienen store_name llenado
                logging.debug(f"action: creating_chunk_header | type:{self.joiner_type} | client_id:{client_id}")
                header = ProcessChunkHeader(client_id, TableType.PURCHASES_PER_USER_STORE)

                logging.debug(f"action: creating_chunk | type:{self.joiner_type} | client_id:{client_id}")
                chunk = ProcessChunk(header, joiner_results)

                logging.debug(f"action: sending_chunk | type:{self.joiner_type} | client_id:{client_id}")
                self.data_sender.send(chunk.serialize())

                logging.info(f"action: sent_to_users_joiner | type:{self.joiner_type} | client_id:{client_id} | rows:{len(joiner_results)}")
            except Exception as publish_error:
                logging.error(f"action: error_publishing_results | type:{self.joiner_type} | client_id:{client_id} | error:{publish_error} | error_type:{type(publish_error).__name__}")
                raise publish_error
        else:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")

    def run(self):
        from .joiner import Joiner
        Joiner.run(self)

    def shutdown(self, signum=None, frame=None):
        # Delegate to base shutdown to ensure signal handling exists
        super().shutdown(signum, frame)
