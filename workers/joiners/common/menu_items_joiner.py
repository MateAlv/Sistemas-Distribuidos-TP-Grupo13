from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageQueryEnd
from utils.file_utils.table_type import ResultTableType
from utils.processing.process_table import TableProcessRow
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.results.result_table import Query2_1ResultRow, Query2_2ResultRow
from .joiner import Joiner
import logging

class MenuItemsJoiner(Joiner):

    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")

    def save_data_join_fields(self, row, client_id):
        self.working_state_join.add_join_data(client_id, row.item_id, row.name)

    def join_result(self, row: TableProcessRow, client_id):
        item_name = self.working_state_join.get_join_data(client_id, row.item_id)
        if item_name is None:
            item_name = "UNKNOWN"
            
        result = {
            "item_id": row.item_id,
            "item_name": item_name,
            "quantity": row.quantity,
            "subtotal": row.subtotal,
            "month_year": row.month_year_created_at,
        }
        return result

    def send_end_query_msg(self, client_id):
        end_query_msg_1 = MessageQueryEnd(client_id, ResultTableType.QUERY_2_1, 1)
        end_query_msg_2 = MessageQueryEnd(client_id, ResultTableType.QUERY_2_2, 1)

        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")
        client_queue.send(end_query_msg_1.encode())
        client_queue.send(end_query_msg_2.encode())
        client_queue.close()


    def publish_results(self, client_id):
        sellings_results = {}
        profit_results = {}
        message_ids = set()
        joiner_results = self.working_state_main.get_results(client_id)
        
        for message_id, row in joiner_results.items():
            # INCLUIR CLIENT_ID EN LOS RESULTADOS
            if message_id not in sellings_results:
                sellings_results[message_id] = []
            if message_id not in profit_results:
                profit_results[message_id] = []

            row["client_id"] = client_id
            item_id = row["item_id"]  # Agregar esta línea
            item_name = row["item_name"]
            year_month = row["month_year"]
            if row["quantity"] is not None:
                sellings_quantity = row["quantity"]
                max_selling = Query2_1ResultRow(item_id, item_name, sellings_quantity, year_month)
                sellings_results[message_id].append(max_selling)
            if row["subtotal"] is not None:
                profit_sum = row["subtotal"]
                max_profit = Query2_2ResultRow(item_id, item_name, profit_sum, year_month)
                profit_results[message_id].append(max_profit)
            message_ids.add(message_id)

        if not sellings_results and not profit_results:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")
            return

        # Enviar a cola específica del cliente
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

        for message_id in message_ids:
            sellings_results_chunk = sellings_results.get(message_id, [])
            profit_results_chunk = profit_results.get(message_id, [])

            if sellings_results_chunk:
                sellings_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_1)
                sellings_chunk = ResultChunk(sellings_header, sellings_results_chunk)
                client_queue.send(sellings_chunk.serialize())
                logging.info(f"action: sent_selling_results | type:{self.joiner_type} | client_id:{client_id} | message_id:{message_id} | results:{len(sellings_results_chunk)}")

            if profit_results_chunk:
                profit_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_2)  # Corregir TableType
                profit_chunk = ResultChunk(profit_header, profit_results_chunk)
                client_queue.send(profit_chunk.serialize())
                logging.info(f"action: sent_profit_results | type:{self.joiner_type} | client_id:{client_id} | message_id:{message_id} | results:{len(profit_results_chunk)}")
        
            self.persistence_main.commit_send_ack(client_id, message_id)
        client_queue.close()
        logging.info(f"action: sent_result_message | type:{self.joiner_type} | client_id:{client_id}")

    def save_data_join(self, chunk) -> bool:
        """
        Guarda los datos de join (tabla de menú de items) enviados por el server.
        """
        client_id = chunk.client_id()
        rows = chunk.rows
        for row in rows:
            item_id = getattr(row, "item_id", None)
            item_name = getattr(row, "item_name", None)
            if item_id is None:
                logging.warning(f"action: invalid_menu_item_row | client_id:{client_id} | row:{row}")
                continue
            self.working_state_join.add_join_data(client_id, item_id, item_name or "")
        logging.info(f"action: saved_menu_items_join_data | type:{self.joiner_type} | client_id:{client_id} | items_loaded:{self.working_state_join.get_join_data_count(client_id)}")
        return True

    def shutdown(self, signum=None, frame=None):
        # Delegate to base shutdown for consistent signal handling
        super().shutdown(signum, frame)
