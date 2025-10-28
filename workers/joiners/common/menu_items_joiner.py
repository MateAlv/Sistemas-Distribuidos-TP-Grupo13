from middleware.middleware_interface import MessageMiddlewareQueue
from utils.eof_protocol.end_messages import MessageQueryEnd
from utils.file_utils.table_type import ResultTableType
from utils.processing.process_table import TableProcessRow
from utils.results.result_chunk import ResultChunkHeader, ResultChunk
from utils.results.result_table import Query2_1ResultRow, Query2_2ResultRow
from common.joiner import Joiner
import logging

class MenuItemsJoiner(Joiner):

    def define_queues(self):
        self.data_receiver = MessageMiddlewareQueue("rabbitmq", "to_transaction_items_to_join")
        self.data_join_receiver = MessageMiddlewareQueue("rabbitmq", "to_join_menu_items")
        self.data_sender = MessageMiddlewareQueue("rabbitmq", "to_merge_data")

    def save_data_join_fields(self, row, client_id):
        self.joiner_data[client_id][row.item_id] = row.name

    def join_result(self, row: TableProcessRow, client_id):
        result = {
            "item_id": row.item_id,
            "item_name": self.joiner_data[client_id].get(row.item_id, "UNKNOWN"),
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

    def publish_results(self, client_id):
        sellings_results = []
        profit_results = []
        joiner_results = self.joiner_results.get(client_id, [])
        for row in joiner_results:
            # INCLUIR CLIENT_ID EN LOS RESULTADOS
            row["client_id"] = client_id
            item_id = row["item_id"]  # Agregar esta línea
            item_name = row["item_name"]
            year_month = row["month_year"]
            if row["quantity"] is not None:
                sellings_quantity = row["quantity"]
                max_selling = Query2_1ResultRow(item_id, item_name, sellings_quantity, year_month)
                sellings_results.append(max_selling)
            if row["subtotal"] is not None:
                profit_sum = row["subtotal"]
                max_profit = Query2_2ResultRow(item_id, item_name, profit_sum, year_month)
                profit_results.append(max_profit)

        if not sellings_results and not profit_results:
            logging.info(f"action: no_results_to_send | type:{self.joiner_type} | client_id:{client_id}")
            return

        # Enviar a cola específica del cliente
        client_queue = MessageMiddlewareQueue("rabbitmq", f"to_merge_data_{client_id}")

        if sellings_results:
            sellings_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_1)
            sellings_chunk = ResultChunk(sellings_header, sellings_results)
            client_queue.send(sellings_chunk.serialize())
            logging.info(f"action: sent_selling_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(sellings_results)}")

        if profit_results:
            profit_header = ResultChunkHeader(client_id, ResultTableType.QUERY_2_2)  # Corregir TableType
            profit_chunk = ResultChunk(profit_header, profit_results)
            client_queue.send(profit_chunk.serialize())
            logging.info(f"action: sent_profit_results | type:{self.joiner_type} | client_id:{client_id} | results:{len(profit_results)}")

        client_queue.close()
        logging.info(f"action: sent_result_message | type:{self.joiner_type} | client_id:{client_id}")