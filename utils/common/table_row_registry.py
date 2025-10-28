from utils.file_utils.table_type import ResultTableType, TableType
from utils.processing.process_table import *
from utils.file_utils.file_table import *
from utils.results.result_table import *

PROCESS_CLASSES = {
    TableType.TRANSACTIONS: TransactionsProcessRow,
    TableType.TRANSACTION_ITEMS: TransactionItemsProcessRow,
    TableType.MENU_ITEMS: MenuItemsProcessRow,
    TableType.STORES: StoresProcessRow,
    TableType.USERS: UsersProcessRow,
    TableType.PURCHASES_PER_USER_STORE: PurchasesPerUserStoreRow,
    TableType.TPV: TPVProcessRow,
}

FILE_CLASSES = {
    TableType.TRANSACTIONS: TransactionsFileRow,
    TableType.TRANSACTION_ITEMS: TransactionsItemsFileRow,
    TableType.MENU_ITEMS: MenuItemsFileRow,
    TableType.STORES: StoresFileRow,
    TableType.USERS: UsersFileRow,
}

RESULT_CLASSES = {
    ResultTableType.QUERY_1: Query1ResultRow,
    ResultTableType.QUERY_2_1: Query2_1ResultRow,
    ResultTableType.QUERY_2_2: Query2_2ResultRow,
    ResultTableType.QUERY_3: Query3ResultRow,
    ResultTableType.QUERY_4: Query4ResultRow,
}

class TableRowRegistry:
    @staticmethod
    def get_process_class(table_type: TableType):
        if table_type in PROCESS_CLASSES:
            return PROCESS_CLASSES[table_type]
        raise ValueError(f"No se encontró la clase de procesamiento para el tipo de tabla: {table_type}")

    @staticmethod
    def get_file_class(table_type: TableType):
        if table_type in FILE_CLASSES:
            return FILE_CLASSES[table_type]
        raise ValueError(f"No se encontró la clase de archivo para el tipo de tabla: {table_type}")
    
    @staticmethod
    def get_result_class(result_table_type: ResultTableType):
        if result_table_type in RESULT_CLASSES:
            return RESULT_CLASSES[result_table_type]
        raise ValueError(f"No se encontró la clase de resultado para el tipo de tabla: {result_table_type}")