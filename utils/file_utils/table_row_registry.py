from .table_type import TableType
from .process_table import *
from .file_table import *

PROCESS_CLASSES = {
    TableType.TRANSACTIONS: TransactionsProcessRow,
    TableType.TRANSACTIONS_ITEMS: TransactionsItemsProcessRow,
    TableType.MENU_ITEMS: MenuItemsProcessRow,
    TableType.STORES: StoresProcessRow,
    TableType.USERS: UsersProcessRow,
}

FILE_CLASSES = {
    TableType.TRANSACTIONS: TransactionsFileRow,
    TableType.TRANSACTIONS_ITEMS: TransactionsItemsFileRow,
    TableType.MENU_ITEMS: MenuItemsFileRow,
    TableType.STORES: StoresFileRow,
    TableType.USERS: UsersFileRow,
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
