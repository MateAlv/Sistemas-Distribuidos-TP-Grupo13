import datetime
import TransactionsFileRow, TransactionsItemsFileRow, MenuItemsFileRow, StoresFileRow, UsersFileRow
import TransactionsProcessRow, TransactionsItemsProcessRow, MenuItemsProcessRow, StoresProcessRow, UsersProcessRow
from utils.file_utils.table_type import TableType

PROCESS_CLASSES = {
    TableType.TRANSACTIONS: lambda: TransactionsProcessRow,
    TableType.TRANSACTIONS_ITEMS: lambda: TransactionsItemsProcessRow,
    TableType.MENU_ITEMS: lambda: MenuItemsProcessRow,
    TableType.STORES: lambda: StoresProcessRow,
    TableType.USERS: lambda: UsersProcessRow,
}

FILE_CLASSES = {
    TableType.TRANSACTIONS: lambda: TransactionsFileRow,
    TableType.TRANSACTIONS_ITEMS: lambda: TransactionsItemsFileRow,
    TableType.MENU_ITEMS: lambda: MenuItemsFileRow,
    TableType.STORES: lambda: StoresFileRow,
    TableType.USERS: lambda: UsersFileRow,
}
    
class ProcessBatchHeader:

    def __init__(self, client_id: int, table_type: TableType, size: int = 0):
        self.client_id = client_id
        self.table_type = table_type
        self.size = size

    def serialize(self) -> bytes:
        # Serializa como 3 enteros de 4 bytes cada uno (big-endian)
        return (
            self.client_id.to_bytes(4, byteorder="big") +
            self.table_type.value.to_bytes(4, byteorder="big") +
            self.size.to_bytes(4, byteorder="big")
        )

    def header_length(self) -> int:
        """Get the length of the header in bytes."""
        return 12

    @staticmethod
    def deserialize(data: bytes):
        client_id = int.from_bytes(data[0:4], byteorder="big")
        table_type_val = int.from_bytes(data[4:8], byteorder="big")
        size = int.from_bytes(data[8:12], byteorder="big")
        return ProcessBatchHeader(client_id, TableType(table_type_val), size)

# =========================================
# PROCESS BATCH
# =========================================
class ProcessBatch:
    def __init__(self, rows, table_type: TableType, client_id: int):
        self.header = ProcessBatchHeader(client_id, table_type)
        self.rows = rows
        self.header.size = sum(len(r.serialize()) for r in rows)

    def table_type(self) -> TableType:
        return self.header.table_type
    
    def serialize(self) -> bytes:
        payload = b"".join(r.serialize() for r in self.rows)
        return self.header.serialize() + payload

    @staticmethod
    def deserialize(data: bytes, header: ProcessBatchHeader):
        rows = []
        offset = 0
        row_cls = header.row_cls()
        payload = data
        while offset < len(payload):
            row, consumed = row_cls.deserialize(payload[offset:])
            rows.append(row)
            offset += consumed
        return ProcessBatch(rows, header.table_type, header.client_id)

    def from_file_rows(file_rows_serialized, file_path: str, client_id: int):
        if not file_rows_serialized:
            raise ValueError("No se pueden convertir filas vacías")
        table_type = TableType.from_path(file_path)
        
        file_cls = FILE_CLASSES.get(table_type)
        process_cls = PROCESS_CLASSES.get(table_type)
        if not file_cls or not process_cls:
            raise ValueError(f"Tipo de tabla no soportado: {table_type}")
        
        process_rows = []
        start_offset = 0

        while start_offset < len(file_rows_serialized):
            file_row, consumed = file_cls.deserialize(file_rows_serialized[start_offset:])
            process_rows.append(process_cls.from_file_row(file_row))
            start_offset += consumed

        return ProcessBatch(process_rows, table_type, client_id)

