import datetime
from .file_table import *
from .process_table import *
from .table_type import TableType
from .table_row_registry import TableRowRegistry

class ProcessChunkHeader:

    HEADER_SIZE = 12  # 4 bytes client_id + 4 bytes table_type + 4 bytes size
    
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

    @staticmethod
    def deserialize(data: bytes):
        client_id = int.from_bytes(data[0:4], byteorder="big")
        table_type_val = int.from_bytes(data[4:8], byteorder="big")
        size = int.from_bytes(data[8:12], byteorder="big")
        return ProcessChunkHeader(client_id, TableType(table_type_val), size)

# =========================================
# PROCESS BATCH
# =========================================
class ProcessChunk:
    def __init__(self, header: ProcessChunkHeader, rows: TableProcessRow):
        self.rows = rows
        self.header = header
        self.header.size = sum(len(r.serialize()) for r in rows)

    def table_type(self) -> TableType:
        return self.header.table_type
    
    def serialize(self) -> bytes:
        payload = b"".join(r.serialize() for r in self.rows)
        return self.header.serialize() + payload

    @staticmethod
    def deserialize(header: ProcessChunkHeader, data: bytes):
        rows = []
        offset = 0
        process_cls = TableRowRegistry.get_process_class(header.table_type)
        payload = data
        while offset < len(payload):
            row, consumed = process_cls.deserialize(payload[offset:])
            rows.append(row)
            offset += consumed
        return ProcessChunk(header, rows)

