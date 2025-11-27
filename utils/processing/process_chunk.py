from .process_table import *
from utils.file_utils.table_type import TableType
from utils.common.table_row_registry import TableRowRegistry
import uuid


class ProcessChunkHeader:
    HEADER_SIZE = 28  # 4 bytes client_id + 4 bytes table_type + 4 bytes size + 16 bytes message_id

    def __init__(self, client_id: int, table_type: TableType, size: int = 0, message_id: uuid.UUID = None):
        self.client_id = client_id
        self.table_type = table_type
        self.size = size
        self.message_id = message_id or uuid.uuid4()

    def serialize(self) -> bytes:
        # Serializa como 3 enteros de 4 bytes cada uno (big-endian)
        return (
                self.client_id.to_bytes(4, byteorder="big") +
                self.table_type.value.to_bytes(4, byteorder="big") +
                self.size.to_bytes(4, byteorder="big") +
                self.message_id.bytes
        )

    @staticmethod
    def deserialize(data: bytes):
        client_id = int.from_bytes(data[0:4], byteorder="big")
        table_type_val = int.from_bytes(data[4:8], byteorder="big")
        size = int.from_bytes(data[8:12], byteorder="big")
        message_id = uuid.UUID(bytes=data[12:28])
        return ProcessChunkHeader(client_id, TableType(table_type_val), size, message_id)


# =========================================
# PROCESS BATCH
# =========================================
class ProcessChunk:
    def __init__(self, header: ProcessChunkHeader, rows: TableProcessRow):
        self.rows = rows
        self.header = header
        self.header.size = sum(len(r.serialize()) for r in rows)

    def client_id(self) -> int:
        return self.header.client_id

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
