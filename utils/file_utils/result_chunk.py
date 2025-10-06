from .result_table import *
from .table_type import ResultTableType 
from .table_row_registry import TableRowRegistry

class ResultChunkHeader:

    HEADER_SIZE = 12  # 4 bytes client_id + 4 bytes result_query + 4 bytes size
    
    def __init__(self, client_id: int, result_query: ResultTableType, size: int = 0):
        self.client_id = client_id
        self.result_query = result_query
        self.size = size

    def serialize(self) -> bytes:
        # Serializa como 3 enteros de 4 bytes cada uno (big-endian)
        return (
            self.client_id.to_bytes(4, byteorder="big") +
            self.result_query.value.to_bytes(4, byteorder="big") +
            self.size.to_bytes(4, byteorder="big")
        )

    @staticmethod
    def deserialize(data: bytes):
        client_id = int.from_bytes(data[0:4], byteorder="big")
        result_query_value = int.from_bytes(data[4:8], byteorder="big")
        size = int.from_bytes(data[8:12], byteorder="big")
        return ResultChunkHeader(client_id, ResultTableType(result_query_value), size)

# =========================================
# PROCESS BATCH
# =========================================
class ResultChunk:
    def __init__(self, header: ResultChunkHeader, rows: TableResultRow):
        self.rows = rows
        self.header = header
        self.header.size = sum(len(r.serialize()) for r in rows)

    def client_id(self) -> int:
        return self.header.client_id
    
    def query_type(self) -> ResultTableType:
        return self.header.result_query
    
    def serialize(self) -> bytes:
        payload = b"".join(r.serialize() for r in self.rows)
        return self.header.serialize() + payload

    @staticmethod
    def deserialize(header: ResultChunkHeader, data: bytes):
        rows = []
        offset = 0
        result_cls = TableRowRegistry.get_result_class(header.result_query)
        payload = data
        while offset < len(payload):
            row, consumed = result_cls.deserialize(payload[offset:])
            rows.append(row)
            offset += consumed
        return ResultChunk(header, rows)