import base64
import json
from utils.file_utils.table_type import TableType


class AggregatorStatsMessage:
    def __init__(self, aggregator_id: int, client_id: int, table_type: TableType, 
                 total_expected: int, chunks_received: int, chunks_processed: int):
        self.aggregator_id = aggregator_id
        self.client_id = client_id
        self.table_type = table_type
        self.total_expected = total_expected
        self.chunks_received = chunks_received
        self.chunks_processed = chunks_processed
    
    def encode(self) -> bytes:
        return f"AGG_STATS;{self.aggregator_id};{self.client_id};{self.table_type.value};{self.total_expected};{self.chunks_received};{self.chunks_processed}".encode("utf-8")
    
    @classmethod
    def decode(cls, message: bytes) -> "AggregatorStatsMessage":
        decoded = message.decode("utf-8")
        parts = decoded.split(";")
        if len(parts) != 7 or parts[0] != "AGG_STATS":
            raise ValueError(f"Formato inválido de mensaje AGG_STATS: {decoded}")
        
        _, aggregator_id, client_id, table_type_value, total_expected, chunks_received, chunks_processed = parts
        table_type = TableType(int(table_type_value))
        
        return cls(int(aggregator_id), int(client_id), table_type, 
                   int(total_expected), int(chunks_received), int(chunks_processed))
    

class AggregatorStatsEndMessage:
    def __init__(self, aggregator_id: int, client_id: int, table_type: TableType):
        self.aggregator_id = aggregator_id
        self.client_id = client_id
        self.table_type = table_type
    
    def encode(self) -> bytes:
        return f"AGG_STATS_END;{self.aggregator_id};{self.client_id};{self.table_type.value}".encode("utf-8")
    
    @classmethod
    def decode(cls, message: bytes) -> "AggregatorStatsEndMessage":
        decoded = message.decode("utf-8")
        parts = decoded.split(";")
        if len(parts) != 4 or parts[0] != "AGG_STATS_END":
            raise ValueError(f"Formato inválido de mensaje AGG_STATS_END: {decoded}")
        
        _, aggregator_id, client_id, table_type_value = parts
        table_type = TableType(int(table_type_value))
        
        return cls(int(aggregator_id), int(client_id), table_type)


class AggregatorDataMessage:
    def __init__(self, aggregator_type: str, aggregator_id: int, client_id: int, table_type: TableType, payload: dict):
        self.aggregator_type = aggregator_type
        self.aggregator_id = aggregator_id
        self.client_id = client_id
        self.table_type = table_type
        self.payload = payload

    def encode(self) -> bytes:
        payload_json = json.dumps(self.payload).encode("utf-8")
        payload_b64 = base64.b64encode(payload_json).decode("ascii")
        return f"AGG_DATA;{self.aggregator_type};{self.aggregator_id};{self.client_id};{self.table_type.value};{payload_b64}".encode("utf-8")

    @classmethod
    def decode(cls, message: bytes) -> "AggregatorDataMessage":
        decoded = message.decode("utf-8")
        parts = decoded.split(";", 5)
        if len(parts) != 6 or parts[0] != "AGG_DATA":
            raise ValueError(f"Formato inválido de mensaje AGG_DATA: {decoded}")

        _, aggregator_type, aggregator_id, client_id, table_type_value, payload_b64 = parts
        payload_json = base64.b64decode(payload_b64.encode("ascii"))
        payload = json.loads(payload_json.decode("utf-8"))
        table_type = TableType(int(table_type_value))

        return cls(aggregator_type, int(aggregator_id), int(client_id), table_type, payload)
