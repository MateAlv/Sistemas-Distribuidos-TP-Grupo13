from utils.file_utils.table_type import TableType


class FilterStatsMessage:
    def __init__(self, filter_id: int, client_id: int, table_type: TableType, 
                 chunks_received: int, chunks_not_sent: int):
        self.filter_id = filter_id
        self.client_id = client_id
        self.table_type = table_type
        self.chunks_received = chunks_received
        self.chunks_not_sent = chunks_not_sent
    
    def encode(self) -> bytes:
        return f"STATS;{self.filter_id};{self.client_id};{self.table_type.value};{self.chunks_received};{self.chunks_not_sent}".encode("utf-8")
    
    @classmethod
    def decode(cls, message: bytes) -> "FilterStatsMessage":
        decoded = message.decode("utf-8")
        parts = decoded.split(";")
        if len(parts) != 6 or parts[0] != "STATS":
            raise ValueError(f"Formato inválido de mensaje STATS: {decoded}")
        
        _, filter_id, client_id, table_type_value, chunks_received, chunks_not_sent = parts
        table_type = TableType(int(table_type_value))
        
        return cls(int(filter_id), int(client_id), table_type, 
                   int(chunks_received), int(chunks_not_sent))
    
class FilterStatsEndMessage:
    def __init__(self, filter_id: int, client_id: int, table_type: TableType):
        self.filter_id = filter_id
        self.client_id = client_id
        self.table_type = table_type
    
    def encode(self) -> bytes:
        return f"STATS_END;{self.filter_id};{self.client_id};{self.table_type.value}".encode("utf-8")
    
    @classmethod
    def decode(cls, message: bytes) -> "FilterStatsEndMessage":
        decoded = message.decode("utf-8")
        parts = decoded.split(";")
        if len(parts) != 3 or parts[0] != "STATS_END":
            raise ValueError(f"Formato inválido de mensaje STATS_END: {decoded}")
        
        _, filter_id, client_id, table_type_value = parts
        table_type = TableType(int(table_type_value))
        
        return cls(int(filter_id), int(client_id), table_type)