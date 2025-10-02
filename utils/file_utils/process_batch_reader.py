from .process_chunk import ProcessChunk, ProcessChunkHeader
from .table_type import TableType
from .table_row_registry import TableRowRegistry
from ..communication.socket_utils import recv_exact

import socket

class ProcessBatchReader:
    
    @staticmethod
    def from_bytes(data: bytes):
        if len(data) < ProcessChunkHeader.HEADER_SIZE:
            raise ValueError("Datos insuficientes para el header")
        
        header = ProcessChunkHeader.deserialize(data[:ProcessChunkHeader.HEADER_SIZE])
        if len(data) < ProcessChunkHeader.HEADER_SIZE + header.size:
            raise ValueError("Datos insuficientes para el payload")
        
        payload = data[ProcessChunkHeader.HEADER_SIZE:ProcessChunkHeader.HEADER_SIZE + header.size]
        return ProcessChunk.deserialize(header, payload)
    
    @staticmethod
    def from_socket(socket: socket.socket):
        header_bytes = recv_exact(socket, ProcessChunkHeader.HEADER_SIZE)
        header = ProcessChunkHeader.deserialize(header_bytes)
        payload = recv_exact(socket, header.size)
        return ProcessChunk.deserialize(header, payload)

    @staticmethod
    def from_file_rows(file_rows_serialized: bytes, file_path: str, client_id: int):
        if not file_rows_serialized:
            raise ValueError("No se pueden convertir filas vacías")
        
        table_type = TableType.from_path(file_path)
        file_cls = TableRowRegistry.get_file_class(table_type)
        process_cls = TableRowRegistry.get_process_class(table_type)
        
        if not file_cls or not process_cls:
            raise ValueError(f"Tipo de tabla no soportado: {table_type}")
        
        process_rows = []
        start_offset = 0

        while start_offset < len(file_rows_serialized):
            file_row, consumed = file_cls.deserialize(file_rows_serialized[start_offset:])
            process_rows.append(process_cls.from_file_row(file_row))
            start_offset += consumed

        header = ProcessChunkHeader(client_id, table_type)
        return ProcessChunk(header, process_rows)

