from .result_chunk import ResultChunk, ResultChunkHeader
from utils.communication.socket_utils import recv_exact

import socket

class ResultBatchReader:
    
    @staticmethod
    def from_bytes(data: bytes):
        if len(data) < ResultChunkHeader.HEADER_SIZE:
            raise ValueError("Datos insuficientes para el header")
        
        header = ResultChunkHeader.deserialize(data[:ResultChunkHeader.HEADER_SIZE])
        if len(data) < ResultChunkHeader.HEADER_SIZE + header.size:
            raise ValueError("Datos insuficientes para el payload")
        
        payload = data[ResultChunkHeader.HEADER_SIZE:ResultChunkHeader.HEADER_SIZE + header.size]
        return ResultChunk.deserialize(header, payload)
    
    @staticmethod
    def from_socket(socket: socket.socket):
        header_bytes = recv_exact(socket, ResultChunkHeader.HEADER_SIZE)
        header = ResultChunkHeader.deserialize(header_bytes)
        payload = recv_exact(socket, header.size)
        return ResultChunk.deserialize(header, payload)

