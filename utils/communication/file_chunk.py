import socket
from .socket_utils import recv_exact

class FileChunkHeader:
    
    # Tamaño fijo del header en bytes (client_id, size, last, var_header_size)
    HEADER_SIZE = 13
    
    def __init__(self, rel_path: str, client_id: int, size: int, last: bool):
        # ---- fixed size ----
        # Size: 4 + 4 + 1 + 4 = 13 bytes + rel_path (variable)
        self.client_id = client_id # int (4 bytes)
        self.payload_size = size # int (4 bytes)
        self.last = last # bool (1 byte)
        self.var_header_size = len(rel_path.encode('utf-8')) # int (4 bytes)
        # ---- variable size ----
        self.rel_path = rel_path

    def serialize(self) -> bytes:
        last_byte = b'1' if self.last else b'0'
        c_id_bytes = self.client_id.to_bytes(4, byteorder='big')
        size_bytes = self.payload_size.to_bytes(4, byteorder='big')
        var_size_bytes = self.var_header_size.to_bytes(4, byteorder='big')
        header_bytes = c_id_bytes + size_bytes + last_byte + var_size_bytes + self.rel_path.encode('utf-8')
        return header_bytes
    
    def deserialize(data: bytes) -> 'FileChunkHeader':
        if len(data) < FileChunkHeader.HEADER_SIZE:
            raise ValueError("Datos insuficientes para el header")
        
        client_id = int.from_bytes(data[0:4], byteorder='big')
        size = int.from_bytes(data[4:8], byteorder='big')
        last = data[8:9] == b'1'
        var_header_size = int.from_bytes(data[9:13], byteorder='big')
        
        if len(data) < FileChunkHeader.HEADER_SIZE + var_header_size:
            raise ValueError("Datos insuficientes para el header variable")

        rel_path = data[13:13+var_header_size].decode('utf-8')

        return FileChunkHeader(rel_path, client_id, size, last)

    def recv(socket: socket.socket) -> 'FileChunkHeader':
        # Leer header fijo
        data = recv_exact(socket, FileChunkHeader.HEADER_SIZE)
        
        client_id = int.from_bytes(data[0:4], byteorder='big')
        size = int.from_bytes(data[4:8], byteorder='big')
        last = data[8:9] == b'1'
        var_header_size = int.from_bytes(data[9:13], byteorder='big')

        # Leer header variable
        var_header_bytes = recv_exact(socket, var_header_size)

        rel_path = var_header_bytes.decode('utf-8')

        return FileChunkHeader(rel_path, client_id, size, last)

class FileChunk:
    """
    Representa un fragmento (chunk) de un archivo dentro de un directorio.

    - rel_path: ruta relativa al root del reader (para mandar en el header)
    - file_size: tamaño total del archivo en bytes (SIZE)
    - data: bytes del chunk (puede ser b"" si el archivo es de 0 bytes)
    - first: True si este chunk es el primero del archivo
    - last: True si este chunk es el último del archivo
    """
    def __init__(self, rel_path: str, client_id: int, last: bool, data: bytes):
        header = FileChunkHeader(rel_path, client_id, len(data), last)
        self.header = header
        self.data = data
    
    def path(self) -> str:
        return self.header.rel_path
    
    def client_id(self) -> int:
        return self.header.client_id
    
    def payload_size(self) -> int:
        return self.header.payload_size
    
    def payload(self) -> bytes:
        return self.data
    
    def is_last_file_chunk(self) -> bool:
        return self.header.last
    
    def serialize(self) -> bytes:
        return self.header.serialize() + self.data
    
    def deserialize(data: bytes) -> 'FileChunk':

        header = FileChunkHeader.deserialize(data)

        if len(data) < FileChunkHeader.HEADER_SIZE + header.var_header_size + header.payload_size:
            raise ValueError("Datos insuficientes para el payload")

        payload = data[FileChunkHeader.HEADER_SIZE + header.var_header_size:FileChunkHeader.HEADER_SIZE + header.var_header_size + header.payload_size]
        return FileChunk(header.rel_path, header.client_id, header.last, payload)
    
    def recv(socket: socket.socket) -> 'FileChunk':
    
        # Leer header
        header = FileChunkHeader.recv(socket)

        # Leer payload
        payload = recv_exact(socket, header.payload_size)
        
        return FileChunk(header.rel_path, header.client_id, header.last, payload)
