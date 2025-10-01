import socket

def ensure_socket(socket: socket.socket) -> None:
    if socket is None:
        raise RuntimeError("Sender no conectado. Llamá a connect() primero.")

def sendall(socket: socket.socket, data: bytes) -> None:
    assert socket is not None
    data_len = len(data)
    while data_len > 0:
        sent = socket.send(data)
        if sent == 0:
            raise OSError("socket send devolvió 0 (conexión rota)")
        data = data[sent:]
        data_len -= sent

def recv_exact(sock: socket.socket, nbytes: int, *, sink=None) -> bytes:
    """
    Lee exactamente `nbytes` del socket.
    Si la conexión se cierra antes, lanza OSError.
    """
    remaining = nbytes
    parts = []
    while remaining > 0:
        data = sock.recv(remaining)   # read only what’s left
        if not data:
            raise OSError(f"connection closed early; remaining={remaining}")
        parts.append(data)
        remaining -= len(data)
        if sink:
            sink(data)
    return b"".join(parts)
