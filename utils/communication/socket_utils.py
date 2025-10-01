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

def recv_exact(sock: socket.socket, nbytes: int, *, sink=None) -> int:
    """
    Lee exactamente nbytes del socket. Si 'sink' es una función (p. ej. f.write),
    envía cada chunk allí; si no, descarta. Devuelve bytes recibidos.
    """
    remaining = nbytes
    total = 0
    CHUNK = 64 * 1024
    while remaining > 0:
        to_read = CHUNK if remaining > CHUNK else remaining
        data = sock.recv(to_read)
        if not data:
            raise OSError(f"connection closed early; remaining={remaining}")
        total += len(data)
        remaining -= len(data)
        if sink:
            sink(data)
    return total
