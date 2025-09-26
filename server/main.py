import os
import socket
import threading
import configparser

CFG_PATH = os.getenv("SERVER_CONFIG", "/config.ini")

def load_config():
    cfg = configparser.ConfigParser()
    cfg.read(CFG_PATH)
    host = cfg.get("server", "host", fallback="0.0.0.0")
    port = cfg.getint("server", "port", fallback=5000)
    expected_env = cfg.get("server", "expected_clients_env", fallback="CLI_CLIENTS")
    expected = int(os.getenv(expected_env, "0"))
    return host, port, expected

def recv_until_blankline(conn):
    """
    Lee cabeceras estilo:
      KEY: VALUE\n
    termina con una línea en blanco
    """
    headers = {}
    buf = b""
    while True:
        chunk = conn.recv(1)
        if not chunk:
            break
        buf += chunk
        if buf.endswith(b"\n\n"):
            break
    # Parse
    for line in buf.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().upper()] = v.strip()
    return headers

def recv_exact(conn, nbytes, sink=None):
    """
    Lee exactamente nbytes de 'conn'. Si 'sink' es callable, se invoca con cada chunk.
    """
    remaining = nbytes
    total = 0
    CHUNK = 64 * 1024
    while remaining > 0:
        to_read = CHUNK if remaining > CHUNK else remaining
        data = conn.recv(to_read)
        if not data:
            raise ConnectionError(f"Conexión cerrada antes de recibir todo: faltaban {remaining} bytes")
        total += len(data)
        remaining -= len(data)
        if sink:
            sink(data)
    return total

def handle_client(conn, addr):
    try:
        print(f"[server] conexión de {addr}")
        headers = recv_until_blankline(conn)
        cli_id = headers.get("CLI_ID", "?")
        fname  = headers.get("FILENAME", "unknown")
        size_s = headers.get("SIZE", "0")
        try:
            size = int(size_s)
        except ValueError:
            size = 0

        received_bytes = 0

        def sink(_chunk):
            nonlocal received_bytes
            received_bytes += len(_chunk)

        if size > 0:
            recv_exact(conn, size, sink=sink)
        else:
            # Fallback: si no vino SIZE, drenar todo hasta FIN de escritura del cliente
            while True:
                data = conn.recv(64 * 1024)
                if not data:
                    break
                sink(data)

        print(f"[server] CLI_ID={cli_id} FILENAME={fname} BYTES={received_bytes}")

        # Responder ACK al cliente (una vez recibido el archivo completo)
        conn.sendall(b"OK\n")
    except Exception as e:
        print(f"[server] error con {addr}: {e!r}")
        try:
            conn.sendall(b"ERROR\n")
        except Exception:
            pass
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        conn.close()
        print(f"[server] conexión cerrada {addr}")

def serve(host: str, port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Reuse addr para reinicios rápidos
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        print(f"[server] escuchando en {host}:{port}")
        while True:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()

if __name__ == "__main__":
    host, port, expected = load_config()
    print(f"[server] esperado de clientes (hint): {expected}")
    serve(host, port)
