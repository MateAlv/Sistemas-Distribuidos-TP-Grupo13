#!/usr/bin/env python3
import os
import socket
import yaml

CFG_PATH = os.getenv("CLIENT_CONFIG", "/config.yaml")

def load_config():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    host = cfg.get("server_host", "server")
    port = int(cfg.get("server_port", 5000))
    csv_path = cfg.get("csv_path", "/data/agency.csv")
    chunk = int(cfg.get("chunk_size_bytes", 65536))
    send_header = bool(cfg.get("send_header", True))
    cli_id = os.getenv("CLI_ID", "0")
    return host, port, csv_path, chunk, send_header, cli_id

def file_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0

def send_file(sock, path, chunk_size):
    sent = 0
    with open(path, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            sock.sendall(data)
            sent += len(data)
    return sent

def main():
    host, port, csv_path, chunk_size, send_header, cli_id = load_config()

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV no encontrado en {csv_path}")

    size = file_size(csv_path)
    fname = os.path.basename(csv_path)

    addr = (host, port)
    print(f"[client {cli_id}] conectando a {addr}, enviando {fname} ({size} bytes) en chunks de {chunk_size}...")

    with socket.create_connection(addr) as sock:
        # Enviamos un header simple estilo HTTP (texto) y luego el binario del CSV.
        if send_header:
            header = (
                f"CLI_ID: {cli_id}\n"
                f"FILENAME: {fname}\n"
                f"SIZE: {size}\n"
                f"\n"
            ).encode("utf-8")
            sock.sendall(header)

        total_sent = send_file(sock, csv_path, chunk_size)
        # Importante para indicar fin de escritura sin cerrar la conexión de lectura
        try:
            sock.shutdown(socket.SHUT_WR)
        except Exception:
            pass

        print(f"[client {cli_id}] enviado total {total_sent} bytes, esperando ACK...")
        # Espera del ACK "OK\n"
        ack = sock.recv(1024)
        print(f"[client {cli_id}] ACK recibido: {ack!r}")

    print(f"[client {cli_id}] finalizado.")

if __name__ == "__main__":
    main()
