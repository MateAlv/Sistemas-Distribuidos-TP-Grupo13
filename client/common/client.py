# common/client.py
import os
import socket
import logging
from typing import Dict, List, Optional, Tuple

from common.batch_reader import BatchReader


class Client:
    """
    - Conexión persistente (default): 1 TCP para TODOS los archivos del directorio.
    - Por cada archivo:
        Header (CLI_ID/FILENAME/SIZE + blank line) + cuerpo en batches
        Espera 'OK\\n' del server, SIN cerrar escritura.
    - Al final:
        Envía 'F:FINISHED\\n' y hace shutdown(SHUT_WR) una sola vez.
    """

    def __init__(self, config: Dict, default_data_dir: str = "/data") -> None:
        self.id: str = str(config.get("id", ""))

        mp = config.get("message_protocol", {}) or {}
        self.batch_size: int = int(mp.get("batch_size", 100))
        self.success_resp: bytes = str(mp.get("success_response", "OK")).encode("utf-8")
        self.message_delim: bytes = str(mp.get("message_delimiter", "\n")).encode("utf-8")

        self.finished_header: str = str(mp.get("finished_header", "F:"))
        self.finished_body: str = str(mp.get("finished_body", "FINISHED"))
        self.field_separator: str = str(mp.get("field_separator", ";"))
        self.batch_separator: str = str(mp.get("batch_separator", "~"))
        self.handshake_hello: str = str(mp.get("hello", "HELLO"))

        self.server_address_str: str = str(config.get("server_address", "server:5000"))
        self.server_host, self.server_port = self._parse_host_port(self.server_address_str)

        self.data_dir: str = str(config.get("data_dir", default_data_dir))

        # Opcionales
        conn_cfg = config.get("connection", {}) or {}
        self.connection_mode: str = str(conn_cfg.get("mode", "single")).lower().strip()
        if self.connection_mode not in {"single", "per_file"}:
            self.connection_mode = "single"
        self.connect_timeout: float = float(conn_cfg.get("connect_timeout", 10))
        self.io_timeout: float = float(conn_cfg.get("io_timeout", 30))

        data_cfg = config.get("data", {}) or {}
        self.allowed_exts: List[str] = [e.lower() for e in data_cfg.get("extensions", [])]

        # Handshake on/off (si no viene, asumimos habilitado)
        hs_cfg = config.get("handshake", {}) or {}
        self.handshake_enabled: bool = bool(hs_cfg.get("enabled", True))

        logging.debug(
            "client_init | id=%s host=%s port=%s data_dir=%s mode=%s batch_size=%s",
            self.id, self.server_host, self.server_port, self.data_dir, self.connection_mode, self.batch_size
        )

    # ---------------------------
    # API pública
    # ---------------------------
    def start_client_loop(self) -> None:
        files = self._list_files(self.data_dir)
        if not files:
            logging.warning("No se encontraron archivos en %s", self.data_dir)
            return

        if self.connection_mode == "single":
            logging.info("Conexión TCP persistente (single) para %d archivo(s)", len(files))
            with self._open_socket() as sock:
                self._maybe_handshake(sock)

                for abs_path, rel_path in files:
                    self._send_one_file(sock, abs_path, rel_path)

                # FIN del lote (una sola vez)
                self._send_finished(sock)
                # Cierro escritura recién ahora
                try:
                    sock.shutdown(socket.SHUT_WR)
                except Exception:
                    pass

                # (opcional) intento leer ACK del FIN
                try:
                    ack = self._recv_line(sock)
                    if ack:
                        logging.debug("ACK FIN recibido: %r", ack)
                except Exception:
                    pass

        else:
            logging.info("Una conexión por archivo (per_file) para %d archivo(s)", len(files))
            for abs_path, rel_path in files:
                with self._open_socket() as sock:
                    self._maybe_handshake(sock)
                    self._send_one_file(sock, abs_path, rel_path)
                    # para per_file, puede enviarse FIN por conexión si quisieras,
                    # pero no es necesario si el server cierra post-ACK

        logging.info("Cliente %s: envío completado.", self.id)

    # ---------------------------
    # Envío de archivos
    # ---------------------------
    def _send_one_file(self, sock: socket.socket, abs_path: str, rel_path: str) -> None:
        size = os.path.getsize(abs_path)
        logging.info("Enviando archivo: rel=%s size=%d", rel_path, size)

        header = (
            f"CLI_ID: {self.id}\n"
            f"FILENAME: {rel_path}\n"
            f"SIZE: {size}\n"
            f"\n"
        ).encode("utf-8")
        self._sendall(sock, header)

        reader = BatchReader(abs_path, self.batch_size)  # por defecto, mode="bytes"
        total_sent = 0
        for chunk in reader:
            if not isinstance(chunk, (bytes, bytearray)):
                chunk = str(chunk).encode("utf-8")
            self._sendall(sock, chunk)
            total_sent += len(chunk)

        # NO hacemos shutdown(SHUT_WR) acá: queremos seguir mandando más archivos
        # Esperamos ACK del server (OK\n) por este archivo
        ack = self._recv_line(sock)
        logging.debug("ACK recibido para %s: %r", rel_path, ack)
        if not ack or not ack.startswith(self.success_resp):
            raise RuntimeError(f"ACK inválido para {rel_path}: {ack!r}")

        logging.info(
            "Archivo enviado OK: %s (%d bytes; último batch=%d bytes)",
            rel_path, total_sent, len(reader.last_batch or b"")
        )

    def _send_finished(self, sock: socket.socket) -> None:
        msg = (f"{self.finished_header}{self.finished_body}{self._delim_str()}").encode("utf-8")
        try:
            self._sendall(sock, msg)
        except Exception as e:
            logging.debug("Fallo al enviar FIN opcional: %r (puede no ser necesario)", e)

    # ---------------------------
    # Utilitarios de red
    # ---------------------------
    def _open_socket(self) -> socket.socket:
        sock = socket.create_connection((self.server_host, self.server_port), timeout=self.connect_timeout)
        sock.settimeout(self.io_timeout)
        logging.debug("Conexión abierta a %s:%s", self.server_host, self.server_port)
        return sock

    def _maybe_handshake(self, sock: socket.socket) -> None:
        if not self.handshake_enabled:
            return
        hello = f"{self.handshake_hello} {self.id}{self._delim_str()}".encode("utf-8")
        self._sendall(sock, hello)
        try:
            line = self._recv_line(sock)
            logging.debug("Handshake respuesta: %r", line)
            if line and not line.startswith(self.success_resp):
                logging.warning("Handshake sin OK; continúo: %r", line)
        except Exception as e:
            logging.debug("Handshake sin respuesta (%r); continúo", e)

    def _sendall(self, sock: socket.socket, data: bytes) -> None:
        view = memoryview(data)
        while view:
            sent = sock.send(view)
            view = view[sent:]

    def _recv_line(self, sock: socket.socket) -> bytes:
        buf = bytearray()
        delim = self.message_delim
        while True:
            b = sock.recv(1)
            if not b:
                break
            buf += b
            if buf.endswith(delim):
                break
        return bytes(buf).rstrip(b"\r\n")

    # ---------------------------
    # Utilitarios de archivos
    # ---------------------------
    def _list_files(self, root: str) -> List[Tuple[str, str]]:
        files: List[Tuple[str, str]] = []
        root_abs = os.path.abspath(root)
        for dirpath, _, filenames in os.walk(root_abs):
            for name in sorted(filenames):
                abs_path = os.path.join(dirpath, name)
                if not os.path.isfile(abs_path):
                    continue
                if self.allowed_exts:
                    _, ext = os.path.splitext(name)
                    if ext.lower() not in self.allowed_exts:
                        continue
                rel_path = os.path.relpath(abs_path, root_abs)
                files.append((abs_path, rel_path))
        files.sort(key=lambda t: t[1])
        return files

    # ---------------------------
    # Helpers
    # ---------------------------
    def _parse_host_port(self, s: str) -> Tuple[str, int]:
        if ":" not in s:
            return s, 5000
        host, port = s.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            logging.warning("Puerto inválido en %s; uso 5000", s)
            return host, 5000

    def _delim_str(self) -> str:
        try:
            return self.message_delim.decode("utf-8")
        except Exception:
            return "\n"
