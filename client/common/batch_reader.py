# common/batch_reader.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Optional, Generator, List, Tuple


@dataclass(frozen=True)
class FileChunk:
    """
    Representa un fragmento (chunk) de un archivo dentro de un directorio.

    - rel_path: ruta relativa al root del reader (para mandar en el header)
    - file_size: tamaño total del archivo en bytes (SIZE)
    - data: bytes del chunk (puede ser b"" si el archivo es de 0 bytes)
    - first: True si este chunk es el primero del archivo
    - last: True si este chunk es el último del archivo
    """
    rel_path: str
    file_size: int
    data: bytes
    first: bool
    last: bool


class BatchReader:
    """
    Lector de UN archivo por batches (bytes o lines).

    Se mantiene para reutilización y test unitario independiente.
    """
    def __init__(
        self,
        path: str,
        batch_size: int,
        *,
        mode: str = "bytes",
        encoding: str = "utf-8",
        newline: str = "\n",
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size debe ser > 0")
        self.path: str = os.path.abspath(path)
        self.batch_size: int = int(batch_size)
        self.mode: str = mode.lower().strip()
        if self.mode not in {"bytes", "lines"}:
            raise ValueError("mode debe ser 'bytes' o 'lines'")

        self.encoding: str = encoding
        self.newline: str = newline

        self.last_batch: Optional[bytes] = None
        self.total_bytes: int = 0
        self.total_batches: int = 0
        try:
            self.file_size: int = os.path.getsize(self.path)
        except OSError:
            self.file_size = 0

    def __iter__(self) -> Iterable[bytes]:
        if self.mode == "bytes":
            return self._iter_bytes()
        return self._iter_lines()

    # -----------------------
    # Implementaciones
    # -----------------------
    def _iter_bytes(self) -> Generator[bytes, None, None]:
        with open(self.path, "rb") as f:
            while True:
                chunk = f.read(self.batch_size)
                if not chunk:
                    break
                self._update_stats(chunk)
                yield chunk

    def _iter_lines(self) -> Generator[bytes, None, None]:
        """
        No parte líneas: acumula texto hasta aproximarse a batch_size
        y luego lo encodea a bytes. Ideal para CSV/JSONL.
        """
        buf_text_parts: list[str] = []
        size_hint = 0
        nl = self.newline

        with open(self.path, "r", encoding=self.encoding, newline="") as f:
            for line in f:
                line = line.rstrip("\r\n")
                # estimación rápida en bytes (~1.1x)
                estimated_add = max(1, int(len(line) * 1.1)) + len(nl)
                if size_hint and (size_hint + estimated_add) >= self.batch_size:
                    batch_bytes = (nl.join(buf_text_parts) + nl).encode(self.encoding)
                    if batch_bytes:
                        self._update_stats(batch_bytes)
                        yield batch_bytes
                    buf_text_parts = []
                    size_hint = 0

                buf_text_parts.append(line)
                size_hint += estimated_add

            if buf_text_parts:
                batch_bytes = (nl.join(buf_text_parts) + nl).encode(self.encoding)
                if batch_bytes:
                    self._update_stats(batch_bytes)
                    yield batch_bytes

    # -----------------------
    # Helpers
    # -----------------------
    def _update_stats(self, chunk: bytes) -> None:
        self.last_batch = chunk
        self.total_bytes += len(chunk)
        self.total_batches += 1

