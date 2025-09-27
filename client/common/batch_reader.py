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


class DirectoryBatchReader:
    """
    Recorre un directorio (recursivo) y emite FileChunk por cada archivo encontrado.

    - Ordena los archivos por ruta relativa (estable).
    - Si 'extensions' no está vacío, filtra por extensión (e.g., [".csv"]).
    - Para archivos de 0 bytes emite UN chunk vacío con first=True, last=True
      (para que el caller pueda enviar header + SIZE=0 y esperar ACK sin cuerpo).
    - No conoce nada del protocolo; sólo entrega datos y metadata.

    Stats:
      - total_files, total_bytes, total_chunks
    """

    def __init__(
        self,
        root_dir: str,
        batch_size: int,
        *,
        mode: str = "bytes",
        encoding: str = "utf-8",
        newline: str = "\n",
        extensions: Optional[List[str]] = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size debe ser > 0")
        self.root_dir = os.path.abspath(root_dir)
        self.batch_size = int(batch_size)
        self.mode = mode
        self.encoding = encoding
        self.newline = newline
        self.extensions = [e.lower() for e in (extensions or [])]

        # stats
        self.total_files = 0
        self.total_bytes = 0
        self.total_chunks = 0

    def __iter__(self) -> Generator[FileChunk, None, None]:
        for abs_path, rel_path in self._iter_files(self.root_dir):
            size = self._safe_size(abs_path)
            self.total_files += 1
            self.total_bytes += size

            if size == 0:
                self.total_chunks += 1
                yield FileChunk(rel_path=rel_path, file_size=0, data=b"", first=True, last=True)
                continue

            reader = BatchReader(
                abs_path,
                self.batch_size,
                mode=self.mode,
                encoding=self.encoding,
                newline=self.newline,
            )

            first = True
            remaining = size
            for chunk in reader:
                remaining -= len(chunk)
                last = remaining == 0
                self.total_chunks += 1
                yield FileChunk(
                    rel_path=rel_path,
                    file_size=size,
                    data=chunk,
                    first=first,
                    last=last,
                )
                first = False

    # -----------------------
    # Internos
    # -----------------------
    def _iter_files(self, root: str) -> Generator[Tuple[str, str], None, None]:
        root_abs = os.path.abspath(root)
        for dirpath, _, filenames in os.walk(root_abs):
            for name in sorted(filenames):
                abs_path = os.path.join(dirpath, name)
                if not os.path.isfile(abs_path):
                    continue
                if self.extensions:
                    _, ext = os.path.splitext(name)
                    if ext.lower() not in self.extensions:
                        continue
                rel_path = os.path.relpath(abs_path, root_abs)
                yield abs_path, rel_path

    @staticmethod
    def _safe_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0
