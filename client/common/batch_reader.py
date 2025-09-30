# common/batch_reader.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Optional, Generator, List, Tuple
from common.file_chunk import FileChunk
from common.directory_reader import DirectoryReader


class BatchReader:
    """
    Lector de todos los archivos por batches (bytes o lines).

    Se mantiene para reutilización y test unitario independiente.
    """
    def __init__(
        self,
        client_id: int,
        root: str,
        max_batch_size: int,
    ) -> None:
        if max_batch_size <= 0:
            raise ValueError("batch_size debe ser > 0")
        self.client_id = client_id 
        self.root = os.path.abspath(root)
        self.max_batch_size: int = int(max_batch_size)
        # Directorio reader -> itera todos los archivos del directorio root
        self.directory_reader = DirectoryReader(root)
        self.current_line: int = 0
        # Stats
        self.total_bytes: int = 0
        self.total_batches: int = 0
        self.encoding: str = "utf-8"

    # -----------------------
    # Implementaciones
    # -----------------------
    def iter(self) -> Generator[FileChunk, None, None]:
        # Recorre todos los archivos del directorio
        for abs_path, rel_path, _ in self.directory_reader.iter():
            # Itera por batches del archivo actual
            for batch_payload in self.__iter_batch_payload__(abs_path):
                # Determina si es el último chunk (si el batch < max_batch_size)
                is_last = len(batch_payload) < self.max_batch_size
                # Actualiza stats
                self._update_stats(batch_payload)
                # Crea y devuelve FileChunk
                yield FileChunk(
                    client_id=self.client_id,
                    rel_path=rel_path,
                    data=batch_payload,
                    last=is_last,
                )

    def __iter_batch_payload__(self, current_file: str) -> Generator[bytes, None, None]:
        """Itera el archivo actual hasta completar el batch_size."""
        buffer = b""
        self.current_line = 0

        with open(current_file, "r", encoding=self.encoding, newline="") as f:
            for line in f:
                # Saltear header
                if self.current_line == 0:
                    self.current_line += 1
                    continue
                self.current_line += 1

                encoded = line.encode(self.encoding)

                # Si la línea no entra en el buffer actual
                if len(buffer) + len(encoded) > self.max_batch_size:
                    # yield el buffer actual
                    if buffer:
                        yield buffer
                    # arranca un nuevo batch con la línea que no entraba
                    buffer = encoded
                else:
                    buffer += encoded

            # Último batch si quedó algo
            if buffer:
                yield buffer

    # -----------------------
    # Helpers
    # -----------------------
    def _update_stats(self, chunk: bytes) -> None:
        self.total_bytes += len(chunk)
        self.total_batches += 1
