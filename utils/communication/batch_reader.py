# common/batch_reader.py
from __future__ import annotations

import os
import logging
from typing import Generator
from utils.file_utils.file_chunk import FileChunk
from .directory_reader import DirectoryReader
from utils.file_utils.table_type import TableType

class TableStats:
    """
    Estadísticas de un tabla procesada.

    - total_rows: filas procesadas (sin contar header)
    - total_bytes: bytes procesados (sin contar header)
    - total_files: archivos procesados
    """
    def __init__(self, table: TableType) -> None:
        self.table: TableType = table
        self.total_rows: int = 0
        self.total_bytes: int = 0
        self.total_files: int = 0

    def __str__(self) -> str:
        return f"(rows={self.total_rows}, bytes={self.total_bytes}, files={self.total_files})"

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
        self.currentTable = None

    # -----------------------
    # Implementaciones
    # -----------------------
    def iter(self) -> Generator[FileChunk, None, None]:
        # Recorre todos los archivos del directorio
        for abs_path, rel_path, _ in self.directory_reader.iter():
            # Verificar si el archivo es reconocido por el sistema antes de procesarlo
            try:
                table = TableType.from_path(rel_path)  # Verifica si es un tipo válido
                if self.currentTable is None:
                    self.currentTable = TableStats(table)
                    logging.info("action: start_new_table | client_id:%s | table:%s", self.client_id, table.name)
                elif self.currentTable.table != table:
                    logging.info("action: finish_table | client_id:%s | table:%s | stats:%s", 
                                 self.client_id, self.currentTable.table, str(self.currentTable) if self.currentTable else "N/A")
                    self.currentTable = TableStats(table)
                    logging.info("action: start_new_table | client_id:%s | table:%s", self.client_id, table.name)
                    
                self.currentTable.total_files += 1
            
            except ValueError as e:
                # Archivo no reconocido (ej: payment_methods.csv) - ignorar silenciosamente
                logging.info("action: skip_unsupported_file | client_id:%s | file:%s | reason:%s", 
                           self.client_id, rel_path, str(e))
                continue  # Salta al siguiente archivo
            
            # Itera por batches del archivo actual
            for batch_payload, lines in self._iter_batch_payload(abs_path):
                # Actualiza stats
                if lines is not None:
                    self.currentTable.total_rows = lines
                    self.currentTable.total_bytes += len(batch_payload)
            
                self._update_stats(batch_payload)
                # Crea y devuelve FileChunk
                yield FileChunk(
                    client_id=self.client_id,
                    rel_path=rel_path,
                    data=batch_payload
                )

    def _iter_batch_payload(self, current_file: str) -> Generator[bytes, None, None]:
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
                        yield buffer, self.current_line - 1  # -1 because current_line was incremented
                    # arranca un nuevo batch con la línea que no entraba
                    buffer = encoded
                else:
                    buffer += encoded

            # Último batch si quedó algo
            if buffer:
                yield buffer, self.current_line

    # -----------------------
    # Helpers
    # -----------------------
    def _update_stats(self, chunk: bytes) -> None:
        self.total_bytes += len(chunk)
        self.total_batches += 1
