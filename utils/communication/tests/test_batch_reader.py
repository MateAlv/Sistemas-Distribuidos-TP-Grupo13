# client/common/tests/test_batch_reader.py
import os
import pytest
from ..batch_reader import BatchReader
from ..file_chunk import FileChunk

TEST_PATH = "./utils/communication/tests/test_data"
MAX_BATCH_SIZE = 8 * 1024  # 8KB

@pytest.mark.parametrize("folder", ["menu_items", "stores"])
def test_batch_reader_batches_no_data_loss(folder):
    abs_dir = os.path.join(TEST_PATH, folder)
    client_id = 1
    reader = BatchReader(client_id, abs_dir, max_batch_size=MAX_BATCH_SIZE)  # tamaño chico para forzar varios batches

    chunks = list(reader.iter())
    assert len(chunks) > 0, f"No se generaron batches para {folder}"

    # Verificar que todos son FileChunk
    for chunk in chunks:
        assert chunk.header.client_id == client_id
        if not chunk.header.last:
            assert len(chunk.data) == reader.max_batch_size
        else:
            assert len(chunk.data) <= reader.max_batch_size
        assert isinstance(chunk, FileChunk)

    # Reconstruir el archivo desde los batches (concatenar payloads)
    reconstructed = b"".join(chunk.data for chunk in chunks).decode("utf-8")

    # Leer el archivo original quitando la primera línea (header)
    file_path = os.path.join(abs_dir, os.listdir(abs_dir)[0])
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    expected = "".join(lines[1:])  # quitar header

    assert reconstructed == expected, f"Contenido no coincide para {folder}"

def test_batch_reader_stats_updates():
    abs_dir = os.path.join(TEST_PATH, "menu_items")
    client_id = 1
    reader = BatchReader(client_id, abs_dir, max_batch_size=MAX_BATCH_SIZE)

    chunks = list(reader.iter())

    # Stats deben ser iguales a lo acumulado
    total_bytes = sum(len(c.data) for c in chunks)
    assert reader.total_bytes == total_bytes
    assert reader.total_batches == len(chunks)
