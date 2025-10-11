from _thread import start_new_thread
import os
from ..directory_reader import DirectoryReader

TEST_PATH = "./.data-test-reduced"  # Directorio con datos de prueba generados

def test_directory_reader_creation():
    # Crear reader
    reader = DirectoryReader(TEST_PATH)
    assert reader.root_dir.endswith("data-test-reduced")
    assert reader.rel_root_dir == TEST_PATH
    assert reader.total_files == 0

def test_directory_reader_iteration():
    # Leer todos los archivos
    reader = DirectoryReader(TEST_PATH)
    files = list(reader.iter())
    assert len(files) == 2
    assert files[0][1] == "menu_items/menu_items.csv"
    assert files[1][1] == "stores/stores.csv"
    assert reader.total_files == 2
    
def test_directory_reader_single_folder():
    # Leer un solo folder
    reader = DirectoryReader(os.path.join(TEST_PATH, "stores"))
    files = list(reader.iter())
    assert len(files) == 1
    assert files[0][1] == "stores.csv"
    assert reader.total_files == 1
    
    reader = DirectoryReader(os.path.join(TEST_PATH, "menu_items"))
    files = list(reader.iter())
    assert len(files) == 1
    assert files[0][1] == "menu_items.csv"
    assert reader.total_files == 1


