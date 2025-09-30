from common.directory_reader import DirectoryReader

TEST_PATH = "./client/common/tests/test_data"

def test_directory_reader_creation():
    # Crear reader
    reader = DirectoryReader(TEST_PATH)
    assert reader.root_dir.endswith("test_data")
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

