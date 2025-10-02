from utils.communication.batch_reader import BatchReader
from utils.communication.file_chunk import FileChunk, FileChunkHeader
from utils.file_utils.process_table import StoresProcessRow
from utils.file_utils.process_batch_reader import ProcessBatchReader
from utils.file_utils.table_type import TableType

SHORT_TEST_PATH = "./utils/tests/test_short_data"

def test_integration_one_short_file():
    # Lector de batches
    reader = BatchReader(
        client_id=int(1),
        root=SHORT_TEST_PATH,
        max_batch_size=1024,
    )

    #Lee un solo archivo de un solo chunk (stores.csv)
    chunk = list(reader.iter())[0]

    assert isinstance(chunk, FileChunk)
    assert isinstance(chunk.header, FileChunkHeader)
    assert chunk.header.client_id == 1
    assert chunk.header.payload_size == len(chunk.data)
    assert chunk.header.rel_path == "stores/stores.csv"
    assert chunk.header.last == chunk.is_last_file_chunk()
    assert chunk.header.payload_size <= 1024

    bytes_serialized = chunk.serialize()
    
    client_id = int.from_bytes(bytes_serialized[0:4], byteorder='big')
    payload_size = int.from_bytes(bytes_serialized[4:8], byteorder='big')
    last = bytes_serialized[8:9] == b'1'
    var_header_size = int.from_bytes(bytes_serialized[9:13], byteorder='big')
    rel_path = bytes_serialized[13:13+var_header_size].decode('utf-8')

    assert client_id == chunk.header.client_id
    assert payload_size == chunk.header.payload_size
    assert last == chunk.header.last
    assert rel_path == chunk.header.rel_path
    assert var_header_size == chunk.header.var_header_size
    
    chunk_header = FileChunkHeader(rel_path, client_id, payload_size, last)

    payload = bytes_serialized[13+var_header_size:13+var_header_size+payload_size]
    assert payload == chunk.data
    chunk_readed = FileChunk(rel_path, client_id, last, payload)
    
    assert chunk_readed.header.client_id == chunk.header.client_id
    assert chunk_readed.header.payload_size == chunk.header.payload_size
    assert chunk_readed.header.last == chunk.header.last
    assert chunk_readed.header.rel_path == chunk.header.rel_path
    assert chunk_readed.data == chunk.data
    
    process_chunk = ProcessBatchReader.from_file_rows(chunk_readed.data, chunk_readed.header.rel_path, chunk_readed.header.client_id)
    assert process_chunk.table_type() == TableType.STORES
    assert len(process_chunk.rows) == 10
    assert process_chunk.rows[0].store_id == 1
    assert process_chunk.rows[0].store_name == "G Coffee @ USJ 89q"
    assert process_chunk.rows[1].store_id == 2
    assert process_chunk.rows[1].store_name == "G Coffee @ Kondominium Putra"
    assert process_chunk.rows[2].store_id == 3
    assert process_chunk.rows[2].store_name == "G Coffee @ USJ 57W"
    assert process_chunk.rows[3].store_id == 4
    assert process_chunk.rows[3].store_name == "G Coffee @ Kampung Changkat"
    assert process_chunk.rows[4].store_id == 5
    assert process_chunk.rows[4].store_name == "G Coffee @ Seksyen 21"
    assert process_chunk.rows[5].store_id == 6
    assert process_chunk.rows[5].store_name == "G Coffee @ Alam Tun Hussein Onn"
    assert process_chunk.rows[6].store_id == 7
    assert process_chunk.rows[6].store_name == "G Coffee @ Damansara Saujana"
    assert process_chunk.rows[7].store_id == 8
    assert process_chunk.rows[7].store_name == "G Coffee @ Bandar Seri Mulia"
    assert process_chunk.rows[8].store_id == 9
    assert process_chunk.rows[8].store_name == "G Coffee @ PJS8"
    assert process_chunk.rows[9].store_id == 10
    assert process_chunk.rows[9].store_name == "G Coffee @ Taman Damansara"

LONG_TEST_PATH = "./utils/tests/test_long_data"

def test_integration_one_long_file():
    # Lector de batches
    reader = BatchReader(
        client_id=int(1),
        root=LONG_TEST_PATH,
        max_batch_size=4096,
    )


    for chunk in reader.iter():
        assert isinstance(chunk, FileChunk)
        assert isinstance(chunk.header, FileChunkHeader)
        assert chunk.header.client_id == 1
        assert chunk.header.payload_size == len(chunk.data)
        assert chunk.header.rel_path == "transactions/transactions_202307.csv"
        assert chunk.header.last == chunk.is_last_file_chunk()
        if not chunk.is_last_file_chunk():
            assert chunk.header.payload_size == 4096
        else:
            assert chunk.header.payload_size <= 4096

        bytes_serialized = chunk.serialize()
        
        client_id = int.from_bytes(bytes_serialized[0:4], byteorder='big')
        payload_size = int.from_bytes(bytes_serialized[4:8], byteorder='big')
        last = bytes_serialized[8:9] == b'1'
        var_header_size = int.from_bytes(bytes_serialized[9:13], byteorder='big')
        rel_path = bytes_serialized[13:13+var_header_size].decode('utf-8')

        assert client_id == chunk.header.client_id
        assert payload_size == chunk.header.payload_size
        assert last == chunk.header.last
        assert rel_path == chunk.header.rel_path
        assert var_header_size == chunk.header.var_header_size
        
        chunk_header = FileChunkHeader(rel_path, client_id, payload_size, last)

        payload = bytes_serialized[13+var_header_size:13+var_header_size+payload_size]
        assert payload == chunk.data
        chunk_readed = FileChunk(rel_path, client_id, last, payload)
        
        assert chunk_readed.header.client_id == chunk.header.client_id
        assert chunk_readed.header.payload_size == chunk.header.payload_size
        assert chunk_readed.header.last == chunk.header.last
        assert chunk_readed.header.rel_path == chunk.header.rel_path
        assert chunk_readed.data == chunk.data

        process_chunk = ProcessBatchReader.from_file_rows(chunk_readed.data, chunk_readed.header.rel_path, chunk_readed.header.client_id)
        assert process_chunk.table_type() == TableType.TRANSACTIONS

"""
DATA_PATH = "./.data"

def test_integration_all_files():
    # Lector de batches
    reader = BatchReader(
        client_id=int(1),
        root=DATA_PATH,
        max_batch_size=1024 * 8,  # 8 KB
    )

    tables_acum = {
        TableType.STORES: 0,
        TableType.USERS: 0,
        TableType.MENU_ITEMS: 0,
        TableType.TRANSACTIONS: 0,
        TableType.TRANSACTION_ITEMS: 0,
    }
    
    for chunk in reader.iter():
        assert isinstance(chunk, FileChunk)
        assert isinstance(chunk.header, FileChunkHeader)
        assert chunk.header.client_id == 1
        assert chunk.header.payload_size == len(chunk.data)
        assert chunk.header.last == chunk.is_last_file_chunk()
        if not chunk.is_last_file_chunk():
            assert chunk.header.payload_size == 1024 * 8
        else:
            assert chunk.header.payload_size <= 1024 * 8

        bytes_serialized = chunk.serialize()
        
        client_id = int.from_bytes(bytes_serialized[0:4], byteorder='big')
        payload_size = int.from_bytes(bytes_serialized[4:8], byteorder='big')
        last = bytes_serialized[8:9] == b'1'
        var_header_size = int.from_bytes(bytes_serialized[9:13], byteorder='big')
        rel_path = bytes_serialized[13:13+var_header_size].decode('utf-8')

        assert client_id == chunk.header.client_id
        assert payload_size == chunk.header.payload_size
        assert last == chunk.header.last
        assert rel_path == chunk.header.rel_path
        assert var_header_size == chunk.header.var_header_size
        
        chunk_header = FileChunkHeader(rel_path, client_id, payload_size, last)

        payload = bytes_serialized[13+var_header_size:13+var_header_size+payload_size]
        assert payload == chunk.data
        chunk_readed = FileChunk(rel_path, client_id, last, payload)
        
        assert chunk_readed.header.client_id == chunk.header.client_id
        assert chunk_readed.header.payload_size == chunk.header.payload_size
        assert chunk_readed.header.last == chunk.header.last
        assert chunk_readed.header.rel_path == chunk.header.rel_path
        assert chunk_readed.data == chunk.data

        process_chunk = ProcessBatchReader.from_file_rows(chunk_readed.data, chunk_readed.header.rel_path, chunk_readed.header.client_id)
        table_type = process_chunk.table_type()
        tables_acum[table_type] += 1
    
    assert tables_acum[TableType.STORES] == 1
    assert tables_acum[TableType.USERS] == 24
    assert tables_acum[TableType.MENU_ITEMS] == 1
    assert tables_acum[TableType.TRANSACTIONS] == 24
    assert tables_acum[TableType.TRANSACTION_ITEMS] == 24
""" 
