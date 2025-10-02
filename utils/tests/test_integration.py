from utils.communication.batch_reader import BatchReader
from utils.communication.file_chunk import FileChunk, FileChunkHeader
from utils.file_utils.process_batch_reader import ProcessBatchReader


TEST_PATH = "./utils/tests/test_data"

def test_integration():
    # Lector de batches
    reader = BatchReader(
        client_id=int(1),
        root=TEST_PATH,
        max_batch_size=1024,
    )
    
    for chunk in reader.iter():
        assert isinstance(chunk, FileChunk)
        assert isinstance(chunk.header, FileChunkHeader)
        assert chunk.header.client_id == 1
        assert chunk.header.payload_size == len(chunk.data)
        assert chunk.header.rel_path == "stores/stores.csv"
        assert chunk.header.last == chunk.is_last_file_chunk()
        if chunk.header.last:
            assert chunk.header.payload_size <= 1024
        else:
            assert chunk.header.payload_size == 1024

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
        
        process_chunk = ProcessBatchReader.from_file_rows(chunk_readed.payload, chunk_readed.header.rel_path, chunk_readed.header.client_id)
        assert process_chunk.table_type == TableType.STORES
        assert len(process_chunk.rows) == 10
        