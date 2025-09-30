from common.file_chunk import FileChunk, FileChunkHeader

def test_header_creation():
    header = FileChunkHeader("path/to/file.txt", 123, 456, True)
    assert header.client_id == 123
    assert header.payload_size == 456
    assert header.last is True
    assert header.rel_path == "path/to/file.txt"
    assert header.var_header_size == len("path/to/file.txt".encode('utf-8'))

def test_header_serialize_deserialize():
    header = FileChunkHeader("path/to/file.txt", 123, 456, False)
    serialized = header.serialize()
    deserialized = FileChunkHeader.deserialize(serialized)
    assert deserialized.client_id == 123
    assert deserialized.payload_size == 456
    assert deserialized.last is False
    assert deserialized.rel_path == "path/to/file.txt"
    assert deserialized.var_header_size == len("path/to/file.txt".encode('utf-8'))

def test_file_chunk_creation():
    data = b"Hello, World!"
    chunk = FileChunk("path/to/file.txt", 123, True , data)
    assert chunk.header.client_id == 123
    assert chunk.header.payload_size == len(data)
    assert chunk.header.last is True
    assert chunk.header.rel_path == "path/to/file.txt"
    assert chunk.data == data

def test_file_chunk_serialize_deserialize():
    data = b"Hello, World!"
    chunk = FileChunk("path/to/file.txt", 123 ,False, data)
    serialized = chunk.serialize()
    deserialized = FileChunk.deserialize(serialized)
    assert deserialized.header.client_id == 123
    assert deserialized.header.payload_size == len(data)
    assert deserialized.header.last is False
    assert deserialized.header.rel_path == "path/to/file.txt"
    assert deserialized.data == data