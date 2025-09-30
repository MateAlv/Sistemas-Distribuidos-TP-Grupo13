from file_table import UsersFileRow
import datetime
import pytest
from process_chunk import ProcessChunk, ProcessChunkHeader
from file_table import TransactionsFileRow
from process_table import TransactionsProcessRow
from table_type import TableType

def test_process_chunk_header_serialize_deserialize():
    header = ProcessChunkHeader(123, TableType.TRANSACTIONS, 456)
    serialized = header.serialize()
    deserialized = ProcessChunkHeader.deserialize(serialized)
    assert deserialized.client_id == 123
    assert deserialized.table_type == TableType.TRANSACTIONS
    assert deserialized.size == 456

def test_process_chunk_serialize_deserialize():
    
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, datetime.date(2023, 5, 1))
    process_row = TransactionsProcessRow.from_file_row(row)
    
    header = ProcessChunkHeader(999, TableType.TRANSACTIONS)
    batch = ProcessChunk(header, [process_row])
    serialized = batch.serialize()
    
    header = ProcessChunkHeader.deserialize(serialized[:ProcessChunkHeader.HEADER_SIZE])
    assert header.client_id == 999
    assert header.table_type == TableType.TRANSACTIONS
    assert header.size == len(serialized) - ProcessChunkHeader.HEADER_SIZE

    deserialized = ProcessChunk.deserialize(header, serialized[ProcessChunkHeader.HEADER_SIZE:])
    assert len(deserialized.rows) == 1
    assert deserialized.rows[0].store_id == 1
