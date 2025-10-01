import datetime
import pytest
from ..process_chunk import ProcessChunk, ProcessChunkHeader
from ..file_table import TransactionsFileRow, UsersFileRow, DateTime
from ..process_table import TransactionsProcessRow
from ..table_type import TableType

def test_process_chunk_header_serialize_deserialize():
    header = ProcessChunkHeader(123, TableType.TRANSACTIONS, 456)
    serialized = header.serialize()
    deserialized = ProcessChunkHeader.deserialize(serialized)
    assert deserialized.client_id == 123
    assert deserialized.table_type == TableType.TRANSACTIONS
    assert deserialized.size == 456

def test_process_chunk_serialize_deserialize():
    
    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
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
    assert deserialized.rows[0].transaction_id == "tx1"
    assert deserialized.rows[0].user_id == 4
    assert deserialized.rows[0].final_amount == 100
    assert deserialized.rows[0].created_at.date == datetime.date(2023, 5, 1)
    assert deserialized.rows[0].created_at.time == datetime.time(0, 0)
    assert str(deserialized.rows[0].year_half_created_at) == "2023-H1"
