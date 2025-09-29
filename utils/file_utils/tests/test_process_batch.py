import datetime
import pytest
from process_batch import ProcessBatch, ProcessBatchHeader
from file_table import TransactionsFileRow
from process_table import TransactionsProcessRow
from table_type import TableType

def test_process_batch_header_serialize_deserialize():
    header = ProcessBatchHeader(123, TableType.TRANSACTIONS, 456)
    serialized = header.serialize()
    deserialized = ProcessBatchHeader.deserialize(serialized)
    assert deserialized.client_id == 123
    assert deserialized.table_type == TableType.TRANSACTIONS
    assert deserialized.size == 456

def test_process_batch_serialize_deserialize():
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, datetime.date(2023, 5, 1))
    process_row = TransactionsProcessRow.from_file_row(row)
    batch = ProcessBatch([process_row], TableType.TRANSACTIONS, client_id=999)
    serialized = batch.serialize()
    header = ProcessBatchHeader.deserialize(serialized[:12])
    assert header.client_id == 999
    assert header.table_type == TableType.TRANSACTIONS
    assert header.size == len(serialized) - 12
    deserialized = ProcessBatch.deserialize(serialized[12:], header)
    assert len(deserialized.rows) == 1
    assert deserialized.rows[0].store_id == 1

def test_process_batch_from_file_rows():
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, datetime.date(2023, 5, 1))
    serialized = row.serialize()
    batch = ProcessBatch.from_file_rows(serialized, "/data/transactions/tx.csv", client_id=111)
    assert len(batch.rows) == 1
    assert batch.rows[0].store_id == 1

def test_process_batch_from_file_rows_empty():
    with pytest.raises(ValueError):
        ProcessBatch.from_file_rows(b"", "/data/transactions/tx.csv", client_id=111)
