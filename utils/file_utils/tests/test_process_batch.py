from file_table import UsersFileRow
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

def test_process_batch_from_many_file_rows():
    rows = [
        UsersFileRow(1, "M", datetime.date(1990, 1, 1), datetime.date(2020, 1, 1)),
        UsersFileRow(2, "F", datetime.date(1985, 6, 15), datetime.date(2019, 3, 20)),
        UsersFileRow(3, "M", datetime.date(1992, 2, 2), datetime.date(2021, 2, 2)),
        UsersFileRow(4, "F", datetime.date(1988, 3, 3), datetime.date(2020, 3, 3)),
        UsersFileRow(5, "M", datetime.date(1995, 4, 4), datetime.date(2022, 4, 4)),
        UsersFileRow(6, "F", datetime.date(1991, 5, 5), datetime.date(2018, 5, 5)),
    ]
    
    serialized = b"".join(r.serialize() for r in rows)
    batch = ProcessBatch.from_file_rows(serialized, "/data/users/tx.csv", client_id=111)
    
    assert batch.header.client_id == 111
    assert batch.header.table_type == TableType.USERS
    assert len(batch.rows) == 6
    assert batch.rows[0].user_id == 1
    assert batch.rows[1].user_id == 2
    assert batch.rows[2].user_id == 3
    assert batch.rows[3].user_id == 4
    assert batch.rows[4].user_id == 5
    assert batch.rows[5].user_id == 6

def test_process_batch_serialize_deserialize_many():
    rows = [
        UsersFileRow(1, "M", datetime.date(1990, 1, 1), datetime.date(2020, 1, 1)),
        UsersFileRow(2, "F", datetime.date(1985, 6, 15), datetime.date(2019, 3, 20)),
        UsersFileRow(3, "M", datetime.date(1992, 2, 2), datetime.date(2021, 2, 2)),
        UsersFileRow(4, "F", datetime.date(1988, 3, 3), datetime.date(2020, 3, 3)),
        UsersFileRow(5, "M", datetime.date(1995, 4, 4), datetime.date(2022, 4, 4)),
        UsersFileRow(6, "F", datetime.date(1991, 5, 5), datetime.date(2018, 5, 5)),
    ]
    
    serialized = b"".join(r.serialize() for r in rows)
    batch = ProcessBatch.from_file_rows(serialized, "/data/users/tx.csv", client_id=111)
    serialized_batch = batch.serialize()
    
    header = ProcessBatchHeader.deserialize(serialized_batch[:12])
    assert header.client_id == 111
    assert header.table_type == TableType.USERS
    assert header.size == len(serialized_batch) - 12
    
    deserialized_batch = ProcessBatch.deserialize(serialized_batch[12:], header)
    assert len(deserialized_batch.rows) == 6
    assert deserialized_batch.rows[0].user_id == 1
    assert deserialized_batch.rows[1].user_id == 2
    assert deserialized_batch.rows[2].user_id == 3
    assert deserialized_batch.rows[3].user_id == 4
    assert deserialized_batch.rows[4].user_id == 5
    assert deserialized_batch.rows[5].user_id == 6

def test_process_batch_from_file_rows_empty():
    with pytest.raises(ValueError):
        ProcessBatch.from_file_rows(b"", "/data/transactions/tx.csv", client_id=111)
