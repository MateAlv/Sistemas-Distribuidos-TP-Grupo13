import datetime
import pytest
from utils.processing.process_chunk import ProcessChunkHeader
from utils.processing.process_batch_reader import ProcessBatchReader
from utils.file_utils.file_table import UsersFileRow, TransactionsFileRow, DateTime
from utils.file_utils.table_type import TableType

def test_process_batch_from_file_rows():
    date = DateTime(datetime.date(2023, 5, 1), datetime.time(0, 0))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, date)
    serialized = row.serialize()
    batch = ProcessBatchReader.from_file_rows(serialized, "transactions/transactions.csv", client_id=111)
    assert len(batch.rows) == 1
    assert batch.header.client_id == 111
    assert batch.header.table_type == TableType.TRANSACTIONS
    assert batch.header.size == len(serialized) 
    assert batch.rows[0].store_id == 1
    assert batch.rows[0].transaction_id == "tx1"
    assert batch.rows[0].user_id == 4
    assert batch.rows[0].final_amount == 100
    assert batch.rows[0].created_at.date == datetime.date(2023, 5, 1)
    assert batch.rows[0].created_at.time == datetime.time(0, 0)
    assert str(batch.rows[0].year_half_created_at) == "2023-H1"

def test_process_batch_from_many_file_rows():
    rows = [
        UsersFileRow(1, "M", datetime.date(1990, 1, 1), DateTime(datetime.date(2020, 1, 1), datetime.time(0, 0))),
        UsersFileRow(2, "F", datetime.date(1985, 6, 15), DateTime(datetime.date(2019, 3, 20), datetime.time(0, 0))),
        UsersFileRow(3, "M", datetime.date(1992, 2, 2), DateTime(datetime.date(2021, 2, 2), datetime.time(0, 0))),
        UsersFileRow(4, "F", datetime.date(1988, 3, 3), DateTime(datetime.date(2020, 3, 3), datetime.time(0, 0))),
        UsersFileRow(5, "M", datetime.date(1995, 4, 4), DateTime(datetime.date(2022, 4, 4), datetime.time(0, 0))),
        UsersFileRow(6, "F", datetime.date(1991, 5, 5), DateTime(datetime.date(2018, 5, 5), datetime.time(0, 0))),
    ]
    
    serialized = b"".join(r.serialize() for r in rows)
    batch = ProcessBatchReader.from_file_rows(serialized, "users/users.csv", client_id=111)
    
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
        UsersFileRow(1, "M", datetime.date(1990, 1, 1), DateTime(datetime.date(2020, 1, 1), datetime.time(0, 0))),
        UsersFileRow(2, "F", datetime.date(1985, 6, 15), DateTime(datetime.date(2019, 3, 20), datetime.time(0, 0))),
        UsersFileRow(3, "M", datetime.date(1992, 2, 2), DateTime(datetime.date(2021, 2, 2), datetime.time(0, 0))),
        UsersFileRow(4, "F", datetime.date(1988, 3, 3), DateTime(datetime.date(2020, 3, 3), datetime.time(0, 0))),
        UsersFileRow(5, "M", datetime.date(1995, 4, 4), DateTime(datetime.date(2022, 4, 4), datetime.time(0, 0))),
        UsersFileRow(6, "F", datetime.date(1991, 5, 5), DateTime(datetime.date(2018, 5, 5), datetime.time(0, 0))),
    ]
    
    serialized = b"".join(r.serialize() for r in rows)
    batch = ProcessBatchReader.from_file_rows(serialized, "users/users.csv", client_id=111)
    serialized_batch = batch.serialize()

    deserialized_batch = ProcessBatchReader.from_bytes(serialized_batch)
    assert deserialized_batch.header.client_id == 111
    assert deserialized_batch.header.table_type == TableType.USERS
    assert deserialized_batch.header.size == len(serialized_batch) - ProcessChunkHeader.HEADER_SIZE
    
    assert len(deserialized_batch.rows) == 6
    assert deserialized_batch.rows[0].user_id == 1
    assert deserialized_batch.rows[1].user_id == 2
    assert deserialized_batch.rows[2].user_id == 3
    assert deserialized_batch.rows[3].user_id == 4
    assert deserialized_batch.rows[4].user_id == 5
    assert deserialized_batch.rows[5].user_id == 6

def test_process_batch_from_file_rows_empty():
    with pytest.raises(ValueError):
        ProcessBatchReader.from_file_rows(b"", "transactions/transactions.csv", client_id=111)
