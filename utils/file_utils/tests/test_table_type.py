import pytest
from ..table_type import TableType

def test_table_type_from_path_valid():
    path = "transactions/transactions_2023.csv"
    t = TableType.from_path(path)
    assert t == TableType.TRANSACTIONS

def test_table_type_from_path_invalid():
    with pytest.raises(ValueError):
        TableType.from_path("unknown/file.csv")
