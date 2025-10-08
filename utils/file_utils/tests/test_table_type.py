import pytest
from ..table_type import TableType

def test_table_type_from_path_valid():
    assert TableType.from_path("transactions/transactions_2023.csv") == TableType.TRANSACTIONS
    assert TableType.from_path("transaction_items/transaction_items_202401.csv") ==  TableType.TRANSACTION_ITEMS
    assert TableType.from_path("client-1/menu_items/menu_items.csv") == TableType.MENU_ITEMS
    assert TableType.from_path("stores/stores_data.csv") == TableType.STORES
    assert TableType.from_path("users/users_list.csv") == TableType.USERS

def test_table_type_from_path_invalid():
    with pytest.raises(ValueError):
        TableType.from_path("unknown/file.csv")
