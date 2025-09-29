import datetime
import pytest
from file_table import (
    TransactionsFileRow,
    TransactionsItemsFileRow,
    MenuItemsFileRow,
    StoresFileRow,
    UsersFileRow
)

def test_transactions_file_row_serialize_deserialize():
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100.0, 10.0, 90.0, datetime.date(2023, 5, 10))
    serialized = row.serialize()
    deserialized, consumed = TransactionsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx1"
    assert deserialized.store_id == 1
    assert deserialized.payment_method_id == 2
    assert deserialized.voucher_id == 3
    assert deserialized.user_id == 4
    assert deserialized.original_amount == 100.0
    assert deserialized.discount_applied == 10.0
    assert deserialized.final_amount == 90.0
    assert deserialized.created_at == datetime.date(2023, 5, 10)
    assert consumed == len(serialized)

def test_transactions_items_file_row_serialize_deserialize():
    row = TransactionsItemsFileRow("tx2", 10, 2, 50.0, 100.0, datetime.date(2023, 6, 1))
    serialized = row.serialize()
    deserialized, consumed = TransactionsItemsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx2"
    assert deserialized.item_id == 10
    assert deserialized.quantity == 2
    assert deserialized.unit_price == 50.0
    assert deserialized.subtotal == 100.0
    assert deserialized.created_at == datetime.date(2023, 6, 1)
    assert consumed == len(serialized)

def test_menu_items_file_row_serialize_deserialize():
    row = MenuItemsFileRow(1, "Burger", "Fast Food", 5.0, True, datetime.date(2023, 1, 1), datetime.date(2023, 12, 31))
    serialized = row.serialize()
    deserialized, consumed = MenuItemsFileRow.deserialize(serialized)
    assert deserialized.item_id == 1
    assert deserialized.item_name == "Burger"
    assert deserialized.category == "Fast Food"
    assert deserialized.price == 5.0
    assert deserialized.is_seasonal is True
    assert deserialized.available_from == datetime.date(2023, 1, 1)
    assert deserialized.available_to == datetime.date(2023, 12, 31)
    assert consumed == len(serialized)

def test_stores_file_row_serialize_deserialize():
    row = StoresFileRow(1, "StoreX", "Main St", "City", "State", -34.6, -58.4)
    serialized = row.serialize()
    deserialized, consumed = StoresFileRow.deserialize(serialized)
    assert deserialized.store_name == "StoreX"
    assert deserialized.store_id == 1
    assert deserialized.street == "Main St"
    assert deserialized.city == "City"
    assert deserialized.state == "State"
    assert deserialized.longitude == -58.4
    assert deserialized.latitude == -34.6
    assert consumed == len(serialized)

def test_users_file_row_serialize_deserialize():
    row = UsersFileRow(1, "Male", datetime.date(1990, 1, 1), datetime.date(2020, 1, 1))
    serialized = row.serialize()
    deserialized, consumed = UsersFileRow.deserialize(serialized)
    assert deserialized.user_id == 1
    assert deserialized.gender == "Male"
    assert deserialized.registration_at == datetime.date(2020, 1, 1)
    assert deserialized.birthdate == datetime.date(1990, 1, 1)
    assert consumed == len(serialized)
