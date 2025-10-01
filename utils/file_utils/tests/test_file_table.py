import datetime
import pytest
from ..file_table import (
    TransactionsFileRow,
    TransactionsItemsFileRow,
    MenuItemsFileRow,
    StoresFileRow,
    UsersFileRow,
    DateTime
)

def test_transactions_file_row_serialize_deserialize():
    date = DateTime(datetime.date(2023, 5, 10), datetime.time(14, 30))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100.0, 10.0, 90.0, date)
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
    assert deserialized.created_at.date == datetime.date(2023, 5, 10)
    assert deserialized.created_at.time == datetime.time(14, 30)
    assert consumed == len(serialized)

def test_transactions_items_file_row_serialize_deserialize():
    date = DateTime(datetime.date(2023, 6, 1), datetime.time(10, 0))
    row = TransactionsItemsFileRow("tx2", 10, 2, 50.0, 100.0, date)
    serialized = row.serialize()
    deserialized, consumed = TransactionsItemsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx2"
    assert deserialized.item_id == 10
    assert deserialized.quantity == 2
    assert deserialized.unit_price == 50.0
    assert deserialized.subtotal == 100.0
    assert deserialized.created_at.date == datetime.date(2023, 6, 1)
    assert deserialized.created_at.time == datetime.time(10, 0)
    assert consumed == len(serialized)

def test_menu_items_file_row_serialize_deserialize():
    date_from = DateTime(datetime.date(2023, 1, 1), datetime.time(0, 0))
    date_to = DateTime(datetime.date(2023, 12, 31), datetime.time(23, 59))
    row = MenuItemsFileRow(1, "Burger", "Fast Food", 5.0, True, date_from, date_to)
    serialized = row.serialize()
    deserialized, consumed = MenuItemsFileRow.deserialize(serialized)
    assert deserialized.item_id == 1
    assert deserialized.item_name == "Burger"
    assert deserialized.category == "Fast Food"
    assert deserialized.price == 5.0
    assert deserialized.is_seasonal is True
    assert deserialized.available_from.date == datetime.date(2023, 1, 1)
    assert deserialized.available_from.time == datetime.time(0, 0)
    assert deserialized.available_to.date == datetime.date(2023, 12, 31)
    assert deserialized.available_to.time == datetime.time(23, 59)
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
    register_at = DateTime(datetime.date(2020, 1, 1), datetime.time(0, 0))
    row = UsersFileRow(1, "Male", datetime.date(1990, 1, 1), register_at)
    serialized = row.serialize()
    deserialized, consumed = UsersFileRow.deserialize(serialized)
    assert deserialized.user_id == 1
    assert deserialized.gender == "Male"
    assert deserialized.registration_at.date == datetime.date(2020, 1, 1)
    assert deserialized.registration_at.time == datetime.time(0, 0)
    assert deserialized.birthdate == datetime.date(1990, 1, 1)
    assert consumed == len(serialized)
    
def test_transactions_file_row_empty_field_serialize():
    # Discount applied is None
    date = DateTime(datetime.date(2023, 5, 10), datetime.time(14, 30))
    row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100.0, None, 90.0, date)
    serialized = row.serialize()
    assert serialized == b"tx1;1;2;3;4;100.0;;90.0;2023-05-10 14:30:00\n"

def test_transactions_file_row_empty_field_deserialize():
    # Discount applied is empty
    row_text = "tx1;1;2;3;4;100.0;;90.0;2023-05-10 14:30:00"

    serialized = row_text.encode() + b"\n"
    deserialized, consumed = TransactionsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx1"
    assert deserialized.store_id == 1
    assert deserialized.payment_method_id == 2
    assert deserialized.voucher_id == 3
    assert deserialized.user_id == 4
    assert deserialized.original_amount == 100.0
    assert deserialized.discount_applied == None
    assert deserialized.final_amount == 90.0
    assert deserialized.created_at.date == datetime.date(2023, 5, 10)
    assert deserialized.created_at.time == datetime.time(14, 30)

    assert consumed == len(row_text.encode()) + 1

def test_transactions_file_row_multiple_empty_fields_serialize():
    # Discount applied and voucher_id are None
    date = DateTime(datetime.date(2023, 5, 10), datetime.time(14, 30))
    row = TransactionsFileRow("tx1", 1, 2, None, 4, 100.0, None, 90.0, date)
    serialized = row.serialize()
    assert serialized == b"tx1;1;2;;4;100.0;;90.0;2023-05-10 14:30:00\n"
    
def test_transactions_file_row_multiple_empty_fields_deserialize():
    # Discount applied and voucher_id are empty
    row_text = "tx1;1;2;;4;100.0;;90.0;2023-05-10 14:30:00"

    serialized = row_text.encode() + b"\n"
    deserialized, consumed = TransactionsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx1"
    assert deserialized.store_id == 1
    assert deserialized.payment_method_id == 2
    assert deserialized.voucher_id == None
    assert deserialized.user_id == 4
    assert deserialized.original_amount == 100.0
    assert deserialized.discount_applied == None
    assert deserialized.final_amount == 90.0
    assert deserialized.created_at.date == datetime.date(2023, 5, 10)
    assert deserialized.created_at.time == datetime.time(14, 30)

    assert consumed == len(row_text.encode()) + 1

def test_transactions_file_row_all_empty_fields_serialize():
    # All optional fields are None
    date = DateTime(datetime.date(2023, 5, 10), datetime.time(11, 30))
    row = TransactionsFileRow("tx1", 1, 2, None, None, 100.0, None, None, date)
    serialized = row.serialize()
    assert serialized == b"tx1;1;2;;;100.0;;;2023-05-10 11:30:00\n"
    
def test_transactions_file_row_all_empty_fields_deserialize():
    # All optional fields are empty
    row_text = "tx1;1;2;;;100.0;;;2023-05-10 08:23:32"

    serialized = row_text.encode() + b"\n"
    deserialized, consumed = TransactionsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx1"
    assert deserialized.store_id == 1
    assert deserialized.payment_method_id == 2
    assert deserialized.voucher_id == None
    assert deserialized.user_id == None
    assert deserialized.original_amount == 100.0
    assert deserialized.discount_applied == None
    assert deserialized.final_amount == None
    assert deserialized.created_at.date == datetime.date(2023, 5, 10)
    assert deserialized.created_at.time == datetime.time(8, 23, 32)

    assert consumed == len(row_text.encode()) + 1

# --- TransactionsItemsFileRow ---

def test_transactions_items_file_row_empty_field_serialize():
    # subtotal is None
    date = DateTime(datetime.date(2023, 6, 1), datetime.time(23, 59, 59))
    row = TransactionsItemsFileRow("tx2", 10, 2, 50.0, None, date)
    serialized = row.serialize()
    assert serialized == b"tx2;10;2;50.0;;2023-06-01 23:59:59\n"

def test_transactions_items_file_row_empty_field_deserialize():
    row_text = "tx2;10;2;50.0;;2023-06-01 23:59:59"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = TransactionsItemsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx2"
    assert deserialized.item_id == 10
    assert deserialized.quantity == 2
    assert deserialized.unit_price == 50.0
    assert deserialized.subtotal == None
    assert deserialized.created_at.date == datetime.date(2023, 6, 1)
    assert deserialized.created_at.time == datetime.time(23, 59, 59)
    assert consumed == len(row_text.encode()) + 1

def test_transactions_items_file_row_multiple_empty_fields_serialize():
    # subtotal and unit_price are None
    date = DateTime(datetime.date(2023, 6, 1), datetime.time(0, 0))
    row = TransactionsItemsFileRow("tx2", 10, 2, None, None, date)
    serialized = row.serialize()
    assert serialized == b"tx2;10;2;;;" + b"2023-06-01 00:00:00\n"

def test_transactions_items_file_row_multiple_empty_fields_deserialize():
    row_text = "tx2;10;2;;;2023-06-01 03:00:00"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = TransactionsItemsFileRow.deserialize(serialized)
    assert deserialized.transaction_id == "tx2"
    assert deserialized.item_id == 10
    assert deserialized.quantity == 2
    assert deserialized.unit_price == None
    assert deserialized.subtotal == None
    assert deserialized.created_at.date == datetime.date(2023, 6, 1)
    assert deserialized.created_at.time == datetime.time(3, 0, 0)
    assert consumed == len(row_text.encode()) + 1

# --- MenuItemsFileRow ---

def test_menu_items_file_row_empty_field_serialize():
    # available_to is None
    date = DateTime(datetime.date(2023, 1, 1), datetime.time(9, 0))
    row = MenuItemsFileRow(1, "Burger", "Fast Food", 5.0, True, date, None)
    serialized = row.serialize()
    assert serialized == b"1;Burger;Fast Food;5.0;True;2023-01-01 09:00:00;\n"

def test_menu_items_file_row_empty_field_deserialize():
    row_text = "1;Burger;Fast Food;5.0;True;2023-01-01 09:00:00;"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = MenuItemsFileRow.deserialize(serialized)
    assert deserialized.item_id == 1
    assert deserialized.item_name == "Burger"
    assert deserialized.category == "Fast Food"
    assert deserialized.price == 5.0
    assert deserialized.is_seasonal is True
    assert deserialized.available_from.date == datetime.date(2023, 1, 1)
    assert deserialized.available_from.time == datetime.time(9, 0)
    assert deserialized.available_to == None
    assert consumed == len(row_text.encode()) + 1

def test_menu_items_file_row_multiple_empty_fields_serialize():
    # available_from and available_to are None
    row = MenuItemsFileRow(1, "Burger", "Fast Food", 5.0, True, None, None)
    serialized = row.serialize()
    assert serialized == b"1;Burger;Fast Food;5.0;True;;\n"

def test_menu_items_file_row_multiple_empty_fields_deserialize():
    row_text = "1;Burger;Fast Food;5.0;True;;"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = MenuItemsFileRow.deserialize(serialized)
    assert deserialized.item_id == 1
    assert deserialized.item_name == "Burger"
    assert deserialized.category == "Fast Food"
    assert deserialized.price == 5.0
    assert deserialized.is_seasonal is True
    assert deserialized.available_from == None
    assert deserialized.available_to == None
    assert consumed == len(row_text.encode()) + 1

# --- StoresFileRow ---

def test_stores_file_row_empty_field_serialize():
    # latitude is None
    row = StoresFileRow(1, "StoreX", "Main St", "City", "State", None, -58.4)
    serialized = row.serialize()
    assert serialized == b"1;StoreX;Main St;City;State;;-58.4\n"

def test_stores_file_row_empty_field_deserialize():
    row_text = "1;StoreX;Main St;City;State;-58.4;;"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = StoresFileRow.deserialize(serialized)
    assert deserialized.store_id == 1
    assert deserialized.store_name == "StoreX"
    assert deserialized.street == "Main St"
    assert deserialized.city == "City"
    assert deserialized.state == "State"
    assert deserialized.latitude == -58.4
    assert deserialized.longitude == None
    assert consumed == len(row_text.encode()) + 1

def test_stores_file_row_multiple_empty_fields_serialize():
    # latitude and longitude are None
    row = StoresFileRow(1, "StoreX", "Main St", "City", "State", None, None)
    serialized = row.serialize()
    assert serialized == b"1;StoreX;Main St;City;State;;\n"

def test_stores_file_row_multiple_empty_fields_deserialize():
    row_text = "1;StoreX;Main St;City;State;;"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = StoresFileRow.deserialize(serialized)
    assert deserialized.store_id == 1
    assert deserialized.store_name == "StoreX"
    assert deserialized.street == "Main St"
    assert deserialized.city == "City"
    assert deserialized.state == "State"
    assert deserialized.longitude == None
    assert deserialized.latitude == None
    assert consumed == len(row_text.encode()) + 1

# --- UsersFileRow ---

def test_users_file_row_empty_field_serialize():
    # birthdate is None
    register_at = DateTime(datetime.date(2020, 1, 1), datetime.time(0, 0))
    row = UsersFileRow(1, "Male", None, register_at)
    serialized = row.serialize()
    assert serialized == b"1;Male;;2020-01-01 00:00:00\n"

def test_users_file_row_empty_field_deserialize():
    row_text = "1;Male;;2020-01-01 05:23:00"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = UsersFileRow.deserialize(serialized)
    assert deserialized.user_id == 1
    assert deserialized.gender == "Male"
    assert deserialized.birthdate == None
    assert deserialized.registration_at.date == datetime.date(2020, 1, 1)
    assert deserialized.registration_at.time == datetime.time(5, 23, 0)
    assert consumed == len(row_text.encode()) + 1

def test_users_file_row_multiple_empty_fields_serialize():
    # birthdate and registration_at are None
    row = UsersFileRow(1, "Male", None, None)
    serialized = row.serialize()
    assert serialized == b"1;Male;;\n"

def test_users_file_row_multiple_empty_fields_deserialize():
    row_text = "1;Male;;"
    serialized = row_text.encode() + b"\n"
    deserialized, consumed = UsersFileRow.deserialize(serialized)
    assert deserialized.user_id == 1
    assert deserialized.gender == "Male"
    assert deserialized.birthdate == None
    assert deserialized.registration_at == None
    assert consumed == len(row_text.encode()) + 1

