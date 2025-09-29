import datetime
from process_table import (
    YearHalf, MonthYear,
    TransactionsProcessRow, TransactionsItemsProcessRow,
    MenuItemsProcessRow, StoresProcessRow, UsersProcessRow
)
from file_table import (
    TransactionsFileRow, TransactionsItemsFileRow,
    MenuItemsFileRow, StoresFileRow, UsersFileRow
)

def test_year_half_from_date():
    date = datetime.date(2023, 5, 10)
    yh = YearHalf.from_date(date)
    assert str(yh) == "2023-H1"

def test_month_year_from_date():
    date = datetime.date(2023, 11, 2)
    my = MonthYear.from_date(date)
    assert str(my) == "11-2023"

def test_month_year_from_date_january():
    date = datetime.date(2023, 1, 15)
    my = MonthYear.from_date(date)
    assert str(my) == "01-2023"

def test_transactions_process_row_from_file_row():
    file_row = TransactionsFileRow("tx1", 1, 2, 3, 4, 100, 0, 100, datetime.date(2023, 2, 1))
    process_row = TransactionsProcessRow.from_file_row(file_row)
    assert process_row.transaction_id == "tx1"
    assert process_row.store_id == 1
    assert process_row.final_amount == 100
    assert process_row.created_at == datetime.date(2023, 2, 1)
    assert str(process_row.year_half_created_at) == "2023-H1"

def test_transactions_items_process_row_from_file_row():
    file_row = TransactionsItemsFileRow("tx2", 10, 2, 50.0, 100.0, datetime.date(2023, 9, 1))
    process_row = TransactionsItemsProcessRow.from_file_row(file_row)
    assert process_row.item_id == 10
    assert process_row.transaction_id == "tx2"
    assert process_row.quantity == 2
    assert process_row.subtotal == 100.0
    assert process_row.created_at == datetime.date(2023, 9, 1)
    assert str(process_row.month_year_created_at) == "09-2023"

def test_menu_items_process_row_from_file_row():
    file_row = MenuItemsFileRow(1, "Burger", "Fast Food", 5.0, True, datetime.date(2023, 1, 1), datetime.date(2023, 12, 31))
    process_row = MenuItemsProcessRow.from_file_row(file_row)
    assert process_row.item_name == "Burger"
    assert process_row.item_id == 1

def test_stores_process_row_from_file_row():
    file_row = StoresFileRow(1, "Shop", "Street", "City", "State", 10.0, 20.0)
    process_row = StoresProcessRow.from_file_row(file_row)
    assert process_row.store_name == "Shop"
    assert process_row.store_id == 1

def test_users_process_row_from_file_row():
    file_row = UsersFileRow(1, "F", datetime.date(1995, 3, 15), datetime.date(2020, 5, 1))
    process_row = UsersProcessRow.from_file_row(file_row)
    assert process_row.birthdate.year == 1995
    assert process_row.user_id == 1
