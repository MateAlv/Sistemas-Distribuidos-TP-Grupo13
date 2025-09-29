import datetime
from file_table import *
from table_type import TableType

class YearHalf:
    def __init__(self, year: int, half: int):
        self.year = year
        self.half = half

    def __str__(self):
        return f"{self.year}-H{self.half}"

    @staticmethod
    def from_date(date: datetime.date):
        return YearHalf(date.year, 1 if date.month <= 6 else 2)

class MonthYear:
    def __init__(self, month: int, year: int):
        self.month = month
        self.year = year

    def __str__(self):
        return f"{self.month:02d}-{self.year}"

    @staticmethod
    def from_date(date: datetime.date):
        return MonthYear(date.month, date.year)

# =========================================
# BASE PROCESS ROW
# =========================================
class TableProcessRow:
    def serialize(self) -> bytes:
        raise NotImplementedError

    def from_file_row(file_row: TableFileRow):
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes):
        raise NotImplementedError

# =========================================
# Transactions Process Row
# - Representa una fila del procesamiento de transacciones.
# - Campos: 
#       - transaction_id: String
#       - store_id: int
#       - user_id: int
#       - final_amount: float
#       - created_at: Date
#       - year_half_created_at: YearHalf
# =========================================
class TransactionsProcessRow(TableProcessRow):
    def __init__(self, transaction_id, store_id, final_amount, created_at: datetime.date):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.final_amount = final_amount
        self.created_at = created_at
        self.year_half_created_at = YearHalf.from_date(created_at)

    def serialize(self) -> bytes:
        return f"{self.transaction_id};{self.store_id};{self.final_amount};{self.created_at.isoformat()};{self.year_half_created_at}\n".encode("utf-8")

    def from_file_row(file_row: TransactionsFileRow):
        return TransactionsProcessRow(
            file_row.transaction_id,
            file_row.store_id,
            file_row.final_amount,
            file_row.created_at
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = TransactionsProcessRow(
            parts[0],
            int(parts[1]),
            float(parts[2]),
            datetime.date.fromisoformat(parts[3]),
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed

# =========================================
# Transactions items Process Row
# - Representa una fila del procesamiento de items por transaccion.
# - Campos: 
#       - transaction_id: String
#       - item_id: int
#       - quantity: int
#       - subtotal: float
#       - created_at: Date
#       - month_year_created_at: MonthYear
# =========================================

class TransactionsItemsProcessRow(TableProcessRow):
    def __init__(self, transaction_id, item_id, quantity, subtotal, created_at: datetime.date):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.subtotal = subtotal
        self.created_at = created_at
        self.month_year_created_at = MonthYear.from_date(created_at)
        
    def serialize(self) -> bytes:
        return f"{self.transaction_id};{self.item_id};{self.quantity};{self.subtotal};{self.created_at.isoformat()};{self.month_year_created_at}\n".encode("utf-8")

    def from_file_row(file_row: TransactionsItemsFileRow):
        return TransactionsItemsProcessRow(
            file_row.transaction_id,
            file_row.item_id,
            file_row.quantity,
            file_row.subtotal,
            file_row.created_at
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = TransactionsItemsProcessRow(
            parts[0],
            int(parts[1]),
            int(parts[2]),
            float(parts[3]),
            datetime.date.fromisoformat(parts[4])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
    
# =========================================
# Menu items Process Row
# - Representa una fila del procesamiento de items del menu.
# - Campos:
#       - item_id: int
#       - item_name: String
# =========================================

class MenuItemsProcessRow(TableProcessRow):
    def __init__(self, item_id, item_name):
        self.item_id = item_id
        self.item_name = item_name

    def serialize(self) -> bytes:
        return f"{self.item_id};{self.item_name}\n".encode("utf-8")

    def from_file_row(file_row: MenuItemsFileRow):
        return MenuItemsProcessRow(
            file_row.item_id,
            file_row.item_name
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = MenuItemsProcessRow(
            int(parts[0]),
            parts[1]
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
# =========================================
# Stores Process Row
# - Representa una fila del procesamiento de stores.
# - Campos:
#       - store_id: int
#       - store_name: String
# =========================================

class StoresProcessRow(TableProcessRow):
    def __init__(self, store_id, store_name):
        self.store_id = store_id
        self.store_name = store_name
        
    def serialize(self) -> bytes:
        return f"{self.store_id};{self.store_name}\n".encode("utf-8")

    def from_file_row(file_row: StoresFileRow):
        return StoresProcessRow(
            file_row.store_id,
            file_row.store_name
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = StoresProcessRow(
            int(parts[0]),
            parts[1]
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
    
# =========================================
# Users Process Row
# - Representa una fila del procesamiento de users.
# - Campos:
#       - user_id: int
#       - birth_date: Date
# =========================================

class UsersProcessRow(TableProcessRow):
    def __init__(self, user_id, birthdate: datetime.date):
        self.user_id = user_id
        self.birthdate = birthdate
        
    def serialize(self) -> bytes:
        return f"{self.user_id};{self.birthdate.isoformat()}\n".encode("utf-8")

    def from_file_row(file_row: UsersFileRow):
        return UsersProcessRow(
            file_row.user_id,
            file_row.birthdate
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = UsersProcessRow(
            int(parts[0]),
            datetime.date.fromisoformat(parts[1])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
# =========================================