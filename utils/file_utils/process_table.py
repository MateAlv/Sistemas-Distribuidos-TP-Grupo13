import datetime
from .file_table import *
from .table_type import TableType

class YearHalf:
    def __init__(self, year: int, half: int):
        self.year = year
        self.half = half

    def __str__(self):
        return f"{self.year}-H{self.half}"

    @staticmethod
    def from_date(date: datetime.date):
        return YearHalf(date.year, 1 if date.month <= 6 else 2)
    
    @staticmethod
    def from_str(string: str):
        try:
            year_part, half_part = string.split('-H')
            year = int(year_part)
            half = int(half_part)
            if half not in (1, 2):
                raise ValueError("El valor de 'half' debe ser 1 o 2.")
            return YearHalf(year, half)
        except Exception as e:
            raise ValueError(f"Formato inválido para YearHalf: '{string}'. Debe ser 'YYYY-H1' o 'YYYY-H2'.") from e

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
    def __init__(self, transaction_id: str, store_id: int, user_id: int, final_amount: float, created_at: DateTime):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.user_id = user_id
        self.final_amount = final_amount
        self.created_at = created_at
        self.year_half_created_at = YearHalf.from_date(created_at.date)

    def serialize(self) -> bytes:
        transaction_id_str = self.transaction_id if self.transaction_id is not None else ""
        store_id_str = str(self.store_id) if self.store_id is not None else ""
        user_id_str = str(self.user_id) if self.user_id is not None else ""
        final_amount_str = str(self.final_amount) if self.final_amount is not None else ""
        created_at_str = str(self.created_at) if self.created_at is not None else ""
        year_half_created_at_str = str(self.year_half_created_at) if self.year_half_created_at is not None else ""

        return f"{transaction_id_str},{store_id_str},{user_id_str},{final_amount_str},{created_at_str},{year_half_created_at_str}\n".encode("utf-8")

    def from_file_row(file_row: TransactionsFileRow):
        return TransactionsProcessRow(
            file_row.transaction_id,
            file_row.store_id,
            file_row.user_id,
            file_row.final_amount,
            file_row.created_at
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)
        
        trans_id = parts[0] if len(parts[0]) > 0 else None
        store_id = int(float(parts[1])) if len(parts[1]) > 0 else None
        user_id = int(float(parts[2])) if len(parts[2]) > 0 else None
        final_amount = float(parts[3]) if len(parts[3]) > 0 else None
        created_at = DateTime.from_string(parts[4]) if len(parts[4]) > 0 else None

        row = TransactionsProcessRow(trans_id, store_id, user_id, final_amount, created_at)
        consumed = len(line.encode("utf-8")) + 1
        
        return row, consumed

# =========================================
# Transactions items Process Row
# - Representa una fila del procesamiento de items por transaccion.
# - Campos: 
#       - transaction_id: String
#       - item_id: int
#       - quantity: int -> SUM
#       - subtotal: float -> SUM
#       - created_at: Date
#       - month_year_created_at: MonthYear
# =========================================

class TransactionItemsProcessRow(TableProcessRow):
    def __init__(self, transaction_id: str, item_id: int, quantity: int, subtotal: float, created_at: DateTime):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.subtotal = subtotal
        self.created_at = created_at
        self.month_year_created_at = MonthYear.from_date(created_at.date)
        
    def serialize(self) -> bytes:
        transaction_id_str = self.transaction_id if self.transaction_id is not None else ""
        item_id_str = str(self.item_id) if self.item_id is not None else ""
        quantity_str = str(self.quantity) if self.quantity is not None else ""
        subtotal_str = str(self.subtotal) if self.subtotal is not None else ""
        created_at_str = str(self.created_at) if self.created_at is not None else ""
        month_year_created_at_str = str(self.month_year_created_at) if self.month_year_created_at is not None else ""

        return f"{transaction_id_str},{item_id_str},{quantity_str},{subtotal_str},{created_at_str},{month_year_created_at_str}\n".encode("utf-8")

    def from_file_row(file_row: TransactionsItemsFileRow):
        return TransactionItemsProcessRow(
            file_row.transaction_id,
            file_row.item_id,
            file_row.quantity,
            file_row.subtotal,
            file_row.created_at
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        transaction_id = parts[0] if len(parts[0]) > 0 else None
        item_id = int(float(parts[1])) if len(parts[1]) > 0 else None
        quantity = int(float(parts[2])) if len(parts[2]) > 0 else None
        subtotal = float(parts[3]) if len(parts[3]) > 0 else None
        created_at = DateTime.from_string(parts[4]) if len(parts[4]) > 0 else None

        row = TransactionItemsProcessRow(transaction_id, item_id, quantity, subtotal, created_at)
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
    def __init__(self, item_id: int, item_name: str):
        self.item_id = item_id
        self.item_name = item_name

    def serialize(self) -> bytes:
        item_id_str = str(self.item_id) if self.item_id is not None else ""
        item_name_str = self.item_name if self.item_name is not None else ""
        
        return f"{item_id_str},{item_name_str}\n".encode("utf-8")

    def from_file_row(file_row: MenuItemsFileRow):
        return MenuItemsProcessRow(
            file_row.item_id,
            file_row.item_name
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        item_id = int(float(parts[0])) if len(parts[0]) > 0 else None
        item_name = parts[1] if len(parts[1]) > 0 else None

        row = MenuItemsProcessRow(item_id, item_name)
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
    def __init__(self, store_id: int, store_name: str):
        self.store_id = store_id
        self.store_name = store_name
        
    def serialize(self) -> bytes:
        store_id_str = str(self.store_id) if self.store_id is not None else ""
        store_name_str = self.store_name if self.store_name is not None else ""
        
        return f"{store_id_str},{store_name_str}\n".encode("utf-8")

    def from_file_row(file_row: StoresFileRow):
        return StoresProcessRow(
            file_row.store_id,
            file_row.store_name
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        store_id = int(float(parts[0])) if len(parts[0]) > 0 else None
        store_name = parts[1] if len(parts[1]) > 0 else None
        
        row = StoresProcessRow(store_id, store_name)
        
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
    def __init__(self, user_id: int, birthdate: datetime.date):
        self.user_id = user_id
        self.birthdate = birthdate
        
    def serialize(self) -> bytes:
        user_id_str = str(self.user_id) if self.user_id is not None else ""
        birthdate_str = self.birthdate.isoformat() if self.birthdate is not None else ""

        return f"{user_id_str},{birthdate_str}\n".encode("utf-8")

    def from_file_row(file_row: UsersFileRow):
        return UsersProcessRow(
            file_row.user_id,
            file_row.birthdate
        )
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        user_id = int(float(parts[0])) if len(parts[0]) > 0 else None
        birthdate = datetime.date.fromisoformat(parts[1]) if len(parts[1]) > 0 else None

        row = UsersProcessRow(user_id, birthdate)
        
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
# =========================================