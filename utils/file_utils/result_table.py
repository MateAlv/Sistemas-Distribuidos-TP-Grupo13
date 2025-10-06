from .table_type import ResultTableType
from .file_table import CSV_DELIMITER
from .process_table import YearHalf
from datetime import date

# =========================================
# BASE RESULT ROW
# =========================================
class TableResultRow:
    def serialize(self) -> bytes:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes):
        raise NotImplementedError
    

# =========================================
# QUERY 1
# =========================================
class Query1ResultRow(TableResultRow):
    def __init__(self, transaction_id: str, final_amount: float):
        self.transaction_id = transaction_id
        self.final_amount = final_amount

    def serialize(self) -> bytes:
        return f"{self.transaction_id},{str(self.final_amount)}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        transaction_id = parts[0] if len(parts) > 0 and parts[0] else None
        final_amount = float(parts[1]) if len(parts) > 1 and parts[1] else None

        row = Query1ResultRow(transaction_id, final_amount)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 2.1
# =========================================
class Query2_1ResultRow(TableResultRow):
    def __init__(self, product_id: int, product_name: str, quantity: int):
        self.product_id = product_id
        self.product_name = product_name
        self.quantity = quantity

    def serialize(self) -> bytes:
        return f"{self.product_id},{self.product_name},{self.quantity}\n".encode("utf-8")
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        product_id = int(parts[0]) if len(parts) > 0 and parts[0] else None
        product_name = parts[1] if len(parts) > 1 and parts[1] else None
        quantity = int(parts[2]) if len(parts) > 2 and parts[2] else None

        row = Query2_1ResultRow(product_id, product_name, quantity)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 2.2
# =========================================
class Query2_2ResultRow(TableResultRow):
    def __init__(self, product_id: int, product_name: str, profit_sum: float):
        self.product_id = product_id
        self.product_name = product_name
        self.profit_sum = profit_sum

    def serialize(self) -> bytes:
        return f"{self.product_id},{self.product_name},{self.profit_sum}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        product_id = int(parts[0]) if len(parts) > 0 and parts[0] else None
        product_name = parts[1] if len(parts) > 1 and parts[1] else None
        profit_sum = float(parts[2]) if len(parts) > 2 and parts[2] else None

        row = Query2_2ResultRow(product_id, product_name, profit_sum)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 3
# =========================================
class Query3ResultRow(TableResultRow):
    def __init__(self, year_half: YearHalf, store_name: str, tpv: float):
        self.year_half = year_half
        self.store_name = store_name
        self.tpv = tpv

    def serialize(self) -> bytes:
        return f"{self.year_half},{self.store_name},{self.tpv}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        year_half_string = parts[0] if len(parts) > 0 and parts[0] else None
        store_name = parts[1] if len(parts) > 1 and parts[1] else None
        tpv = float(parts[2]) if len(parts) > 2 and parts[2] else None

        year_half = YearHalf.from_str(year_half_string) if year_half_string else None
        row = Query3ResultRow(year_half, store_name, tpv)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 4
# =========================================
class Query4ResultRow(TableResultRow):
    def __init__(self, store_name: str, birth_date: date, purchase_quantity: int):
        self.store_name = store_name
        self.birth_date = birth_date
        self.purchase_quantity = purchase_quantity

    def serialize(self) -> bytes:
        return f"{self.store_name},{self.birth_date.isoformat()},{self.purchase_quantity}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        store_name = parts[0] if len(parts) > 0 and parts[0] else None
        birth_date = date.fromisoformat(parts[1]) if len(parts) > 1 and parts[1] else None
        purchase_quantity = int(parts[2]) if len(parts) > 2 and parts[2] else None

        row = Query4ResultRow(store_name, birth_date, purchase_quantity)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
