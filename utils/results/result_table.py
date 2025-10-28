from utils.file_utils.file_table import CSV_DELIMITER
from utils.processing.process_table import YearHalf, MonthYear
from datetime import date

# =========================================
# BASE RESULT ROW
# =========================================
class TableResultRow:
    def serialize(self) -> bytes:
        raise NotImplementedError

    def to_csv(self) -> str:
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

    def to_csv(self) -> str:
        return f"{self.transaction_id},{str(self.final_amount)}\n"

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
# Max sellings por producto
# =========================================
class Query2_1ResultRow(TableResultRow):
    def __init__(self, item_id: int, item_name: str, sellings_quantity: int, year_month_created_at: str = None):
        self.item_id = item_id
        self.item_name = item_name
        self.sellings_quantity = sellings_quantity
        self.year_month_created_at = year_month_created_at

    def serialize(self) -> bytes:
        return f"{str(self.year_month_created_at)},{self.item_id},{self.item_name},{self.sellings_quantity}\n".encode("utf-8")

    def to_csv(self) -> str:
        return f"{str(self.year_month_created_at)},{self.item_name},{self.sellings_quantity}\n"

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        year_month_created_at = MonthYear.from_str(parts[0]) if len(parts) > 0 and parts[0] else None
        item_id = int(parts[1]) if len(parts) > 1 and parts[1] else None
        item_name = parts[2] if len(parts) > 2 and parts[2] else None
        sellings_quantity = int(parts[3]) if len(parts) > 3 and parts[3] else None

        row = Query2_1ResultRow(item_id, item_name, sellings_quantity, year_month_created_at)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 2.2
# Max profit por producto
# =========================================
class Query2_2ResultRow(TableResultRow):
    def __init__(self, item_id: int, item_name: str, profit_sum: float, year_month_created_at: str = None):
        self.item_id = item_id
        self.item_name = item_name
        self.profit_sum = profit_sum
        self.year_month_created_at = year_month_created_at

    def serialize(self) -> bytes:
        return f"{str(self.year_month_created_at)},{self.item_id},{self.item_name},{self.profit_sum}\n".encode("utf-8")

    def to_csv(self) -> str:
        return f"{str(self.year_month_created_at)},{self.item_name},{self.profit_sum}\n"

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        year_month_created_at_string = parts[0] if len(parts) > 0 and parts[0] else None
        item_id = int(parts[1]) if len(parts) > 1 and parts[1] else None
        item_name = parts[2] if len(parts) > 2 and parts[2] else None
        profit_sum = float(parts[3]) if len(parts) > 3 and parts[3] else None

        year_month_created_at = MonthYear.from_str(year_month_created_at_string) if year_month_created_at_string else None
        row = Query2_2ResultRow(item_id, item_name, profit_sum, year_month_created_at)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed

def custom_round(x: float) -> float:
    s = f"{x:.4f}"  # keep 4 decimals safely
    third_decimal = int(s.split(".")[1][2])  # get the 3rd decimal digit

    if third_decimal == 5:
        # Keep 3 decimals, but lower the 3rd by 1 to simulate "1.445"
        # instead of rounding to 1.45
        return float(f"{x:.4f}") - 0.001
    else:
        # Normal rounding to 2 decimals
        return round(x, 2)

# =========================================
# QUERY 3
# =========================================
class Query3ResultRow(TableResultRow):
    def __init__(self, store_id: int, store_name: str, tpv: float, year_half: YearHalf):
        self.store_id = store_id
        self.store_name = store_name
        self.tpv = tpv
        self.year_half = year_half

    def serialize(self) -> bytes:
        return f"{self.year_half},{self.store_id},{self.store_name},{self.tpv}\n".encode("utf-8")

    def to_csv(self) -> str:
        tpv_rounded = custom_round(self.tpv)
        return f"{self.year_half},{self.store_name},{tpv_rounded}\n"

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        year_half_string = parts[0] if len(parts) > 0 and parts[0] else None
        store_id = int(parts[1]) if len(parts) > 1 and parts[1] else None
        store_name = parts[2] if len(parts) > 2 and parts[2] else None
        tpv = float(parts[3]) if len(parts) > 3 and parts[3] else None

        year_half = YearHalf.from_str(year_half_string) if year_half_string else None
        row = Query3ResultRow(store_id, store_name, tpv, year_half)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed


# =========================================
# QUERY 4
# =========================================
class Query4ResultRow(TableResultRow):
    def __init__(self, store_id: int, store_name: str, user_id: int, birthdate: date, purchase_quantity: int):
        self.store_id = store_id
        self.store_name = store_name
        self.user_id = user_id
        self.birthdate = birthdate
        self.purchase_quantity = purchase_quantity

    def serialize(self) -> bytes:
        return f"{self.store_id},{self.store_name},{self.user_id},{self.birthdate.isoformat()},{self.purchase_quantity}\n".encode("utf-8")

    def to_csv(self) -> str:
        return f"{self.store_name},{self.birthdate.isoformat()}\n"

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(CSV_DELIMITER)

        store_id = int(parts[0]) if len(parts) > 0 and parts[0] else None
        store_name = parts[1] if len(parts) > 1 and parts[1] else None
        user_id = int(parts[2]) if len(parts) > 2 and parts[2] else None
        birthdate = date.fromisoformat(parts[3]) if len(parts) > 3 and parts[3] else None
        purchase_quantity = int(parts[4]) if len(parts) > 4 and parts[4] else None

        row = Query4ResultRow(store_id, store_name, user_id, birthdate, purchase_quantity)
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed