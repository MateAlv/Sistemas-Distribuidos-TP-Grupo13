
# =========================================
# BASE FILE ROW
# =========================================
class TableFileRow: 
    
    def serialize_payload(self) -> bytes:
        raise NotImplementedError
    
    def serialize(self) -> bytes:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes) -> List[TableFileRow]:
        raise NotImplementedError


# =========================================
# Transactions File Row
# - Representa una fila del archivo de transacciones.
# - Campos: 
#       - transaction_id: String
#       - store_id: int
#       - payment_method_id: int
#       - voucher_id: int
#       - user_id: int
#       - original_amount: float
#       - discount_applied: float
#       - final_amount: float
#       - created_at: Date
# =========================================
class TransactionsFileRow(TableFileRow):
    def __init__(self, 
                 transaction_id, 
                 store_id, 
                 payment_method_id, 
                 voucher_id, 
                 user_id, 
                 original_amount, 
                 discount_applied, 
                 final_amount, 
                 created_at: datetime.date):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.voucher_id = voucher_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at
    
    def serialize(self) -> bytes:
        return f"{self.transaction_id};{self.store_id};{self.payment_method_id};{self.voucher_id};{self.user_id};{self.original_amount};{self.discount_applied};{self.final_amount};{self.created_at.isoformat()}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = TransactionsFileRow(
            parts[0],
            int(parts[1]),
            int(parts[2]),
            int(parts[3]),
            int(parts[4]),
            float(parts[5]),
            float(parts[6]),
            float(parts[7]),
            datetime.date.fromisoformat(parts[8])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed

# =========================================
# TransactionsItems File Row
# - Representa una fila del archivo de items de transacciones.
# - Campos:
#       - transaction_id: String
#       - item_id: int
#       - quantity: int
#       - unit_price: float
#       - subtotal: float
#       - created_at: Date
# =========================================
class TransactionsItemsFileRow(TableFileRow):
    def __init__(self, 
                 transaction_id, 
                 item_id, 
                 quantity, 
                 unit_price, 
                 subtotal, 
                 created_at: datetime.date):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at
    
    def serialize(self) -> bytes:
        return f"{self.transaction_id};{self.item_id};{self.quantity};{self.unit_price};{self.subtotal};{self.created_at.isoformat()}\n".encode("utf-8")
    
    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = TransactionsItemsFileRow(
            parts[0],
            int(parts[1]),
            int(parts[2]),
            float(parts[3]),
            float(parts[4]),
            datetime.date.fromisoformat(parts[5])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
    
# =========================================
# Menu Items File Row
# - Representa una fila del archivo de items del menu.
# - Campos:
#       - item_id: int
#       - item_name: String
#       - quantity: int
#       - unit_price: float
#       - subtotal: float 
#       - created_at: Date
# =========================================
class MenuItemsFileRow(TableFileRow):
    def __init__(self, item_id, name, quantity, unit_price, subtotal, created_at):
        self.item_id = item_id        
        self.name = name
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at

    def serialize(self) -> bytes:
        return f"{self.item_id};{self.name};{self.quantity};{self.unit_price};{self.subtotal};{self.created_at.isoformat()}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = MenuItemsFileRow(
            int(parts[0]),
            parts[1],
            int(parts[2]),
            float(parts[3]),
            float(parts[4]),
            datetime.date.fromisoformat(parts[5])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
    
# =========================================
# Stores File Row   
# - Representa una fila del archivo de stores.
# - Campos:
#       - store_id: int
#       - store_name: String
#       - street: String
#       - city: String
#       - state: String
#       - latitude: float
#       - longitude: float
# =========================================
class StoresFileRow(TableFileRow):
    def __init__(self, store_id, store_name, street, city, state, latitude, longitude):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

    def serialize(self) -> bytes:
        return f"{self.store_id};{self.store_name};{self.street};{self.city};{self.state};{self.latitude};{self.longitude}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = StoresFileRow(
            int(parts[0]),
            parts[1],
            parts[2],
            parts[3],
            parts[4],
            float(parts[5]),
            float(parts[6])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed

# =========================================
# Users File Row
# - Representa una fila del archivo de users.
# - Campos:
#       - user_id: int
#       - gender: String
#       - birthdate: Date
#       - registration_at: Date
# =========================================

class UsersFileRow(TableFileRow):

    def __init__(self, user_id, gender, birthdate, registration_at):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registration_at = registration_at

    def serialize(self) -> bytes:
        return f"{self.user_id};{self.gender};{self.birthdate.isoformat()};{self.registration_at.isoformat()}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        row = UsersFileRow(
            int(parts[0]),
            parts[1],
            datetime.date.fromisoformat(parts[2]),
            datetime.date.fromisoformat(parts[3])
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed