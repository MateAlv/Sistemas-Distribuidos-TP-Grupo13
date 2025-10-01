import datetime

# =========================================
# BASE FILE ROW
# =========================================
class TableFileRow: 
    
    def serialize_payload(self) -> bytes:
        raise NotImplementedError
    
    def serialize(self) -> bytes:
        raise NotImplementedError

    @staticmethod
    def deserialize(data: bytes):
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
                 transaction_id: str, 
                 store_id: int, 
                 payment_method_id: int, 
                 voucher_id: int, 
                 user_id: int, 
                 original_amount: float, 
                 discount_applied: float, 
                 final_amount: float, 
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
        transaction_id_str = self.transaction_id if self.transaction_id is not None else ""
        store_id_str = str(self.store_id) if self.store_id is not None else ""
        payment_method_id_str = str(self.payment_method_id) if self.payment_method_id is not None else ""
        voucher_id_str = str(self.voucher_id) if self.voucher_id is not None else ""
        user_id_str = str(self.user_id) if self.user_id is not None else ""
        original_amount_str = str(self.original_amount) if self.original_amount is not None else ""
        discount_applied_str = str(self.discount_applied) if self.discount_applied is not None else ""
        final_amount_str = str(self.final_amount) if self.final_amount is not None else ""
        created_at_str = self.created_at.isoformat() if self.created_at is not None else ""

        return f"{transaction_id_str};{store_id_str};{payment_method_id_str};{voucher_id_str};{user_id_str};{original_amount_str};{discount_applied_str};{final_amount_str};{created_at_str}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")

        transaction_id = parts[0] if len(parts[0]) > 0 else None
        store_id = int(parts[1]) if len(parts[1]) > 0 else None
        payment_method_id = int(parts[2]) if len(parts[2]) > 0 else None
        voucher_id = int(parts[3]) if len(parts[3]) > 0 else None
        user_id = int(parts[4]) if len(parts[4]) > 0 else None
        original_amount = float(parts[5]) if len(parts[5]) > 0 else None
        discount_applied = float(parts[6]) if len(parts[6]) > 0 else None
        final_amount = float(parts[7]) if len(parts[7]) > 0 else None
        created_at = datetime.date.fromisoformat(parts[8]) if len(parts[8]) > 0 else None

        row = TransactionsFileRow(
            transaction_id, 
            store_id, 
            payment_method_id, 
            voucher_id,
            user_id,
            original_amount,
            discount_applied,
            final_amount,
            created_at
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
                 transaction_id: str, 
                 item_id: int, 
                 quantity: int, 
                 unit_price: float, 
                 subtotal: float, 
                 created_at: datetime.date):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at
    
    def serialize(self) -> bytes:
        transaction_id_str = self.transaction_id if self.transaction_id is not None else ""
        item_id_str = str(self.item_id) if self.item_id is not None else ""
        quantity_str = str(self.quantity) if self.quantity is not None else ""
        unit_price_str = str(self.unit_price) if self.unit_price is not None else ""
        subtotal_str = str(self.subtotal) if self.subtotal is not None else ""
        created_at_str = self.created_at.isoformat() if self.created_at is not None else ""

        return f"{transaction_id_str};{item_id_str};{quantity_str};{unit_price_str};{subtotal_str};{created_at_str}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        
        trans_id = parts[0] if len(parts[0]) > 0 else None
        item_id = int(parts[1]) if len(parts[1]) > 0 else None
        quantity = int(parts[2]) if len(parts[2]) > 0 else None
        unit_price = float(parts[3]) if len(parts[3]) > 0 else None
        subtotal = float(parts[4]) if len(parts[4]) > 0 else None
        created_at = datetime.date.fromisoformat(parts[5]) if len(parts[5]) > 0 else None
        
        row = TransactionsItemsFileRow(
            trans_id,
            item_id,
            quantity,
            unit_price,
            subtotal,
            created_at
        )
        consumed = len(line.encode("utf-8")) + 1
        return row, consumed
    
# =========================================
# Menu Items File Row
# - Representa una fila del archivo de items del menu.
# - Campos:
#       - item_id: int
#       - item_name: String
#       - category: String
#       - price: float
#       - is_seasonal: bool
#       - available_from: Date
#       - available_to: Date
# =========================================
class MenuItemsFileRow(TableFileRow):
    def __init__(self, 
                 item_id: int, 
                 name: str, 
                 category: str, 
                 price: float, 
                 is_seasonal: bool, 
                 available_from: datetime.date, 
                 available_to: datetime.date):
        self.item_id = item_id
        self.item_name = name
        self.category = category
        self.price = price
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to

    def serialize(self) -> bytes:
        item_id_str = str(self.item_id) if self.item_id is not None else ""
        item_name_str = self.item_name if self.item_name is not None else ""
        category_str = self.category if self.category is not None else ""
        price_str = str(self.price) if self.price is not None else ""
        is_seasonal_str = str(self.is_seasonal) if self.is_seasonal is not None else ""
        available_from_str = self.available_from.isoformat() if self.available_from is not None else ""
        available_to_str = self.available_to.isoformat() if self.available_to is not None else ""

        return f"{item_id_str};{item_name_str};{category_str};{price_str};{is_seasonal_str};{available_from_str};{available_to_str}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")
        
        item_id = int(parts[0]) if len(parts[0]) > 0 else None
        item_name = parts[1] if len(parts[1]) > 0 else None
        category = parts[2] if len(parts[2]) > 0 else None
        price = float(parts[3]) if len(parts[3]) > 0 else None
        is_seasonal = parts[4].lower() == 'true' if len(parts[4]) > 0 else None
        available_from = datetime.date.fromisoformat(parts[5]) if len(parts[5]) > 0 else None
        available_to = datetime.date.fromisoformat(parts[6]) if len(parts[6]) > 0 else None
        
        row = MenuItemsFileRow(
            item_id,
            item_name,
            category,
            price,
            is_seasonal,
            available_from,
            available_to
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
    def __init__(self, 
                 store_id: int, 
                 store_name: str, 
                 street: str, 
                 city: str, 
                 state: str, 
                 latitude: float, 
                 longitude: float):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

    def serialize(self) -> bytes:
        store_id_str = str(self.store_id) if self.store_id is not None else ""
        store_name_str = self.store_name if self.store_name is not None else ""
        street_str = self.street if self.street is not None else ""
        city_str = self.city if self.city is not None else ""
        state_str = self.state if self.state is not None else ""
        latitude_str = str(self.latitude) if self.latitude is not None else ""
        longitude_str = str(self.longitude) if self.longitude is not None else ""

        return f"{store_id_str};{store_name_str};{street_str};{city_str};{state_str};{latitude_str};{longitude_str}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")

        store_id = int(parts[0]) if len(parts[0]) > 0 else None
        store_name = parts[1] if len(parts[1]) > 0 else None
        street = parts[2] if len(parts[2]) > 0 else None
        city = parts[3] if len(parts[3]) > 0 else None
        state = parts[4] if len(parts[4]) > 0 else None
        latitude = float(parts[5]) if len(parts[5]) > 0 else None
        longitude = float(parts[6]) if len(parts[6]) > 0 else None

        row = StoresFileRow(
            store_id,
            store_name,
            street,
            city,
            state,
            latitude,
            longitude
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

    def __init__(self, 
                 user_id: int,
                 gender: str,
                 birthdate: datetime.date,
                 registration_at: datetime.date):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registration_at = registration_at

    def serialize(self) -> bytes:
        
        user_id_str = str(self.user_id) if self.user_id is not None else ""
        gender_str = self.gender if self.gender is not None else ""
        birthdate_str = self.birthdate.isoformat() if self.birthdate is not None else ""
        registration_at_str = self.registration_at.isoformat() if self.registration_at is not None else ""

        return f"{user_id_str};{gender_str};{birthdate_str};{registration_at_str}\n".encode("utf-8")

    @staticmethod
    def deserialize(data: bytes):
        line = data.split(b"\n", 1)[0].decode("utf-8")
        parts = line.split(";")

        user_id = int(parts[0]) if len(parts[0]) > 0 else None
        gender = parts[1] if len(parts[1]) > 0 else None
        birthdate = datetime.date.fromisoformat(parts[2]) if len(parts[2]) > 0 else None
        registration_at = datetime.date.fromisoformat(parts[3]) if len(parts[3]) > 0 else None
        
        row = UsersFileRow(
            user_id,
            gender,
            birthdate,
            registration_at
        )
        consumed = len(line.encode("utf-8")) + 1
        
        return row, consumed