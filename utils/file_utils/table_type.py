from enum import Enum
# =========================================
# ENUM DE TABLAS
# =========================================
class TableType(Enum):
    TRANSACTIONS = 1
    TRANSACTION_ITEMS = 2
    MENU_ITEMS = 3
    STORES = 4
    USERS = 5
    PURCHASES_PER_USER_STORE = 6
    
    def from_path(path: str):
        """
        Clase para determinar el tipo de tabla basado en el nombre del último archivo en el path.
        Ejemplo: transactions/transactions_2023.csv → TableType.TRANSACTIONS
        Ejemplo: client-1/menu_items/menu_items.csv → TableType.MENU_ITEMS
        """
        path = path.lower()
        filename = path.split("/")[-1]  # tomar solo el último segmento (archivo)
        
        if "transaction_items" in filename:
            return TableType.TRANSACTION_ITEMS
        elif "transactions" in filename:
            return TableType.TRANSACTIONS
        elif "menu_items" in filename:
            return TableType.MENU_ITEMS
        elif "stores" in filename:
            return TableType.STORES
        elif "users" in filename:
            return TableType.USERS

        raise ValueError(f"No se pudo determinar el tipo de tabla para el path: {path}")
# =========================================

class ResultTableType(Enum):
    QUERY_1 = 1
    QUERY_2_1 = 2
    QUERY_2_2 = 3
    QUERY_3 = 4
    QUERY_4 = 5