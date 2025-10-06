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
    
    def from_path(path: str):
        """
        Clase para determinar el tipo de tabla basado en el nombre del archivo.
        Se considera que los archivos de un tipo de tabla se encuentran dentro de una carpeta
        que contiene el nombre de la tabla (case insensitive).
        Ejemplo: transactions/transactions_2023.csv → TableType.TRANSACTIONS
        Ejemplo: client-1/menu_items/menu_items.csv → TableType.MENU_ITEMS
        """
        path = path.lower()
        path_parts = path.split("/")
        
        # Buscar en todos los segmentos del path, no solo el primero
        for part in path_parts:
            for table_type in TableType:
                if part == table_type.name.lower():
                    return table_type
                    
        raise ValueError(f"No se pudo determinar el tipo de tabla para el path: {path}")
# =========================================

class ResultTableType(Enum):
    QUERY_1 = 1
    QUERY_2_1 = 2
    QUERY_2_2 = 3
    QUERY_3 = 4
    QUERY_4 = 5