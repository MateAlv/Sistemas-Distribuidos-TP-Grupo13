# =========================================
# ENUM DE TABLAS
# =========================================
class TableType(Enum):
    TRANSACTIONS = 1
    TRANSACTIONS_ITEMS = 2
    MENU_ITEMS = 3
    STORES = 4
    USERS = 5
    
    def from_path(path: str):
    """
    Clase para determinar el tipo de tabla basado en el nombre del archivo.
    Se considera que los archivos de un tipo de tabla se encuentran dentro de una carpeta
    que contiene el nombre de la tabla (case insensitive).
    Ejemplo: /data/transactions/transactions_2023.csv → TableType.TRANSACTIONS
    """
    path = path.lower()
    main_folder = path.split("/")[2]
    for table_type in TableType:
        if main_folder == table_type.name.lower():
            return table_type
    raise ValueError(f"No se pudo determinar el tipo de tabla para el path: {path}")


# =========================================