from .table_type import TableType

class MessageEnd:
    def __init__(self, client_id: int, table_type: TableType, count: int):
        self.client_id = client_id
        self.table_type = table_type
        self.count = count

    def encode(self) -> bytes:
        """
        Serializa el objeto a bytes con el formato:
        b"END;{client_id};{table_type.name};{count}"
        """
        return f"END;{self.client_id};{self.table_type.value};{self.count}".encode("utf-8")

    @classmethod
    def decode(cls, message: bytes) -> "MessageEnd":
        """
        Crea un objeto MessageEnd a partir de bytes.
        """
        decoded = message.decode("utf-8")
        parts = decoded.split(";")
        if len(parts) != 4 or parts[0] != "END":
            raise ValueError(f"Formato inválido de mensaje END: {decoded}")
        
        _, client_id, table_type_value, count = parts

        try:
            table_type = TableType(int(table_type_value))
        except KeyError:
            raise ValueError(f"TableType inválido: {table_type_value}")

        return cls(client_id, table_type, int(count))