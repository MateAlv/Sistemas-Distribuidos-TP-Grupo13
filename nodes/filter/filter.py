import logging
from utils.file_utils.process_table import TransactionsProcessRow


class Filter:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.filter_type = cfg["filter_type"]

    def run(self):
        logging.info(f"Filtro iniciado. Tipo: {self.filter_type}")
        while True:
            tx = {}
            sending = []
            pass #Logica RabbitMQ y deserializacion de mensaje
            if self.apply(tx):
                sending.append(tx)
            pass #Logica RabbitMQ y serializacion de mensaje
            
    def apply(self, tx: TransactionsProcessRow) -> bool:
        """
        Aplica el filtro según el tipo configurado.
        """
        if self.filter_type == "year":
            return self.cfg["year_start"] <= tx.created_at.year <= self.cfg["year_end"]

        elif self.filter_type == "hour":
            return self.cfg["hour_start"] <= tx.created_at.timetuple().tm_hour <= self.cfg["hour_end"]

        elif self.filter_type == "amount":
            return tx.final_amount >= self.cfg["min_amount"]

        logging.error(f"Filtro desconocido: {self.filter_type}")
        return False
