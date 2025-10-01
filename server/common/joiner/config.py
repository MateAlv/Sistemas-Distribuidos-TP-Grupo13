import os
from configparser import ConfigParser

class AppConfig:
    def __init__(self, path="/joiner/config.ini"):
        cfg = ConfigParser(os.environ)
        read = cfg.read(path) or cfg.read(os.path.join(os.path.dirname(__file__), "config.ini"))
        self.log_level = os.getenv("LOG_LEVEL", cfg["DEFAULT"].get("LOG_LEVEL", "INFO"))

        r = cfg["RABBIT"]
        self.rabbit_host = os.getenv("RABBIT_HOST", r.get("HOST", "rabbitmq"))

        q = cfg["QUEUES"]
        self.q_clients = os.getenv("QUEUE_CLIENTS", q.get("CLIENTS", "batches.clients"))
        self.q_transactions = os.getenv("QUEUE_TRANSACTIONS", q.get("TRANSACTIONS", "batches.transactions"))
        self.q_products = os.getenv("QUEUE_PRODUCTS", q.get("PRODUCTS", "batches.products"))
