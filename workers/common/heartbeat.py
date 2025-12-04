import threading
import time
import logging
import json
import pika
import os

from utils.protocol import MSG_HEARTBEAT, HEARTBEAT_EXCHANGE, HEARTBEAT_INTERVAL

class HeartbeatSender:
    def __init__(self, node_id, component_type="worker"):
        self.node_id = node_id
        self.component_type = component_type
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        
    def start(self):
        logging.info(f"Starting HeartbeatSender for {self.node_id}")
        self.thread.start()
        
    def stop(self):
        self.running = False
        
    def _get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))

    def _run(self):
        while self.running:
            try:
                connection = self._get_connection()
                channel = connection.channel()
                channel.exchange_declare(exchange=HEARTBEAT_EXCHANGE, exchange_type='topic', durable=True)
                
                while self.running:
                    msg = {
                        'type': MSG_HEARTBEAT,
                        'id': self.node_id,
                        'timestamp': time.time(),
                        'component': self.component_type
                    }
                    routing_key = f"heartbeat.{self.component_type}.{self.node_id}"
                    
                    channel.basic_publish(
                        exchange=HEARTBEAT_EXCHANGE,
                        routing_key=routing_key,
                        body=json.dumps(msg)
                    )
                    time.sleep(HEARTBEAT_INTERVAL)
                    
            except Exception as e:
                logging.error(f"Heartbeat sender error: {e}")
                time.sleep(5)
            finally:
                try:
                    if 'connection' in locals() and connection.is_open:
                        connection.close()
                except:
                    pass
