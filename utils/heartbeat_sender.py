import threading
import time
import logging
import os
import json
import signal
import pika
from utils.protocol import (
    MSG_HEARTBEAT, MSG_DEATH,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    CONTROL_EXCHANGE, HEARTBEAT_EXCHANGE
)

class HeartbeatSender:
    def __init__(self):
        self.container_name = os.environ.get('CONTAINER_NAME', 'unknown')
        self.node_id = self.container_name
        self.running = True
        
        self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
        self.listener_thread = threading.Thread(target=self._listener_loop, daemon=True)

    def start(self):
        logging.info(f"Starting HeartbeatSender for {self.node_id}")
        self.sender_thread.start()
        self.listener_thread.start()

    def _get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))

    def _sender_loop(self):
        """Sends heartbeats periodically"""
        connection = None
        channel = None
        while self.running:
            try:
                if not connection or connection.is_closed:
                    connection = self._get_connection()
                    channel = connection.channel()
                    channel.exchange_declare(exchange=HEARTBEAT_EXCHANGE, exchange_type='topic', durable=True)

                # Send Heartbeat
                msg = {
                    'type': MSG_HEARTBEAT,
                    'id': self.node_id,
                    'timestamp': time.time(),
                    'component': 'worker' # Generic tag, could be 'server' too
                }
                # Routing key: heartbeat.worker.<node_id>
                # We use 'worker' for everyone who is not a Monitor
                routing_key = f"heartbeat.worker.{self.node_id}"
                
                channel.basic_publish(
                    exchange=HEARTBEAT_EXCHANGE,
                    routing_key=routing_key,
                    body=json.dumps(msg)
                )
                
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logging.error(f"HeartbeatSender loop error: {e}")
                time.sleep(2)

    def _listener_loop(self):
        """Listens for Death Certificates"""
        while self.running:
            connection = None
            try:
                connection = self._get_connection()
                channel = connection.channel()
                channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
                
                # Queue for this node
                queue_name = f"heartbeat_listener_{self.node_id}"
                channel.queue_declare(queue=queue_name, exclusive=True)
                
                # Listen to Control (Death Certificates)
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="control.death")

                def callback(ch, method, properties, body):
                    try:
                        data = json.loads(body)
                        if data.get('type') == MSG_DEATH:
                            target = data.get('target')
                            if target == self.node_id:
                                logging.critical("Received DEATH CERTIFICATE. Terminating...")
                                self._apoptosis()
                    except Exception as e:
                        logging.error(f"Error handling message: {e}")

                channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
                channel.start_consuming()
            except Exception as e:
                logging.error(f"Listener loop error: {e}")
                time.sleep(2)
            finally:
                if connection and not connection.is_closed:
                    try:
                        connection.close()
                    except:
                        pass

    def _apoptosis(self):
        logging.critical("Performing Apoptosis (Self-Destruction)...")
        os.kill(os.getpid(), signal.SIGTERM)
