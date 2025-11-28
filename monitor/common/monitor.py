import threading
import time
import logging
import os
import json
import subprocess
import pika
from utils.protocol import (
    MSG_HEARTBEAT, MSG_ELECTION, MSG_COORDINATOR, MSG_DEATH,
    MSG_FORCE_END, MSG_FORCE_END_CLIENT,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, ELECTION_TIMEOUT,
    CONTROL_EXCHANGE, HEARTBEAT_EXCHANGE
)

class MonitorNode:
    def __init__(self):
        self.container_name = os.environ.get('CONTAINER_NAME', 'unknown')
        self.node_id = self.container_name
        self.is_leader = False
        self.leader_id = None
        self.last_leader_heartbeat = time.time()
        self.running = True
        
        # Tracking
        self.nodes_last_seen = {} # Map: node_id -> timestamp (for ALL nodes: workers, server, monitors)
        
        # Election State
        self.election_in_progress = False
        self.election_start_time = 0
        
        # Threads
        self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
        self.listener_thread = threading.Thread(target=self._listener_loop, daemon=True)
        self.checker_thread = threading.Thread(target=self._checker_loop, daemon=True)

    def start(self):
        logging.info(f"Starting MonitorNode {self.node_id}")
        self.sender_thread.start()
        self.listener_thread.start()
        self.checker_thread.start()

    def _get_connection(self):
        return pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=0))

    def _sender_loop(self):
        """Sends heartbeats periodically"""
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
                        'component': 'monitor',
                        'is_leader': self.is_leader
                    }
                    # Routing key: heartbeat.monitor.<node_id>
                    routing_key = f"heartbeat.monitor.{self.node_id}"
                    
                    channel.basic_publish(
                        exchange=HEARTBEAT_EXCHANGE,
                        routing_key=routing_key,
                        body=json.dumps(msg)
                    )
                    time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logging.error(f"Sender loop error: {e}")
                time.sleep(2)

    def _listener_loop(self):
        """Listens for heartbeats, elections, and control messages"""
        while self.running:
            connection = None
            try:
                connection = self._get_connection()
                channel = connection.channel()
                channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
                channel.exchange_declare(exchange=HEARTBEAT_EXCHANGE, exchange_type='topic', durable=True)
                
                queue_name = f"monitor_listener_{self.node_id}"
                channel.queue_declare(queue=queue_name, exclusive=True)
                
                # Bindings
                # 1. Heartbeats from everyone (Monitors, Workers, Server)
                channel.queue_bind(exchange=HEARTBEAT_EXCHANGE, queue=queue_name, routing_key='heartbeat.#')
                # 2. Elections
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="election.#")
                # 3. Control (Coordinator, Death)
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="control.#")

                def callback(ch, method, properties, body):
                    self._handle_message(body)

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

    def _handle_message(self, body):
        try:
            data = json.loads(body)
            msg_type = data.get('type')
            sender_id = data.get('id')
            
            if msg_type == MSG_HEARTBEAT:
                component = data.get('component')
                
                # Update last seen for everyone
                self.nodes_last_seen[sender_id] = time.time()
                
                if component == 'monitor':
                    is_sender_leader = data.get('is_leader')
                    if is_sender_leader:
                        self.leader_id = sender_id
                        self.last_leader_heartbeat = time.time()
                        
                        # Bully: If I am higher ID, challenge
                        if sender_id < self.node_id:
                            logging.warning(f"Detected Leader {sender_id} with lower ID. Challenging...")
                            self._start_election()
                        
                        if self.election_in_progress and sender_id > self.node_id:
                            self.election_in_progress = False

            elif msg_type == MSG_ELECTION:
                # Only Monitors participate
                if sender_id < self.node_id:
                    logging.info(f"Received ELECTION from {sender_id}. Sending my own election message.")
                    self._start_election()
                else:
                    logging.info(f"Received ELECTION from {sender_id}. Yielding (higher ID).")

            elif msg_type == MSG_COORDINATOR:
                self.leader_id = sender_id
                self.election_in_progress = False
                if sender_id < self.node_id:
                    self._start_election()
                else:
                    self.is_leader = (self.node_id == sender_id)
                    if self.is_leader:
                        logging.info("I am the new LEADER!")
                    else:
                        logging.info(f"New Leader elected: {sender_id}")
                        logging.info(f"Accepting {sender_id} as Coordinator.")

        except Exception as e:
            logging.error(f"Error handling message: {e}")

    def _checker_loop(self):
        """Checks for timeouts"""
        while self.running:
            time.sleep(1)
            
            # Election Timeout
            if self.election_in_progress:
                if time.time() - self.election_start_time > ELECTION_TIMEOUT:
                    # Declare Victory
                    logging.info("Election timeout reached. No higher ID detected. Declaring victory.")
                    self.is_leader = True
                    self.leader_id = self.node_id
                    self._broadcast(MSG_COORDINATOR)
                    self.election_in_progress = False
                    logging.info("I am the new LEADER!")
                continue

            if self.is_leader:
                # Debug: Print nodes seen
                if int(time.time()) % 5 == 0:
                    logging.info(f"Leader {self.node_id} tracking nodes: {list(self.nodes_last_seen.keys())}")

                for node_id, last_seen in list(self.nodes_last_seen.items()):
                    if node_id == self.node_id: continue # Don't check self
                    
                    if time.time() - last_seen > HEARTBEAT_TIMEOUT:
                        logging.info(f"Node {node_id} died. Reviving...")
                        self._revive_node(node_id)
                        del self.nodes_last_seen[node_id]
            
            # If I am NOT Leader: Check Leader
            else:
                if time.time() - self.last_leader_heartbeat > HEARTBEAT_TIMEOUT:
                    logging.warning("Leader died! Starting election...")
                    self._start_election()

    def _start_election(self):
        if self.election_in_progress: return
        logging.info("Starting election process...")
        self.election_in_progress = True
        self.election_start_time = time.time()
        self._broadcast(MSG_ELECTION)

    def _broadcast(self, msg_type, payload=None):
        try:
            connection = self._get_connection()
            channel = connection.channel()
            channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
            msg = {
                'type': msg_type,
                'id': self.node_id,
                'timestamp': time.time()
            }
            if payload:
                msg.update(payload)
                
            routing_key = "control.coordinator" if msg_type == MSG_COORDINATOR else "election.broadcast"
            if msg_type == MSG_FORCE_END:
                routing_key = "control.force_end"
            elif msg_type == MSG_FORCE_END_CLIENT:
                routing_key = "control.force_end_client"
                
            channel.basic_publish(
                exchange=CONTROL_EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(msg)
            )
            connection.close()
        except Exception as e:
            logging.error(f"Broadcast error: {e}")

    def _revive_node(self, node_id):
        # 0. Check for critical failures (Server or Client)
        if node_id == 'server':
            logging.critical("Server died! Broadcasting FORCE_END...")
            self._broadcast(MSG_FORCE_END)
        elif node_id.startswith('client-'):
            try:
                # Extract client ID (e.g., client-1 -> 1)
                client_id = node_id.split('-')[1]
                logging.critical(f"Client {client_id} died! Broadcasting FORCE_END_CLIENT...")
                self._broadcast(MSG_FORCE_END_CLIENT, {'client_id': client_id})
            except IndexError:
                logging.error(f"Failed to extract client ID from {node_id}")

        # 1. Publish Death Certificate
        self._publish_death(node_id)
        # 2. Docker Restart
        try:
            logging.info(f"Restarting container {node_id}...")
            subprocess.run(['docker', 'restart', node_id], check=False)
        except Exception as e:
            logging.error(f"Failed to restart {node_id}: {e}")

    def _publish_death(self, target_id):
        try:
            connection = self._get_connection()
            channel = connection.channel()
            msg = {'type': MSG_DEATH, 'target': target_id}
            channel.basic_publish(
                exchange=CONTROL_EXCHANGE,
                routing_key="control.death",
                body=json.dumps(msg)
            )
            connection.close()
        except Exception as e:
            logging.error(f"Death cert error: {e}")
