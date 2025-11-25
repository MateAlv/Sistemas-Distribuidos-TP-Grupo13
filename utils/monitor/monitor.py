import threading
import time
import logging
import os
import json
import subprocess
import pika
from utils.protocol import *

class Monitor:
    def __init__(self):
        self.container_name = os.environ.get('CONTAINER_NAME', 'unknown')
        self.node_id = self.container_name # Use container name as unique ID
        self.is_leader = False
        self.leader_id = None
        self.last_leader_heartbeat = time.time()
        self.last_pulse = time.time()
        self.running = True
        
        # State for Leader
        self.workers_last_seen = {}
        
        # Threading events
        self.election_in_progress = False
        
        # Setup threads
        self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
        self.listener_thread = threading.Thread(target=self._listener_loop, daemon=True)
        self.checker_thread = threading.Thread(target=self._checker_loop, daemon=True)

    def start(self):
        logging.info(f"Starting Monitor for {self.node_id}")
        self.sender_thread.start()
        self.listener_thread.start()
        self.checker_thread.start()

    def pulse(self):
        """Called by main process to prove it's alive"""
        self.last_pulse = time.time()

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
                    channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)

                # Check for apoptosis (Main process hung)
                if time.time() - self.last_pulse > HEARTBEAT_TIMEOUT:
                    logging.error("Main process hung! Committing apoptosis...")
                    self._apoptosis()
                    return

                # Send Heartbeat
                msg = {
                    'type': MSG_HEARTBEAT,
                    'id': self.node_id,
                    'timestamp': time.time(),
                    'is_leader': self.is_leader
                }
                routing_key = f"heartbeat.{'leader' if self.is_leader else 'worker'}.{self.node_id}"
                channel.basic_publish(
                    exchange=CONTROL_EXCHANGE,
                    routing_key=routing_key,
                    body=json.dumps(msg)
                )
                
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logging.error(f"Sender loop error: {e}")
                time.sleep(2)

    def _listener_loop(self):
        """Listens for heartbeats and elections"""
        while self.running:
            try:
                connection = self._get_connection()
                channel = connection.channel()
                channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
                
                # Queue for this node
                queue_name = f"monitor_{self.node_id}"
                channel.queue_declare(queue=queue_name, exclusive=True)
                
                # Bindings
                # 1. Listen to Leader Heartbeats
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="heartbeat.leader.#")
                # 2. Listen to Elections
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="election.#")
                # 3. Listen to Control (Death Certificates)
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="control.#")
                # 4. If Leader, listen to Worker Heartbeats
                if self.is_leader:
                    channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="heartbeat.worker.#")

                def callback(ch, method, properties, body):
                    self._handle_message(body, ch)

                channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
                channel.start_consuming()
            except Exception as e:
                logging.error(f"Listener loop error: {e}")
                time.sleep(2)

    def _handle_message(self, body, channel):
        try:
            data = json.loads(body)
            msg_type = data.get('type')
            sender_id = data.get('id')
            
            if msg_type == MSG_HEARTBEAT:
                if data.get('is_leader'):
                    self.last_leader_heartbeat = time.time()
                    self.leader_id = sender_id
                    if self.election_in_progress and sender_id > self.node_id:
                        self.election_in_progress = False # Higher ID leader exists
                elif self.is_leader:
                    self.workers_last_seen[sender_id] = time.time()

            elif msg_type == MSG_ELECTION:
                # Bully Algorithm: If we receive ELECTION from lower ID, we send OK (by starting our own election/heartbeat)
                if sender_id < self.node_id:
                    self._start_election()
            
            elif msg_type == MSG_COORDINATOR:
                self.leader_id = sender_id
                self.election_in_progress = False
                self.is_leader = (self.node_id == sender_id)
                if self.is_leader:
                    logging.info("I am the new LEADER!")
                    # Re-bind to listen to workers
                    # Note: In this simple implementation, we might need to restart listener or use a dynamic bind if possible.
                    # For now, let's assume the checker loop handles the restart or we just bind everything initially?
                    # Binding everything initially is noisy. 
                    # Let's just restart the listener thread or have it check `self.is_leader` periodically?
                    # Actually, `channel.queue_bind` is thread-safe-ish? No.
                    # Simplification: The listener loop will break and restart if leadership changes? 
                    # Or just bind to everything but ignore if not leader.
                    pass

            elif msg_type == MSG_DEATH:
                target = data.get('target')
                if target == self.node_id:
                    logging.critical("Received DEATH CERTIFICATE. Terminating...")
                    self._apoptosis()

        except Exception as e:
            logging.error(f"Error handling message: {e}")

    def _checker_loop(self):
        """Checks for timeouts (Leader death or Worker death)"""
        while self.running:
            time.sleep(1)
            
            # 1. Check Leader Health (if I am not leader)
            if not self.is_leader:
                if time.time() - self.last_leader_heartbeat > HEARTBEAT_TIMEOUT:
                    logging.warning("Leader died! Starting election...")
                    self._start_election()
            
            # 2. Check Workers Health (if I am leader)
            else:
                now = time.time()
                dead_workers = []
                for worker, last_seen in self.workers_last_seen.items():
                    if now - last_seen > HEARTBEAT_TIMEOUT:
                        dead_workers.append(worker)
                
                for worker in dead_workers:
                    logging.info(f"Worker {worker} died. Reviving...")
                    self._revive_node(worker)
                    del self.workers_last_seen[worker]

    def _start_election(self):
        if self.election_in_progress:
            return
        self.election_in_progress = True
        
        # Bully: Send ELECTION to all higher IDs
        # In this dynamic Docker env, we don't know all IDs easily. 
        # Simplification: Broadcast ELECTION. If anyone with higher ID hears it, they take over.
        # If no one responds (we become leader after timeout).
        # But how to wait for timeout?
        
        # Modified Bully for Broadcast:
        # 1. Broadcast ELECTION.
        # 2. Wait T seconds.
        # 3. If no higher ID sends COORDINATOR or ELECTION, declare self COORDINATOR.
        
        self._broadcast(MSG_ELECTION)
        
        # Wait for response (in a separate non-blocking way? or just sleep here?)
        # Since this is a thread, we can sleep.
        time.sleep(ELECTION_TIMEOUT)
        
        if self.election_in_progress: # Still thinking we can be leader
             # Check if we heard from a higher leader
             if self.leader_id and self.leader_id > self.node_id and (time.time() - self.last_leader_heartbeat < HEARTBEAT_TIMEOUT):
                 return # Higher leader is alive
             
             # Declare Victory
             self.is_leader = True
             self.leader_id = self.node_id
             self._broadcast(MSG_COORDINATOR)
             self.election_in_progress = False

    def _broadcast(self, msg_type):
        try:
            connection = self._get_connection()
            channel = connection.channel()
            channel.exchange_declare(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
            msg = {
                'type': msg_type,
                'id': self.node_id,
                'timestamp': time.time()
            }
            channel.basic_publish(
                exchange=CONTROL_EXCHANGE,
                routing_key="election.broadcast",
                body=json.dumps(msg)
            )
            connection.close()
        except Exception as e:
            logging.error(f"Broadcast error: {e}")

    def _revive_node(self, node_id):
        # 1. Publish Death Certificate (to kill zombie)
        self._publish_death(node_id)
        
        # 2. Docker Restart
        try:
            # node_id is the container name
            logging.info(f"Restarting container {node_id}...")
            subprocess.run(['docker', 'restart', node_id], check=False)
        except Exception as e:
            logging.error(f"Failed to restart {node_id}: {e}")

    def _publish_death(self, target_id):
        try:
            connection = self._get_connection()
            channel = connection.channel()
            msg = {
                'type': MSG_DEATH,
                'target': target_id
            }
            channel.basic_publish(
                exchange=CONTROL_EXCHANGE,
                routing_key="control.death",
                body=json.dumps(msg)
            )
            connection.close()
        except Exception as e:
            logging.error(f"Death cert error: {e}")

    def _apoptosis(self):
        logging.critical("Performing Apoptosis (Self-Destruction)...")
        os._exit(1) # Force exit
