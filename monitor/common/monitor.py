import threading
import time
import logging
import os
import json
import subprocess
import pika
from utils.protocol import (
    MSG_HEARTBEAT, MSG_ELECTION, MSG_COORDINATOR, MSG_DEATH,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, ELECTION_TIMEOUT,
    CONTROL_EXCHANGE, HEARTBEAT_EXCHANGE,
    COORDINATION_EXCHANGE, COORDINATION_ROUTING_KEY, DEFAULT_SHARD,
    MSG_WORKER_END, MSG_WORKER_STATS, MSG_BARRIER_FORWARD,
    STAGE_FILTER_YEAR, STAGE_FILTER_HOUR, STAGE_FILTER_AMOUNT,
    STAGE_AGG_PRODUCTS, STAGE_AGG_TPV, STAGE_AGG_PURCHASES,
    STAGE_MAX_PARTIALS, STAGE_MAX_ABSOLUTE,
    STAGE_TOP3_PARTIALS, STAGE_TOP3_ABSOLUTE,
    STAGE_JOIN_ITEMS, STAGE_JOIN_STORES_TPV, STAGE_JOIN_STORES_TOP3, STAGE_JOIN_USERS,
    STAGE_SERVER_RESULTS,
)
from collections import defaultdict
import uuid

class MonitorNode:
    def __init__(self):
        logging.getLogger('pika').setLevel(logging.CRITICAL)
        
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

        # Barrier State: client_id -> stage -> shard -> tracker
        # tracker holds counts, expected, last_forward_ts, senders seen, and latest stats.
        self.barrier_state = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
            'expected': None,
            'received_end': 0,
            'sender_ids': set(),
            'end_sender_ids': set(),
            'total_chunks': 0,
            'last_forward_ts': 0,
            'forwarded': False,
            'stats_expected_chunks': None,
            'stats_received': 0,
            'stats_processed': 0,
        })))

        # How often we forward (seconds). Default: 60s.
        self.forward_interval = int(os.environ.get('BARRIER_FORWARD_INTERVAL', '60'))
        
        # Threads
        self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
        self.listener_thread = threading.Thread(target=self._listener_loop, daemon=True)
        self.checker_thread = threading.Thread(target=self._checker_loop, daemon=True)
        self.forward_thread = threading.Thread(target=self._forward_loop, daemon=True)

        # Expected worker counts per stage (loaded from config)
        self.stage_expected = self._load_expected_from_config()

    def start(self):
        logging.info(f"Starting MonitorNode {self.node_id}")
        logging.info(f"Barrier expected counts: {self.stage_expected}")
        self.sender_thread.start()
        self.listener_thread.start()
        self.checker_thread.start()
        self.forward_thread.start()

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
                channel.exchange_declare(exchange=COORDINATION_EXCHANGE, exchange_type='topic', durable=True)
                channel.exchange_declare(exchange=COORDINATION_EXCHANGE, exchange_type='topic', durable=True)
                
                queue_name = f"monitor_listener_{self.node_id}"
                channel.queue_declare(queue=queue_name, exclusive=True)
                
                # Bindings
                # 1. Heartbeats from everyone (Monitors, Workers, Server)
                channel.queue_bind(exchange=HEARTBEAT_EXCHANGE, queue=queue_name, routing_key='heartbeat.#')
                # 2. Elections
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="election.#")
                # 3. Control (Coordinator, Death)
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="control.#")
                # 4. Coordination END/stats from workers/server
                channel.queue_bind(exchange=COORDINATION_EXCHANGE, queue=queue_name, routing_key="coordination.#")

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
            elif msg_type in (MSG_WORKER_END, MSG_WORKER_STATS):
                # Only leader tracks barrier data
                if not self.is_leader:
                    return
                self._handle_barrier_message(data)

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

    def _forward_loop(self):
        """Periodically forward barrier completion messages downstream"""
        while self.running:
            time.sleep(1)
            if not self.is_leader:
                continue
            now = time.time()
            for client_id, stages in list(self.barrier_state.items()):
                for stage, shards in list(stages.items()):
                    for shard, tracker in list(shards.items()):
                        if tracker['forwarded']:
                            continue
                        # Determine expected workers for this stage
                        expected_workers = tracker['expected'] or self.stage_expected.get(stage)
                        if expected_workers is None:
                            continue
                        tracker['expected'] = expected_workers
                        # Gate on END count and stats (if available)
                        ends_ok = tracker['received_end'] >= expected_workers
                        stats_expected = tracker.get('stats_expected_chunks')
                        if stats_expected is None:
                            stats_ok = False
                        else:
                            stats_ok = max(tracker.get('stats_received', 0), tracker.get('stats_processed', 0)) >= stats_expected
                        if ends_ok and stats_ok:
                            if now - tracker['last_forward_ts'] >= self.forward_interval:
                                self._forward_barrier(client_id, stage, shard, tracker)
                                tracker['last_forward_ts'] = now

    def _start_election(self):
        if self.election_in_progress: return
        logging.info("Starting election process...")
        self.election_in_progress = True
        self.election_start_time = time.time()
        self._broadcast(MSG_ELECTION)

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
            routing_key = "control.coordinator" if msg_type == MSG_COORDINATOR else "election.broadcast"
            channel.basic_publish(
                exchange=CONTROL_EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(msg)
            )
            connection.close()
        except Exception as e:
            logging.error(f"Broadcast error: {e}")

    def _revive_node(self, node_id):
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

    def _load_expected_from_config(self):
        cfg_path = os.environ.get("MONITOR_CONFIG_PATH", "/home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/config/config.ini")
        raw = {}
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if ":" not in line:
                        continue
                    key, val = line.split(":", 1)
                    key = key.strip()
                    val = val.strip()
                    try:
                        num = int(val)
                    except ValueError:
                        continue
                    raw[key] = num
        except FileNotFoundError:
            logging.warning(f"Monitor config not found at {cfg_path}, using defaults of 1 per stage.")
        except Exception as e:
            logging.error(f"Failed to load monitor config {cfg_path}: {e}")

        stage_map = {
            "FILTER_YEAR": STAGE_FILTER_YEAR,
            "FILTER_HOUR": STAGE_FILTER_HOUR,
            "FILTER_AMOUNT": STAGE_FILTER_AMOUNT,
            "AGGREGATOR_PRODUCTS": STAGE_AGG_PRODUCTS,
            "AGGREGATOR_TPV": STAGE_AGG_TPV,
            "AGGREGATOR_PURCHASES": STAGE_AGG_PURCHASES,
            "MAXIMIZER_MAX_PARTIAL": STAGE_MAX_PARTIALS,
            "MAXIMIZER_MAX_ABSOLUTE": STAGE_MAX_ABSOLUTE,
            "MAXIMIZER_TOP3_PARTIAL": STAGE_TOP3_PARTIALS,
            "MAXIMIZER_TOP3_ABSOLUTE": STAGE_TOP3_ABSOLUTE,
            "JOINER_ITEMS": STAGE_JOIN_ITEMS,
            "JOINER_STORES_TPV": STAGE_JOIN_STORES_TPV,
            "JOINER_STORES_TOP3": STAGE_JOIN_STORES_TOP3,
            "JOINER_USERS": STAGE_JOIN_USERS,
            "SERVER_RESULTS": STAGE_SERVER_RESULTS,
        }
        # For sharded stages we expect 1 worker per shard; config values represent shard count, not workers-per-shard.
        sharded_stages = {STAGE_AGG_PRODUCTS, STAGE_AGG_TPV, STAGE_AGG_PURCHASES, STAGE_FILTER_YEAR, STAGE_FILTER_HOUR, STAGE_FILTER_AMOUNT}
        stage_expected = {}
        for cfg_key, stage in stage_map.items():
            if stage in sharded_stages:
                stage_expected[stage] = 1
            else:
                stage_expected[stage] = raw.get(cfg_key, 1)
        return stage_expected

    # ===== Barrier handling =====
    def _handle_barrier_message(self, data):
        try:
            client_id = data.get('client_id')
            stage = data.get('stage')
            shard = data.get('shard') or DEFAULT_SHARD
            expected_workers = self.stage_expected.get(stage)
            sender = data.get('sender')
            chunks = data.get('chunks', 0)
            msg_type = data.get('type')
            tracker = self.barrier_state[client_id][stage][shard]
            if tracker.get('expected') is None and expected_workers is not None:
                tracker['expected'] = expected_workers
            if sender:
                tracker['sender_ids'].add(sender)
            if msg_type == MSG_WORKER_END:
                # Count END only once per sender
                if sender not in tracker['end_sender_ids']:
                    tracker['received_end'] += 1
                    tracker['end_sender_ids'].add(sender)
                tracker['total_chunks'] += chunks
            elif msg_type == MSG_WORKER_STATS:
                # Track latest stats
                tracker['stats_expected_chunks'] = data.get('expected') if data.get('expected') is not None else tracker.get('stats_expected_chunks')
                tracker['stats_received'] = max(tracker.get('stats_received', 0), data.get('chunks', 0))
                tracker['stats_processed'] = max(tracker.get('stats_processed', 0), data.get('processed', data.get('chunks', 0)))
            # Persist? (future) – could be written to disk; keeping in-memory for now.
        except Exception as e:
            logging.error(f"Barrier handle error: {e} | data:{data}")

    def _forward_barrier(self, client_id, stage, shard, tracker):
        try:
            # Build a BARRIER_FORWARD message
            msg = {
                'type': MSG_BARRIER_FORWARD,
                'client_id': client_id,
                'stage': stage,
                'shard': shard,
                'timestamp': time.time(),
                'total_chunks': tracker['total_chunks'],
                'senders': list(tracker['end_sender_ids']),
            }
            connection = self._get_connection()
            channel = connection.channel()
            channel.exchange_declare(exchange=COORDINATION_EXCHANGE, exchange_type='topic', durable=True)
            channel.basic_publish(
                exchange=COORDINATION_EXCHANGE,
                routing_key=f"{COORDINATION_ROUTING_KEY}.{stage}.{shard}",
                body=json.dumps(msg)
            )
            connection.close()
            tracker['forwarded'] = True
            logging.info(
                f"Barrier forward | client:{client_id} | stage:{stage} | shard:{shard} | total_chunks:{tracker['total_chunks']} | "
                f"ends:{tracker['received_end']}/{tracker.get('expected')} | stats:{tracker.get('stats_received',0)}/{tracker.get('stats_expected_chunks')}"
                f" | senders:{tracker['sender_ids']}")
        except Exception as e:
            logging.error(f"Barrier forward error: {e} | client:{client_id} | stage:{stage} | shard:{shard}")
