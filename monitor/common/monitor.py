import threading
import time
import logging
import os
import json
import subprocess
import pika
from collections import defaultdict
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

# --- Barrier Tracker ---
class BarrierTracker:
    def __init__(self):
        self.received_end = 0
        self.end_sender_ids = set()
        self.total_chunks = 0
        self.last_forward_ts = 0
        self.forwarded = False
        
        # Aggregator Specific
        self.agg_expected = None
        self.agg_processed = 0
        self.expected_senders = set()
        self.expected_logged = False
        self.expected_by_sender = {}

    def apply_expected(self, expected, sender_id=None, expected_filters=None):
        """Updates expected count from upstream (e.g. Filters telling Aggregators)."""
        if expected is None:
            return
        sender_key = str(sender_id) if sender_id is not None else "unknown"
        self.expected_by_sender[sender_key] = expected
        # Sum all sender expectations (multiple filters feed the same shard)
        self.agg_expected = sum(self.expected_by_sender.values())
        if sender_id is not None:
            self.expected_senders.add(str(sender_id))
        # Always log with a clear counter
        expected_total = expected_filters if expected_filters is not None else "?"
        logging.info(
            f"Aggregator Barrier Expected update from {sender_id}: "
            f"{len(self.expected_senders)}/{expected_total} reports | agg_expected:{self.agg_expected}"
        )
        if expected_filters is not None and len(self.expected_senders) >= expected_filters and not self.expected_logged:
            logging.info(
                f"OKAY I GOT THE GLOBAL AMOUNT OF EXPECTED CHUNKS! upstream_expected_filters={expected_filters} | agg_expected={self.agg_expected}"
            )
            self.expected_logged = True

    def apply_stats(self, chunks, processed, expected=None, sender_id=None, expected_filters=None):
        """Updates stats from a worker."""
        if expected is not None:
            self.apply_expected(expected, sender_id, expected_filters)
            
        # For Aggregators, 'processed' is what matters against 'agg_expected'
        self.agg_processed = max(self.agg_processed, processed)
        logging.info(f"Aggregator Barrier Processed Chunks so far: {self.agg_processed} | EXPECTED:{self.agg_expected}")

    def apply_end(self, sender_id, chunks, expected_filters=None):
        """Handles an END message."""
        if sender_id not in self.end_sender_ids:
            self.received_end += 1
            self.end_sender_ids.add(sender_id)
        self.total_chunks += chunks
        if expected_filters:
            logging.info(f"Aggregator Barrier Received END from {sender_id}: {self.received_end}/{expected_filters}")
        else:
            logging.info(f"Aggregator Barrier Received END from {sender_id}: {self.received_end}")

    def is_complete(self, stage_type, now, forward_interval, expected_filters):
        """
        Determines if the barrier is complete.
        Strictly for Aggregator stages:
        1. agg_expected is known (Filters reported).
        2. agg_processed >= agg_expected.
        3. received_end >= expected_filters (Safety check: all filters finished).
        """
        if self.forwarded:
            return False, "Already forwarded"
        
        # Gate: agg_processed >= agg_expected
        if self.agg_expected is not None and self.agg_processed >= self.agg_expected:
            # Safety check: Ensure we heard END from all upstream filters
            if expected_filters is not None and self.received_end < expected_filters:
                 return False, f"Aggregator Barrier Pending: Waiting for Filters END {self.received_end}/{expected_filters}"

            return True, f"Aggregator Barrier Met: processed {self.agg_processed} >= expected {self.agg_expected}"
            
        return False, f"Aggregator Barrier Pending: processed {self.agg_processed} < {self.agg_expected}"

    def to_dict(self):
        return {
            'received_end': self.received_end,
            'end_sender_ids': list(self.end_sender_ids),
            'total_chunks': self.total_chunks,
            'last_forward_ts': self.last_forward_ts,
            'forwarded': self.forwarded,
            'agg_expected': self.agg_expected,
            'agg_processed': self.agg_processed,
            'expected_senders': list(self.expected_senders),
            'expected_by_sender': self.expected_by_sender,
        }

    @classmethod
    def from_dict(cls, data):
        t = cls()
        t.received_end = data.get('received_end', 0)
        t.end_sender_ids = set(data.get('end_sender_ids', []))
        t.total_chunks = data.get('total_chunks', 0)
        t.last_forward_ts = data.get('last_forward_ts', 0)
        t.forwarded = data.get('forwarded', False)
        t.agg_expected = data.get('agg_expected')
        t.agg_processed = data.get('agg_processed', 0)
        t.expected_senders = set(data.get('expected_senders', []))
        t.expected_by_sender = data.get('expected_by_sender', {})
        # Ignore legacy fields if present in old state files
        return t

# --- Barrier State ---
class BarrierState:
    def __init__(self):
        # client -> stage -> shard -> BarrierTracker
        self.trackers = defaultdict(lambda: defaultdict(lambda: defaultdict(BarrierTracker)))
        self.lock = threading.Lock()

    def get_tracker(self, client_id, stage, shard):
        return self.trackers[client_id][stage][shard]

    def remove_tracker(self, client_id, stage, shard):
        if shard in self.trackers[client_id][stage]:
            del self.trackers[client_id][stage][shard]

    def snapshot(self):
        """Returns a dict representation for persistence."""
        with self.lock:
            state = {}
            for client, stages in self.trackers.items():
                state[client] = {}
                for stage, shards in stages.items():
                    state[client][stage] = {}
                    for shard, tracker in shards.items():
                        state[client][stage][shard] = tracker.to_dict()
            return state

    def restore(self, state_dict):
        """Restores state from dict."""
        with self.lock:
            for client, stages in state_dict.items():
                for stage, shards in stages.items():
                    for shard, data in shards.items():
                        self.trackers[client][stage][shard] = BarrierTracker.from_dict(data)


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
        self.nodes_last_seen = {} 
        
        # Election State
        self.election_in_progress = False
        self.election_start_time = 0

        # Barrier State
        self.state = BarrierState()
        
        # Persistence
        self.persistence_path = "/data/persistence/monitor_state.json"
        self._recover_state()

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
        # No forced election on boot; rely on heartbeats + checker loop
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
                    routing_key = f"heartbeat.monitor.{self.node_id}"
                    
                    channel.basic_publish(
                        exchange=HEARTBEAT_EXCHANGE,
                        routing_key=routing_key,
                        body=json.dumps(msg)
                    )
                    logging.info(f"action: send_heartbeat | node:{self.node_id} | is_leader:{self.is_leader} | leader_id:{self.leader_id}")
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
                
                queue_name = f"monitor_listener_{self.node_id}"
                channel.queue_declare(queue=queue_name, exclusive=True)
                
                # Bindings
                channel.queue_bind(exchange=HEARTBEAT_EXCHANGE, queue=queue_name, routing_key='heartbeat.#')
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="election.#")
                channel.queue_bind(exchange=CONTROL_EXCHANGE, queue=queue_name, routing_key="control.#")
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
            
            if msg_type != MSG_HEARTBEAT:
                logging.info(f"Monitor received message: {data}")

            if msg_type == MSG_HEARTBEAT:
                component = data.get('component')
                self.nodes_last_seen[sender_id] = time.time()
                
                if component == 'monitor':
                    is_sender_leader = data.get('is_leader')
                    # Always refresh heartbeat if it is from the leader we are tracking
                    if self.leader_id and sender_id == self.leader_id:
                        self.last_leader_heartbeat = time.time()
                        logging.info(f"action: heartbeat_from_leader | leader:{sender_id}")

                    if is_sender_leader:
                        # Adopt leader if none or higher, but only log when it changes
                        if (self.leader_id is None) or (sender_id > self.leader_id):
                            prev_leader = self.leader_id
                            self.leader_id = sender_id
                            self.last_leader_heartbeat = time.time()
                            self.election_in_progress = False
                            self.is_leader = (self.node_id == sender_id)
                            if self.is_leader:
                                logging.info("I am the new LEADER! (via heartbeat)")
                            else:
                                logging.info(f"Recognizing leader via heartbeat: {sender_id} (prev:{prev_leader})")
                        elif sender_id == self.leader_id:
                            # Same leader, just refresh heartbeat; do not spam logs
                            self.last_leader_heartbeat = time.time()
                        else:
                            logging.info(f"Ignoring lower-ID leader heartbeat from {sender_id} while tracking {self.leader_id}")

                    # Only cancel an election if the heartbeat comes from a node asserting leadership
                    if self.election_in_progress and is_sender_leader and sender_id > self.node_id:
                        logging.info(f"Cancelling election; leader heartbeat from higher-ID monitor {sender_id}")
                        self.election_in_progress = False

            elif msg_type == MSG_ELECTION:
                if sender_id < self.node_id:
                    logging.info(f"Received ELECTION from {sender_id}. Sending my own election message.")
                    self._start_election()
                else:
                    logging.info(f"Received ELECTION from {sender_id}. Yielding (higher ID).")

            elif msg_type == MSG_COORDINATOR:
                # Bully: accept only if coordinator has higher ID than me AND higher than any tracked leader
                if (sender_id > self.node_id) and ((self.leader_id is None) or (sender_id > self.leader_id)):
                    self.leader_id = sender_id
                    self.last_leader_heartbeat = time.time()
                    self.election_in_progress = False
                    self.is_leader = (self.node_id == sender_id)
                    if self.is_leader:
                        logging.info("I am the new LEADER! (via coordinator)")
                    else:
                        logging.info(f"New Leader elected: {sender_id}")
                        logging.info(f"Accepting {sender_id} as Coordinator.")
                else:
                    logging.info(f"Ignoring coordinator from {sender_id} (my_id:{self.node_id}, current_leader:{self.leader_id})")
            elif msg_type in (MSG_WORKER_END, MSG_WORKER_STATS):
                self._handle_barrier_message(data)

        except Exception as e:
            logging.error(f"Error handling message: {e}")

    def _checker_loop(self):
        """Checks for timeouts"""
        while self.running:
            time.sleep(1)
            
            if self.election_in_progress:
                if time.time() - self.election_start_time > ELECTION_TIMEOUT:
                    logging.info("Election timeout reached. Declaring victory.")
                    self.is_leader = True
                    self.leader_id = self.node_id
                    self._broadcast(MSG_COORDINATOR)
                    self.election_in_progress = False
                    logging.info("I am the new LEADER!")
                continue

            if self.is_leader:
                if int(time.time()) % 5 == 0:
                    logging.info(f"Leader {self.node_id} tracking nodes: {list(self.nodes_last_seen.keys())}")

                for node_id, last_seen in list(self.nodes_last_seen.items()):
                    if node_id == self.node_id: continue
                    if time.time() - last_seen > HEARTBEAT_TIMEOUT:
                        logging.info(f"Node {node_id} died. Reviving...")
                        self._revive_node(node_id)
                        del self.nodes_last_seen[node_id]
            else:
                if time.time() - self.last_leader_heartbeat > HEARTBEAT_TIMEOUT:
                    logging.warning(f"Leader heartbeat timeout (>{HEARTBEAT_TIMEOUT}s). Starting election... | last_leader:{self.leader_id}")
                    self._start_election()

    def _forward_loop(self):
        """Periodically forward barrier completion messages downstream"""
        while self.running:
            time.sleep(1)
            
            now = time.time()
            forwards_to_send = []
            trackers_to_prune = []
            
            # 1. Snapshot / Iterate safely under lock
            with self.state.lock:
                for client_id, stages in self.state.trackers.items():
                    for stage, shards in list(stages.items()):
                        # Only process Aggregator stages
                        if stage not in [STAGE_AGG_PRODUCTS, STAGE_AGG_TPV, STAGE_AGG_PURCHASES]:
                            continue

                        for shard, tracker in list(shards.items()):
                            # Determine upstream expected count: prefer actual senders, fallback to config
                            configured = 0
                            if stage == STAGE_AGG_PRODUCTS:
                                configured = self.stage_expected.get(STAGE_FILTER_YEAR, 1)
                            elif stage == STAGE_AGG_PURCHASES:
                                configured = self.stage_expected.get(STAGE_FILTER_YEAR, 1)
                            elif stage == STAGE_AGG_TPV:
                                configured = self.stage_expected.get(STAGE_FILTER_HOUR, 1)
                            upstream_count = len(tracker.expected_by_sender) if len(tracker.expected_by_sender) > 0 else configured or 1
                            # For END safety, aggregators are 1 per shard
                            end_threshold = 1

                            complete, reason = tracker.is_complete(stage, now, self.forward_interval, end_threshold)
                            
                            if complete:
                                tracker.forwarded = True
                                tracker.last_forward_ts = now
                                forwards_to_send.append((client_id, stage, shard, tracker.total_chunks, list(tracker.end_sender_ids), reason))
                                trackers_to_prune.append((client_id, stage, shard))
            
            # 2. Send messages (I/O outside lock)
            if forwards_to_send:
                self._save_state() # Save state before sending
                for (client, stage, shard, total, senders, reason) in forwards_to_send:
                    self._forward_barrier(client, stage, shard, total, senders, reason)
                
                # 3. Prune completed trackers
                with self.state.lock:
                    for (client, stage, shard) in trackers_to_prune:
                        self.state.remove_tracker(client, stage, shard)
                self._save_state() # Save state after pruning

    def _handle_barrier_message(self, data):
        try:
            client_id = data.get('client_id')
            stage = data.get('stage')
            shard = data.get('shard') or DEFAULT_SHARD
            sender = data.get('sender')
            chunks = data.get('chunks', 0)
            msg_type = data.get('type')
            
            # Determine upstream expected count for logging (filters feeding each aggregator stage)
            upstream_config = 0
            if stage == STAGE_AGG_PRODUCTS:
                upstream_config = self.stage_expected.get(STAGE_FILTER_YEAR, 1)
            elif stage == STAGE_AGG_PURCHASES:
                upstream_config = self.stage_expected.get(STAGE_FILTER_YEAR, 1)
            elif stage == STAGE_AGG_TPV:
                upstream_config = self.stage_expected.get(STAGE_FILTER_HOUR, 1)
            tracker = self.state.get_tracker(client_id, stage, shard)
            # Prefer real senders if present, fallback to config, never below 1
            upstream_count = len(tracker.expected_by_sender) if len(tracker.expected_by_sender) > 0 else upstream_config or 1
            
            # Only process Aggregator stages
            if stage not in [STAGE_AGG_PRODUCTS, STAGE_AGG_TPV, STAGE_AGG_PURCHASES]:
                return

            logging.info(
                f"Processing barrier message: client={client_id}, stage={stage}, shard={shard}, "
                f"type={msg_type}, chunks={chunks}, processed={data.get('processed')}, "
                f"expected={data.get('expected')} | upstream_expected_filters:{upstream_count}"
            )

            with self.state.lock:
                tracker = self.state.get_tracker(client_id, stage, shard)
                
                if msg_type == MSG_WORKER_END:
                    tracker.apply_end(sender, chunks, upstream_count)
                elif msg_type == MSG_WORKER_STATS:
                    processed = data.get('processed', chunks) 
                    expected = data.get('expected')
                    tracker.apply_stats(chunks, processed, expected, sender_id=sender, expected_filters=upstream_count)
                    
                    logging.info(f"Tracker Updated | client:{client_id} | stage:{stage} | shard:{shard} | "
                                 f"AggProcessed:{tracker.agg_processed} | AggExpected:{tracker.agg_expected} | "
                                 f"ExpectedReports:{len(tracker.expected_senders)}/{upstream_count} | "
                                 f"Ends:{tracker.received_end}")
                    logging.info(
                        f"Barrier State | client:{client_id} | stage:{stage} | shard:{shard} | "
                        f"processed:{tracker.agg_processed} | expected:{tracker.agg_expected} | "
                        f"ends:{tracker.received_end}/1"
                    )
                
            self._save_state()
        except Exception as e:
            logging.error(f"Barrier handle error: {e} | data:{data}")

    def _save_state(self):
        try:
            state_dict = self.state.snapshot()
            os.makedirs(os.path.dirname(self.persistence_path), exist_ok=True)
            with open(self.persistence_path, 'w') as f:
                json.dump(state_dict, f)
        except Exception as e:
            logging.error(f"Error saving state: {e}")

    def _recover_state(self):
        try:
            if os.path.exists(self.persistence_path):
                with open(self.persistence_path, 'r') as f:
                    state_dict = json.load(f)
                    self.state.restore(state_dict)
                logging.info("Monitor state recovered.")
        except Exception as e:
            logging.error(f"Error recovering state: {e}")

    def _forward_barrier(self, client_id, stage, shard, total_chunks, senders, reason):
        try:
            msg = {
                'type': MSG_BARRIER_FORWARD,
                'client_id': client_id,
                'stage': stage,
                'shard': shard,
                'timestamp': time.time(),
                'total_chunks': total_chunks,
                'senders': senders,
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
            logging.info(f"BARRIER FORWARD SENT | client:{client_id} | stage:{stage} | shard:{shard} | Reason: {reason}")
        except Exception as e:
            logging.error(f"Barrier forward error: {e} | client:{client_id} | stage:{stage} | shard:{shard}")

    def _start_election(self):
        if self.election_in_progress: return
        logging.info("Starting election process...")
        self.election_in_progress = True
        # Forget any previous leader while we elect
        self.leader_id = None
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
        self._publish_death(node_id)
        try:
            import shutil
            docker_path = shutil.which('docker')
            logging.info(f"Docker path: {docker_path}")
            # logging.info(f"Env: {os.environ}")

            logging.info(f"Restarting container {node_id}...")
            result = subprocess.run(
                ['docker', 'restart', node_id],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                logging.info(f"Successfully restarted {node_id}. Output: {result.stdout}")
            else:
                logging.error(f"Failed to restart {node_id}. Return code: {result.returncode}. Stderr: {result.stderr}")
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
                    if not line or line.startswith("#"): continue
                    if ":" not in line: continue
                    key, val = line.split(":", 1)
                    try: raw[key.strip()] = int(val.strip())
                    except ValueError: continue
        except FileNotFoundError:
            logging.warning(f"Monitor config not found at {cfg_path}, using defaults.")
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
        # Load actual shard counts for all stages
        stage_expected = {}
        for cfg_key, stage in stage_map.items():
            stage_expected[stage] = raw.get(cfg_key, 1)
        return stage_expected
