
# RabbitMQ Constants
HEARTBEAT_EXCHANGE = 'heartbeats_exchange'
ELECTION_EXCHANGE = 'election_exchange'
CONTROL_EXCHANGE = 'control_exchange'

# New coordination exchanges for END/stats barrier handled by monitor leader
COORDINATION_EXCHANGE = 'coordination_exchange'
COORDINATION_ROUTING_KEY = 'coordination.barrier'
DEFAULT_SHARD = 'global'

# New message types for barrier orchestration
MSG_WORKER_END = 'WORKER_END'
MSG_WORKER_STATS = 'WORKER_STATS'
MSG_BARRIER_FORWARD = 'BARRIER_FORWARD'

# Stage names used for centralized barrier. Keep consistent across workers.
STAGE_FILTER_YEAR = 'filter_year'
STAGE_FILTER_HOUR = 'filter_hour'
STAGE_FILTER_AMOUNT = 'filter_amount'
STAGE_AGG_PRODUCTS = 'agg_products'
STAGE_AGG_TPV = 'agg_tpv'
STAGE_AGG_PURCHASES = 'agg_purchases'
STAGE_MAX_PARTIALS = 'max_partials'
STAGE_MAX_ABSOLUTE = 'max_absolute'
STAGE_TOP3_PARTIALS = 'top3_partials'
STAGE_TOP3_ABSOLUTE = 'top3_absolute'
STAGE_JOIN_ITEMS = 'join_items'
STAGE_JOIN_STORES_TPV = 'join_stores_tpv'
STAGE_JOIN_STORES_TOP3 = 'join_stores_top3'
STAGE_JOIN_USERS = 'join_users'
STAGE_SERVER_RESULTS = 'server_results'

# Message Types
MSG_HEARTBEAT = 'HEARTBEAT'
MSG_ELECTION = 'ELECTION'
MSG_COORDINATOR = 'COORDINATOR'
MSG_DEATH = 'DEATH_CERTIFICATE'

# Routing Keys
# Heartbeats are routed by node ID: heartbeat.<node_id>
# Election messages are broadcast or targeted
ROUTING_KEY_HEARTBEAT = 'heartbeat.#'
ROUTING_KEY_ELECTION = 'election.#'
ROUTING_KEY_CONTROL = 'control.#'

# Timeouts (in seconds)
HEARTBEAT_INTERVAL = 2 # seconds
HEARTBEAT_TIMEOUT = 20 # seconds
ELECTION_TIMEOUT = 10 # seconds
