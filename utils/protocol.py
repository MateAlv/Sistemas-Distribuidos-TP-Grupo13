
# RabbitMQ Constants
HEARTBEAT_EXCHANGE = 'heartbeats_exchange'
ELECTION_EXCHANGE = 'election_exchange'
CONTROL_EXCHANGE = 'control_exchange'

# Message Types
MSG_HEARTBEAT = 'heartbeat'
MSG_ELECTION = 'election'
MSG_COORDINATOR = 'coordinator'
MSG_DEATH = 'death'
MSG_FORCE_END = 'force_end'
MSG_FORCE_END_CLIENT = 'force_end_client'

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
