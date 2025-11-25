
# RabbitMQ Constants
HEARTBEAT_EXCHANGE = 'heartbeats_exchange'
ELECTION_EXCHANGE = 'election_exchange'
CONTROL_EXCHANGE = 'control_exchange'

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
HEARTBEAT_TIMEOUT = 10 # seconds
ELECTION_TIMEOUT = 10 # seconds
