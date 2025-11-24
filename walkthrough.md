# Fault Tolerance Implementation Walkthrough

I have implemented a comprehensive fault tolerance system for the distributed application. This system ensures that the application can recover from node failures (workers or server) and handle hung processes.

## Key Components

### 1. Monitor Sidecar (`utils/monitor.py`)
A `Monitor` class has been added to every process (Server, Workers). It runs in a separate thread and performs the following duties:
- **Heartbeats**: Sends periodic "I am alive" messages to a RabbitMQ exchange (`control_exchange`).
- **Leader Election**: Implements the **Bully Algorithm** to elect a leader among the active nodes. The node with the highest lexicographical `CONTAINER_NAME` becomes the leader.
- **Health Checks**: The Leader monitors the heartbeats of all other nodes. If a node is silent for `HEARTBEAT_TIMEOUT` (6 seconds), it is considered dead.
- **Recovery**: The Leader uses the Docker CLI to restart the failed container (`docker restart <container_name>`).
- **Apoptosis**: If the main process hangs (fails to call `monitor.pulse()`), the Monitor thread detects this and terminates the process (`os._exit(1)`), triggering a restart by the Leader (or Docker's restart policy if configured).

### 2. Infrastructure Changes
- **Docker Socket**: The Docker socket (`/var/run/docker.sock`) is mounted into all containers. This allows the Leader (whichever node it is) to issue Docker commands.
- **Docker CLI**: The Docker CLI binary is installed in the `Dockerfile` of the Server and all Workers.
- **Environment Variables**: `CONTAINER_NAME` is injected into every container to serve as a unique ID.

### 3. Chaos Monkey (`chaos_monkey/`)
A new service that randomly kills containers to test the system's resilience.
- **Configurable**: Interval and enablement via `config.ini`.
- **Targeting**: Randomly selects valid targets (excluding itself and infrastructure).
- **Manual Trigger**: Can be triggered via `SIGUSR1`.

## Verification

### Prerequisites
Ensure you have built the latest images:
```bash
make docker-image
```

### 1. Start the System
Start the system as usual:
```bash
make docker-compose-up
```

### 2. Verify Normal Operation
Check the logs to see the election process and heartbeats.

### 3. Test Worker Failure
Kill a worker container manually:
```bash
docker kill <worker_container_name>
```
**Expected Behavior**:
- The Leader detects the missing heartbeat.
- The Leader logs `Node <worker_container_name> is DEAD. Restarting...`.
- The Leader executes `docker restart <worker_container_name>`.

### 4. Test Leader Failure
Identify the leader and kill it.
**Expected Behavior**:
- Other nodes detect the Leader is gone.
- They initiate an election.
- A new Leader is elected.

### 5. Test Chaos Monkey
The Chaos Monkey is available to automate failure testing.
**Configuration**:
- In `config/config.ini`, set `CHAOS_ENABLED: true` and `CHAOS_INTERVAL: 30`.
- Re-generate compose: `python3 scripts/generar-compose.py --config config/config.ini`
- Re-build and start.

**Manual Trigger**:
```bash
docker kill --signal=SIGUSR1 chaos_monkey
```
Check the logs (`docker logs chaos_monkey`) to see which container was targeted.
