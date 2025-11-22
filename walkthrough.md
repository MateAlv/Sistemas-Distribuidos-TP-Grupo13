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

### 3. Integration
The `Monitor` has been integrated into the `main.py` and core logic classes of:
- **Server**
- **Filter Workers**
- **Aggregator Workers**
- **Joiner Workers**
- **Maximizer Workers**

In each component, `monitor.pulse()` is called within the main processing loop to signal liveness.

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
Check the logs to see the election process and heartbeats (set `LOGGING_LEVEL=INFO` or `DEBUG` in `config.ini` or `docker-compose.yaml` if needed, though `Monitor` logs to stdout).
You should see logs like:
- `Starting Monitor for <container_name>`
- `I am the new LEADER!` (on one node)
- `Heartbeat received from ...` (on the leader)

### 3. Test Worker Failure
Kill a worker container manually:
```bash
docker kill <worker_container_name>
```
**Expected Behavior**:
- The Leader detects the missing heartbeat.
- The Leader logs `Node <worker_container_name> is DEAD. Restarting...`.
- The Leader executes `docker restart <worker_container_name>`.
- The worker starts up, joins the cluster, and starts sending heartbeats again.

### 4. Test Leader Failure
Identify the leader (look for "I am the new LEADER!" logs). Kill that container:
```bash
docker kill <leader_container_name>
```
**Expected Behavior**:
- Other nodes detect the Leader is gone (missing heartbeats).
- They initiate an election (`Sending ELECTION...`).
- A new Leader is elected (the remaining node with the highest ID).
- The new Leader takes over monitoring duties.
- The old Leader (if restarted by you or a restart policy) rejoins as a normal worker (or takes over if it has the highest ID).

### 5. Test Hung Process (Apoptosis)
To test this, you would need to artificially block the main thread of a worker (e.g., with a `time.sleep(100)` injected in the code).
**Expected Behavior**:
- The `Monitor` thread detects that `pulse()` hasn't been called for `HEARTBEAT_TIMEOUT`.
- The `Monitor` logs `Main process hung! Committing apoptosis...`.
- The process exits.
- The Leader detects the node death and restarts it.

## Notes
- **RabbitMQ Persistence**: The system relies on RabbitMQ's durability. Messages sent to a failed worker will remain in the queue until the worker restarts and reconnects.
- **Security**: Mounting the Docker socket gives containers root access to the host Docker daemon. This is acceptable for this academic project but should be secured in production.
