# Chaos Monkey Implementation Plan

## Goal Description
Implement a "Chaos Monkey" service that can randomly or specifically kill containers to test the fault tolerance of the system. It should be configurable (interval, targets) and optional in the deployment.

## User Review Required
> [!IMPORTANT]
> The Chaos Monkey will have access to the Docker socket, similar to the Monitor. This allows it to kill any container on the host if not carefully scoped. In this environment, it will be scoped to the project's containers via naming conventions.

## Proposed Changes

### Chaos Monkey Service
#### [NEW] [chaos_monkey/Dockerfile](file:///home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/chaos_monkey/Dockerfile)
- Python base image.
- Install Docker CLI.

#### [NEW] [chaos_monkey/main.py](file:///home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/chaos_monkey/main.py)
- Main logic loop.
- Reads configuration from environment variables.
- Uses `subprocess` to call `docker kill`.
- Implements signal handler (SIGUSR1) for manual triggering.

#### [NEW] [chaos_monkey/manager.py](file:///home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/chaos_monkey/manager.py)
- Logic for selecting targets (random vs specific).
- Logic for excluding critical containers (e.g., rabbitmq, itself).

### Infrastructure
#### [MODIFY] [scripts/generar-compose.py](file:///home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/scripts/generar-compose.py)
- Add `define_chaos_monkey` function.
- Parse `CHAOS_MONKEY` configuration from the input config file.
- Add `chaos_monkey` service to `docker-compose.yaml` if enabled.

### Configuration
#### [MODIFY] [config.ini](file:///home/mate/FIUBA/sistemas-distribuidos/Sistemas-Distribuidos-TP-Grupo13/config.ini)
- Add Chaos Monkey configuration section (commented out by default or enabled for testing).

## Verification Plan
### Automated Tests
- None specific for this tool, but it will be used to test the system.

### Manual Verification
1.  Enable Chaos Monkey in config.
2.  Start system.
3.  Observe logs of `chaos_monkey` service.
4.  Verify containers are being killed.
5.  Verify `Monitor` detects and restarts them.
6.  Test "trigger at will" by sending SIGUSR1 to `chaos_monkey` container.
