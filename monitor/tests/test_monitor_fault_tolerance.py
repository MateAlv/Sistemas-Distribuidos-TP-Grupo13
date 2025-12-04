import subprocess
import yaml
import time
import os
import sys
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs.txt", mode='a')
    ]
)

CONFIG_MONITOR = os.environ.get("MONITOR_TEST_CONFIG", "config/config-monitor.ini")
COMPOSE_SCRIPT = "scripts/generar-compose.py"

def run_command(command, check=True, capture_output=True):
    """Runs a shell command."""
    logging.info(f"Running command: {command}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=check,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {command}")
        logging.error(f"Stdout: {e.stdout}")
        logging.error(f"Stderr: {e.stderr}")
        raise

def get_services_from_compose(compose_file="docker-compose.yaml"):
    """Reads docker-compose.yaml and returns a list of non-client services."""
    with open(compose_file, 'r') as f:
        compose_data = yaml.safe_load(f)
    
    services = []
    for service_name in compose_data.get('services', {}):
        if not service_name.startswith('client'):
            services.append(service_name)
    return services

def wait_for_leader_election(timeout=60):
    """Waits for a leader to be elected by checking logs."""
    logging.info("Waiting for Leader Election...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check logs of all monitors
        result = run_command("grep -E 'I am the new LEADER!|New Leader elected' logs.txt", check=False)
        if result.returncode == 0 and result.stdout.strip():
            logging.info("Leader elected!")
            return True
        time.sleep(2)

    logging.error("Timeout waiting for leader election.")
    return False

def _last_tracked_nodes_from_logs(log_path="logs.txt"):
    """
    Parses the last 'tracking nodes' line from logs.txt and returns the list of nodes.
    """
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        tracked_lines = [l for l in lines if "tracking nodes:" in l]
        if not tracked_lines:
            return []
        last_line = tracked_lines[-1]
        match = re.search(r"tracking nodes: \[(.*)\]", last_line)
        if not match:
            return []
        raw_list = match.group(1)
        return [n.strip().strip("'") for n in raw_list.split(",") if n.strip()]
    except Exception as exc:
        logging.error(f"Failed to parse tracking nodes: {exc}")
        return []

def choose_victim_from_tracking():
    """
    Pick an aggregator that the leader is actually tracking to avoid killing
    a container the monitor never saw.
    """
    tracked = _last_tracked_nodes_from_logs()
    for node in tracked:
        if "aggregator" in node:
            logging.info(f"Choosing victim from tracking list: {node}")
            return node
    logging.warning("No aggregator found in tracking list; falling back to first aggregator service in compose.")
    return None

def latest_leader_from_logs(log_path="logs.txt"):
    """
    Returns the last monitor id that logged 'I am the new LEADER!'.
    """
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        leader_lines = [l for l in lines if "I am the new LEADER!" in l]
        if not leader_lines:
            return None
        last_line = leader_lines[-1]
        m = re.match(r"^(monitor_[0-9]+)", last_line.strip())
        if m:
            return m.group(1)
    except Exception as exc:
        logging.error(f"Failed to parse leader from logs: {exc}")
    return None

def wait_for_new_leader(prev_leader, timeout=60):
    """
    Wait until a new leader (different from prev_leader) is observed in logs.
    """
    logging.info(f"Waiting for new leader (prev:{prev_leader}) timeout={timeout}s...")
    start = time.time()
    while time.time() - start < timeout:
        leader = latest_leader_from_logs()
        if leader and leader != prev_leader:
            logging.info(f"New leader detected in logs: {leader}")
            return leader
        time.sleep(2)
    raise Exception("Timeout waiting for new leader election.")

def wait_for_leader_log(timeout=60):
    """
    Wait until a leader has logged 'I am the new LEADER!', returning its id.
    """
    logging.info(f"Waiting for leader log... timeout={timeout}s")
    start = time.time()
    while time.time() - start < timeout:
        leader = latest_leader_from_logs()
        if leader:
            logging.info(f"Leader detected in logs: {leader}")
            return leader
        time.sleep(2)
    raise Exception("Timeout waiting for leader log.")

def wait_for_revival(victim_container, timeout=120):
    """Waits for the monitor to revive the victim container."""
    logging.info(f"Waiting for revival of {victim_container} (timeout={timeout}s)...")
    start_time = time.time()
    
    detected = False
    revived = False
    
    while time.time() - start_time < timeout:
        # Check for detection
        if not detected:
            result = run_command(f"grep 'Node {victim_container} died. Reviving...' logs.txt", check=False)
            if result.returncode == 0 and result.stdout.strip():
                logging.info(f"Monitor detected death of {victim_container}.")
                detected = True
        
        # Check for restart command
        if not revived:
            result = run_command(f"grep 'Restarting container {victim_container}...' logs.txt", check=False)
            if result.returncode == 0 and result.stdout.strip():
                logging.info(f"Monitor triggered restart for {victim_container}.")
                revived = True
        
        # Check actual container status
        if detected and revived:
            result = run_command(f"docker inspect -f '{{{{.State.Status}}}}' {victim_container}", check=False)
            if result.returncode == 0 and result.stdout.strip() == "running":
                logging.info(f"Container {victim_container} is running again!")
                return True
            
        time.sleep(2)
        
    logging.error(f"Timeout waiting for revival of {victim_container}.")
    logging.error("Debug: Docker PS")
    run_command("docker ps", check=False, capture_output=False)
    logging.error("Debug: Monitor Logs")
    run_command("grep 'monitor' logs.txt | tail -n 20", check=False, capture_output=False)
    return False

def main():
    try:
        # 0. Cleanup previous runs (mirror make test prep)
        logging.info("Cleaning up previous runs...")
        run_command("make clean-results", check=False)
        run_command("make down", check=False)
        run_command("docker rm -f rabbitmq", check=False)
        # Reset logs.txt
        try:
            open("logs.txt", "w").close()
        except Exception:
            pass

        # 1. Generate docker-compose.yaml with no clients
        logging.info("Generating docker-compose.yaml (no clients)...")
        run_command(f"python3 {COMPOSE_SCRIPT} --config={CONFIG_MONITOR}")
        
        # 2. Identify services
        services = get_services_from_compose()
        logging.info(f"Services to start: {services}")
        
        # 3. Start Infrastructure (Background logs)
        logging.info("Starting infrastructure...")
        services_str = " ".join(services)
        run_command(f"docker compose up -d --build {services_str}")
        
        # Start logging in background
        log_process = subprocess.Popen(
            "docker compose logs -f > logs.txt 2>&1",
            shell=True,
            preexec_fn=os.setsid
        )
        
        # 4. Wait for Leader Election
        if not wait_for_leader_election():
            raise Exception("Leader election failed.")

        # 4b. Allow heartbeats to propagate so leader tracks nodes
        logging.info("Waiting for heartbeats to populate tracking (10s)...")
        time.sleep(10)

        # Ensure a leader has actually logged before proceeding
        current_leader = wait_for_leader_log()
            
        # 5. Scenario: Service Revival
        victim = choose_victim_from_tracking()
        if victim is None:
            victim = next((s for s in services if "aggregator" in s), None)
        if not victim:
            raise Exception("No suitable victim found.")
            
        logging.info(f"Killing victim: {victim}")
        run_command(f"docker kill {victim}")
        
        # Verify it's dead
        result = run_command(f"docker inspect -f '{{{{.State.Status}}}}' {victim}", check=False)
        if result.returncode == 0 and result.stdout.strip() == "running":
             logging.error(f"Failed to kill {victim}. It is still running.")
             raise Exception(f"Failed to kill {victim}")
        logging.info(f"{victim} killed successfully.")
        
        if not wait_for_revival(victim):
            raise Exception(f"Revival of {victim} failed.")
            
        logging.info("Test Passed: Service Revival Verified.")

        # 6. Kill current leader monitor and wait for new election
        prev_leader = latest_leader_from_logs()
        if not prev_leader:
            raise Exception("Could not determine current leader from logs.")
        logging.info(f"Killing leader monitor: {prev_leader}")
        run_command(f"docker kill {prev_leader}")
        result = run_command(f"docker inspect -f '{{{{.State.Status}}}}' {prev_leader}", check=False)
        if result.returncode == 0 and result.stdout.strip() == "running":
            logging.error(f"Failed to kill {prev_leader}. It is still running.")
            raise Exception(f"Failed to kill {prev_leader}")
        logging.info(f"{prev_leader} killed successfully.")

        new_leader = wait_for_new_leader(prev_leader)
        logging.info(f"New leader elected after killing {prev_leader}: {new_leader}")

        # 7. Kill another tracked worker and verify revival under new leader
        victim2 = choose_victim_from_tracking()
        if victim2 is None or victim2 == victim:
            victim2 = next((s for s in services if "aggregator" in s and s != victim), None)
        if not victim2:
            raise Exception("No second suitable victim found.")

        logging.info(f"Killing second victim under new leader: {victim2}")
        run_command(f"docker kill {victim2}")
        result = run_command(f"docker inspect -f '{{{{.State.Status}}}}' {victim2}", check=False)
        if result.returncode == 0 and result.stdout.strip() == "running":
            logging.error(f"Failed to kill {victim2}. It is still running.")
            raise Exception(f"Failed to kill {victim2}")
        logging.info(f"{victim2} killed successfully.")

        if not wait_for_revival(victim2):
            raise Exception(f"Revival of {victim2} failed after leader failover.")

        logging.info("Test Passed: Leader failover and second service revival verified.")
        
    except Exception as e:
        logging.error(f"Test Failed: {e}")
        sys.exit(1)
    finally:
        # Cleanup
        logging.info("Cleaning up...")
        if 'log_process' in locals():
            os.killpg(os.getpgid(log_process.pid), 15) # Terminate log streamer
        
        # Dump full logs
        logging.info("Dumping full logs to full_logs.txt...")
        run_command("docker compose logs > full_logs.txt 2>&1", check=False)
        
        # run_command("make down", check=False)

if __name__ == "__main__":
    main()
