import subprocess
import time
import logging
import sys
import re

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

COMPOSE_FILE = "tests/compose-test.yaml"

def run_command(cmd, check=True):
    logging.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        logging.error(f"Command failed: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result

def get_logs(container):
    cmd = ["docker", "logs", container]
    result = run_command(cmd, check=False)
    return result.stdout + result.stderr

def wait_for_log(container, pattern, timeout=30):
    logging.info(f"Waiting for pattern '{pattern}' in {container} logs...")
    start = time.time()
    while time.time() - start < timeout:
        logs = get_logs(container)
        if re.search(pattern, logs):
            logging.info(f"Found pattern '{pattern}' in {container}!")
            return True
        time.sleep(1)
    logging.error(f"Timeout waiting for '{pattern}' in {container}")
    return False

def main():
    try:
        # 1. Start Cluster
        logging.info("Starting test cluster...")
        run_command(["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--build"])
        
        # 2. Wait for Leader Election (monitor_3 should win as it has highest ID)
        logging.info("Waiting for Leader Election...")
        if not wait_for_log("monitor_3", "I am the new LEADER!", timeout=60):
            raise Exception("monitor_3 failed to become leader")
            
        # 3. Verify monitor_1 and monitor_2 recognize leader
        # They should log "New Leader elected: monitor_3"
        if not wait_for_log("monitor_1", "New Leader elected: monitor_3", timeout=30):
             raise Exception("monitor_1 did not recognize monitor_3 as leader")

        # Wait for heartbeats to populate
        logging.info("Waiting for heartbeats to populate...")
        time.sleep(10)

        # 4. Kill a Worker (worker_1)
        logging.info("Killing Worker worker_1...")
        run_command(["docker", "kill", "worker_1"])
        
        # 5. Verify Leader detects and restarts it
        if not wait_for_log("monitor_3", "Node worker_1 died. Reviving...", timeout=60):
            raise Exception("Leader did not detect/revive dead worker")
            
        # Wait for worker_1 to come back
        logging.info("Waiting for worker_1 to restart...")
        time.sleep(5) # Give it time to boot
        # Check if worker_1 is running
        res = run_command(["docker", "inspect", "-f", "{{.State.Running}}", "worker_1"])
        if "true" not in res.stdout:
             raise Exception("worker_1 failed to restart")
        logging.info("worker_1 restarted successfully.")

        # 6. Kill the Leader (monitor_3)
        logging.info("Killing Leader monitor_3...")
        run_command(["docker", "kill", "monitor_3"])
        
        # 7. Verify monitor_2 becomes new Leader (next highest ID)
        logging.info("Waiting for monitor_2 to become new Leader...")
        if not wait_for_log("monitor_2", "I am the new LEADER!", timeout=60):
            raise Exception("monitor_2 failed to become new leader")
            
        # 8. Verify monitor_3 is revived (by monitor_2 or monitor_1 due to race)
        logging.info("Waiting for monitor_3 to be revived...")
        start = time.time()
        revived = False
        while time.time() - start < 120:
            if re.search("Node monitor_3 died. Reviving...", get_logs("monitor_2")):
                logging.info("monitor_2 revived monitor_3!")
                revived = True
                break
            if re.search("Node monitor_3 died. Reviving...", get_logs("monitor_1")):
                logging.info("monitor_1 revived monitor_3!")
                revived = True
                break
            time.sleep(1)
        
        if not revived:
            raise Exception("monitor_3 was not revived by any monitor")

        logging.info("TEST PASSED!")

    except Exception as e:
        logging.error(f"TEST FAILED: {e}")
        logging.info("Dumping monitor_3 logs:")
        print(get_logs("monitor_3"))
        logging.info("Dumping monitor_2 logs:")
        print(get_logs("monitor_2"))
        sys.exit(1)
    finally:
        logging.info("Cleaning up...")
        run_command(["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"], check=False)

if __name__ == "__main__":
    main()
