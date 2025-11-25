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
        
        # 2. Wait for Leader Election (node_3 should win)
        logging.info("Waiting for Leader Election...")
        if not wait_for_log("node_3", "I am the new LEADER!", timeout=120):
            raise Exception("node_3 failed to become leader")
            
        # 3. Verify node_1 and node_2 recognize leader
        # Since we don't log "New Leader elected" by default unless I added it... 
        # I added it in the previous step! "New Leader elected: {sender_id}"
        if not wait_for_log("node_1", "New Leader elected: node_3", timeout=60):
             raise Exception("node_1 did not recognize node_3 as leader")

        # Wait for heartbeats to populate
        logging.info("Waiting for heartbeats to populate...")
        time.sleep(10)

        # 4. Kill a Worker (node_1)
        logging.info("Killing Worker node_1...")
        run_command(["docker", "kill", "node_1"])
        
        # 5. Verify Leader detects and restarts it
        if not wait_for_log("node_3", "Worker node_1 died. Reviving...", timeout=60):
            raise Exception("Leader did not detect/revive dead worker")
            
        # Wait for node_1 to come back
        logging.info("Waiting for node_1 to restart...")
        time.sleep(5) # Give it time to boot
        # Check if node_1 is running
        res = run_command(["docker", "inspect", "-f", "{{.State.Running}}", "node_1"])
        if "true" not in res.stdout:
             raise Exception("node_1 failed to restart")
        logging.info("node_1 restarted successfully.")

        # 6. Kill the Leader (node_3)
        logging.info("Killing Leader node_3...")
        run_command(["docker", "kill", "node_3"])
        
        # 7. Verify node_2 becomes new Leader
        logging.info("Waiting for node_2 to become new Leader...")
        if not wait_for_log("node_2", "I am the new LEADER!", timeout=120):
            raise Exception("node_2 failed to become new leader")
            
        logging.info("TEST PASSED!")

    except Exception as e:
        logging.error(f"TEST FAILED: {e}")
        logging.info("Dumping node_3 logs:")
        print(get_logs("node_3"))
        # sys.exit(1) # Don't exit yet, let cleanup happen? No, user wants to debug.
        # But for automated test, I should exit.
        sys.exit(1)
    finally:
        logging.info("Cleaning up...")
        # run_command(["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"], check=False)

if __name__ == "__main__":
    main()
