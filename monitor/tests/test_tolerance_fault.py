import subprocess
import time
import os
import sys
import threading
import logging
import argparse
import re

LOG_FILE = "logs.txt"
CONFIG = "config/config-test.ini"
COMPOSE_SCRIPT = "scripts/generar-compose.py"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, mode="w")],
)


def run_command(cmd, check=True):
    logging.info(f"Running command: {cmd}")
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if check and result.returncode != 0:
        logging.error(f"Command failed: {cmd}\nstdout: {result.stdout}\nstderr: {result.stderr}")
        raise RuntimeError(f"Command failed: {cmd}")
    return result


def tail_and_kill(pattern, threshold, container_name, stop_event):
    """
    Tail LOG_FILE; when pattern appears threshold times, docker kill container_name.
    """
    count = 0
    logging.info(f"Watcher: waiting for {threshold} occurrences of '{pattern}' to kill {container_name}")
    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, os.SEEK_END)
        while not stop_event.is_set():
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue
            if pattern in line:
                count += 1
                logging.info(f"Watcher: matched ({count}/{threshold}) -> {line.strip()}")
                if count >= threshold:
                    logging.info(f"Watcher: killing {container_name}")
                    try:
                        run_command(f"docker kill {container_name}", check=False)
                    except Exception as e:
                        logging.error(f"Watcher: kill error {e}")
                    break


def _services_from_compose():
    result = subprocess.run("docker compose config --services", shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list services: {result.stderr}")
    return [s.strip() for s in result.stdout.splitlines() if s.strip()]


def main():
    parser = argparse.ArgumentParser(description="Fault tolerance kill test")
    parser.add_argument(
        "--kill-container",
        default="filter_year_service",
        help="Container name to kill after threshold is reached",
    )
    parser.add_argument(
        "--kill-pattern",
        default="action: filter | type:year",
        help="Log pattern to count before killing (chunk-level match)",
    )
    parser.add_argument(
        "--kill-threshold",
        type=int,
        default=2,
        help="Number of pattern matches before killing container",
    )
    args = parser.parse_args()
    stop_event = threading.Event()
    compose_proc = None
    watcher_thread = None
    try:
        # Cleanup
        run_command("make clean-results", check=False)
        run_command("make down", check=False)
        run_command("docker rm -f rabbitmq", check=False)
        # Reset logs
        open(LOG_FILE, "w").close()

        # Generate compose with clients
        run_command(f"python3 {COMPOSE_SCRIPT} --config={CONFIG}")

        services = _services_from_compose()
        non_clients = [s for s in services if not s.startswith("client")]
        clients = [s for s in services if s.startswith("client")]

        # Start non-client services
        logging.info(f"Starting non-client services: {non_clients}")
        run_command(f"docker compose up -d --build {' '.join(non_clients)}")

        # Wait for a leader log before arming the killer to avoid pre-mature kills
        def latest_leader_from_logs():
            try:
                with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                leader_lines = [l for l in lines if "I am the new LEADER!" in l]
                if not leader_lines:
                    return None
                last_line = leader_lines[-1]
                m = re.match(r"^(monitor_[0-9]+)", last_line.strip())
                if m:
                    return m.group(1)
            except Exception:
                return None
            return None

        def wait_for_leader_log(timeout=60):
            logging.info(f"Waiting for leader log before enabling kill... timeout={timeout}s")
            start = time.time()
            while time.time() - start < timeout:
                leader = latest_leader_from_logs()
                if leader:
                    logging.info(f"Leader detected: {leader}")
                    return leader
                time.sleep(2)
            raise RuntimeError("Timeout waiting for leader log")

        wait_for_leader_log()

        # Start clients after leader is confirmed
        if clients:
            logging.info(f"Starting client services: {clients}")
            run_command(f"docker compose up -d {' '.join(clients)}")

        # Stream logs in background to logs.txt
        logging.info("Tailing compose logs...")
        compose_proc = subprocess.Popen(
            "docker compose logs -f", shell=True, stdout=open(LOG_FILE, "a"), stderr=subprocess.STDOUT
        )

        # Start watcher to kill after threshold chunk logs
        watcher_thread = threading.Thread(
            target=tail_and_kill,
            args=(args.kill_pattern, args.kill_threshold, args.kill_container, stop_event),
            daemon=True,
        )
        watcher_thread.start()

        # Wait for completion with log size guard
        while compose_proc.poll() is None:
            try:
                lines = sum(1 for _ in open(LOG_FILE, "r", encoding="utf-8", errors="ignore"))
                if lines > 100000:
                    logging.error(f"Log limit exceeded ({lines} lines). Stopping test...")
                    compose_proc.terminate()
                    raise RuntimeError("Logs exceeded limit")
            except FileNotFoundError:
                pass
            time.sleep(2)

        logging.info("Test completed. Check logs for pipeline result.")

    except Exception as e:
        logging.error(f"Test Failed: {e}")
        sys.exit(1)
    finally:
        stop_event.set()
        if compose_proc and compose_proc.poll() is None:
            compose_proc.terminate()
        # Dump full logs for inspection
        logging.info("Dumping full logs to full_logs.txt...")
        run_command("docker compose logs > full_logs.txt 2>&1", check=False)


if __name__ == "__main__":
    main()
