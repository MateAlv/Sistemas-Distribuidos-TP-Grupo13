import subprocess
import yaml
import time
import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs.txt", mode='a')
    ]
)

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
        # 0. Cleanup previous runs
        logging.info("Cleaning up previous runs...")
        run_command("make hard-down", check=False)
        run_command("docker rm -f rabbitmq", check=False)

        # 1. Generate docker-compose.yaml
        logging.info("Generating docker-compose.yaml...")
        run_command("make compose CONFIG=config/config-test.ini")
        
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
            
        # 5. Scenario: Service Revival
        # Pick a victim (e.g., first aggregator)
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
