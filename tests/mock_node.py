import sys
import os
import time
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.monitor import Monitor

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

def main():
    logging.info("Starting Mock Node...")
    monitor = Monitor()
    monitor.start()
    
    logging.info("Mock Node running. Press Ctrl+C to exit.")
    while True:
        time.sleep(1)
        monitor.pulse()

if __name__ == "__main__":
    main()
