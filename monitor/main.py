import logging
import os
import sys
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common.monitor import MonitorNode

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

def main():
    logging.info("Starting Monitor Node...")
    monitor = MonitorNode()
    monitor.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Monitor stopping...")

if __name__ == "__main__":
    main()
