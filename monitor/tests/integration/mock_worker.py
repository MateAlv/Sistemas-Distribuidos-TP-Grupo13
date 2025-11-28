import time
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.heartbeat_sender import HeartbeatSender

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

def main():
    logging.info("Starting Mock Worker...")
    sender = HeartbeatSender()
    sender.start()
    
    try:
        while True:
            sender.pulse()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Mock Worker stopping...")

if __name__ == "__main__":
    main()
