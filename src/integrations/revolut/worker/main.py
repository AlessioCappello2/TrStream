import json
import time
import signal
import socket

from upstash_redis import Redis
from .config.settings import settings
from .core.event_processor import process_event
from .config.logging_config import setup_logging
from .core.kafka_producer import RevolutKafkaProducer

####################################################################
# Logging
####################################################################
logger = setup_logging()

####################################################################
# Handle SIGTERM/SIGINT Exceptions
####################################################################
running = True 

def handle_termination(signum, frame):
    global running
    logger.warning("Shutdown signal received!")
    running = False

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_termination)

####################################################################
# Constants
####################################################################
WORKER_ID = socket.gethostname()
PROCESSING_QUEUE = f"revolut-processing:{WORKER_ID}"
MAIN_QUEUE = "revolut:queue"

# Redis Client
redis = Redis(
    url=settings.upstash_redis_rest_url,
    token=settings.upstash_redis_rest_token
)

# Kafka Producer
producer = RevolutKafkaProducer()

def recover_processing():
    """
    Move items from processing queue back to main queue on startup.
    """
    logger.info(f"[{WORKER_ID}] Checking for unprocessed items...")
    count = 0

    while True:
        item = redis.rpop(PROCESSING_QUEUE)
        if not item:
            break
        redis.lpush(MAIN_QUEUE, item)
        count += 1
    
    if count > 0:
        logger.info(f"[{WORKER_ID}] Recovered {count} unprocessed items")


# Main funciton: process events queue
def main():
    global running 
    counter = 0

    # In case of crash and new bootstrap, recover possible events in processing queue
    recover_processing()

    logger.info(f"[{WORKER_ID}] Starting to process queue...")

    try: 
        while running:
            # Atomically move item from main queue to processing queue
            item = redis.rpoplpush(MAIN_QUEUE, PROCESSING_QUEUE)

            if not item:
                logger.info(f"[{WORKER_ID}] Queue empty, waiting...")
                time.sleep(10)
                continue 
                
            event = json.loads(item)
            logger.info(f"[{WORKER_ID}] Processing event: {event}")

            processed_event = process_event(event)
            producer.send_event(processed_event)
            
            counter += 1
            if counter % settings.batch_size_log == 0:
                producer.flush()
                logger.info(f"No. of transactions sent: {counter}")

            logger.info(f"[{WORKER_ID}] Sent to Kafka successfully!")

            redis.lrem(PROCESSING_QUEUE, 1, item)

    except Exception as e:
        logger.error(f"[{WORKER_ID}] Fatal error: {e}")

    finally:
        logger.info(f"[{WORKER_ID}] Flushing remaining Kafka messages...")
        producer.flush()

    logger.info(f"[{WORKER_ID}] Shutting down worker...")


if __name__ == "__main__":
    main()