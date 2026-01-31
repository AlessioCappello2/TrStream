from upstash_redis import Redis
from .core.kafka_producer import RevolutKafkaProducer
from .config.settings import settings
from .core.event_processor import process_event

import json
import time
import signal

####################################################################
# Handle SIGTERM/SIGINT Exceptions
####################################################################
running = True 

def handle_termination(signum, frame):
    global running
    print("Shutdown signal received!", flush=True)
    running = False

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_termination)

redis = Redis(
    url=settings.upstash_redis_rest_url,
    token=settings.upstash_redis_rest_token
)

producer = RevolutKafkaProducer()

def process_queue():
    global running 
    while running:
        try: 
            item = redis.lindex("revolut-queue", -1) # oldest

            if not item:
                print("Queue empty, waiting...", flush=True)
                time.sleep(10)
                continue 
                
            print(isinstance(item, str), flush=True)
            event = json.loads(item) if isinstance(item, str) else item
            print(event, flush=True)

            processed_event = process_event(event)

            producer.send_event(processed_event)
            producer.flush()

            print("Item sent to Kafka successfully!", flush=True)

            redis.rpop("revolut-queue")

        except Exception as e:
            print(f"Error: {e}", flush=True)
            print("Item remains in queue, will retry...")
            time.sleep(10)


if __name__ == "__main__":
    print("Starting worker...")
    process_queue()