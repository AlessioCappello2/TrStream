####################################################################
# IMPORTS #
####################################################################
import sys
import json
import time
import signal
import random

from kafka.errors import KafkaError

from producer.config.settings import settings
from producer.config.load_config import load_config
from producer.config.logging_config import setup_logging

from producer.core.transaction_generator import TransactionGenerator
from producer.core.producer import SafeKafkaProducer, KafkaUnavailable

####################################################################
# Logging
####################################################################
logger = setup_logging()
logger.info("Producer service starting...")

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
# Env variables
####################################################################
broker = settings.kafka_broker
topic = settings.kafka_topic
batch_size_log = settings.batch_size_log
max_retries = settings.max_retries

# Main function
def main():
    ####################################################################
    # Config reading
    ####################################################################   
    cfg = load_config()
    min_sleep = cfg['rate']['min_sleep_sec']
    max_sleep = cfg['rate']['max_sleep_sec']

    ####################################################################
    # Producer instantiation
    ####################################################################
    logger.info("Producer instantiation...")
    try:
        producer = SafeKafkaProducer(
            bootstrap_servers=broker, 
            linger_ms=1, 
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except KafkaUnavailable:
        logger.error("Kafka unavailable at startup. Exiting...")
        sys.exit(1)
    logger.info("Producer ready to send messages.")

    #####################################################################
    # Transactions generation and sending
    #####################################################################
    generator = TransactionGenerator(cfg=cfg)
    counter = retry = 0
    transaction = None
    global running

    while running:
        if retry == 0:
            transaction = generator.generate_transaction()

        try: 
            future = producer.send(
                topic, 
                key=f"{transaction['transaction_type']}_{transaction['user_id']}".encode(), 
                value=transaction
            )
            
            future.get(timeout=5)

            retry = 0
            counter += 1
            logger.debug("New transaction sent to Kafka broker.")
            
            if counter % batch_size_log == 0:
                producer.flush()
                logger.info(f"No. of transactions sent: {counter}")

            if running:  # avoid to wait in case of enforced shutdown since the start of the current iteration
                time.sleep(random.uniform(min_sleep, max_sleep))

        except KafkaError as e:
            # exponential backoff with upper bound
            retry += 1

            if retry > max_retries:
                logger.error("Max retries exceeded! Stopping producer.")
                running = False 
                break
            
            logger.warning(f"Sending to Kafka failed. Retry no. {retry}")
            time.sleep(min(2**retry, 30))

    logger.info(f"Total transactions sent: {counter}. Shutting down producer...")
    producer.flush()
    producer.close(timeout=5)
    logger.info("Producer finished! Exiting the container now...")


if __name__ == '__main__':
    main()
