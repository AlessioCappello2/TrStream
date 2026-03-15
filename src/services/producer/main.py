####################################################################
# IMPORTS #
####################################################################
import os
import sys
import time
import signal
import random
import logging
from pathlib import Path

from shared.config.load_config import load_config_from_directory
from shared.config.logging_config import setup_logging

from producer.config.settings import settings
from producer.core.producer import FakerKafkaProducer
from producer.core.transaction_generator import TransactionGenerator

from prometheus_client import start_http_server, Counter

####################################################################
# Logging
####################################################################
logger = setup_logging(service_name="trstream.producer", suppress_loggers={"kafka": logging.CRITICAL})
logger.info("Producer service starting...")

####################################################################
# Prometheus Metrics
####################################################################
start_http_server(9100)
PRODUCER_ID = os.getenv("HOSTNAME", "unknown-producer")
transactions_sent_total = Counter(
    "transactions_sent_total",
    "Total number of transactions sent to Kafka",
    ["producer"]
)
producer_errors_total = Counter(
    "producer_errors_total",
    "Total number of errors encountered by the producer",
    ["producer"]
)

transactions_counter = transactions_sent_total.labels(producer=PRODUCER_ID)
errors_counter = producer_errors_total.labels(producer=PRODUCER_ID)

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
topic = settings.faker_topic

def main():
    ####################################################################
    # Config reading
    ####################################################################   
    cfg = load_config_from_directory(Path("src"), "producer.yaml")
    min_sleep = cfg['rate']['min_sleep_sec']
    max_sleep = cfg['rate']['max_sleep_sec']
    batch_size_log = cfg.get('batch_size_log', 100)
    max_send_retries = cfg.get('max_send_retries', 5)

    ####################################################################
    # Producer instantiation
    ####################################################################
    logger.info("Producer instantiation...")
    try:
        producer = FakerKafkaProducer(
            batch_size_log=batch_size_log,
            max_send_retries=max_send_retries,
            linger_ms=1
        )
    except Exception as e:
        errors_counter.inc()
        logger.error(f"Kafka unavailable at startup. Exiting: {e}...")
        time.sleep(10) # Sleep to allow metrics to be scraped before shutdown
        sys.exit(1)
    logger.info("Producer ready to send messages.")

    #####################################################################
    # Transactions generation and sending
    #####################################################################
    generator = TransactionGenerator(cfg=cfg)
    counter = 0
    global running

    try:
        while running:
            transaction = generator.generate_transaction()

            producer.send_transaction(
                transaction=transaction
            )

            counter += 1
            transactions_counter.inc()
            time.sleep(random.uniform(min_sleep, max_sleep))

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        errors_counter.inc()

    finally:
        logger.info(f"Total transactions sent: {counter}. Shutting down producer...")
        producer.flush()
        producer.close(timeout=5)
        logger.info("Producer finished! Exiting the container now...")


if __name__ == '__main__':
    main()
