####################################################################
# IMPORTS #
####################################################################
import os
import sys
import time
import json
import boto3
import signal
import socket
import logging

from pathlib import Path
from kafka import OffsetAndMetadata
from collections import defaultdict

from shared.config.logging_config import setup_logging
from shared.config.load_config import load_config_from_directory

from consumer.config.settings import settings

from consumer.core.schema import MESSAGES_SCHEMA
from consumer.core.consumer import SafeKafkaConsumer
from consumer.core.writer import ConsumerS3ParquetWriter
from consumer.core.validate import validate_and_normalize_event

from prometheus_client import start_http_server, Counter

####################################################################
# Logging
####################################################################
logger = setup_logging(service_name="trstream.consumer", suppress_loggers={"kafka": logging.CRITICAL})
logger.info("Consumer service starting...")

####################################################################
# Prometheus Metrics
####################################################################
start_http_server(9100)
CONSUMER_ID = os.getenv("HOSTNAME", "unknown-consumer")
messages_consumed_total = Counter(
    "messages_consumed_total",
    "Total number of messages consumed from Kafka",
    ["consumer"]
)
consumer_errors_total = Counter(
    "consumer_errors_total",
    "Total number of errors encountered by the consumer",
    ["consumer"]
)

messages_counter = messages_consumed_total.labels(consumer=CONSUMER_ID)
errors_counter = consumer_errors_total.labels(consumer=CONSUMER_ID)

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
faker_topic = settings.faker_topic
stripe_topic = settings.stripe_topic
revolut_topic = settings.revolut_topic
bucket_name = settings.minio_ingestion_bucket

minio_endpoint = settings.minio_endpoint
access_key = settings.minio_access_key
secret_key = settings.minio_secret_key

# CONSTANTS
CONTAINER_ID = socket.gethostname()
RUN_ID = int(time.time_ns())
FILE_KEY = f'part-{CONTAINER_ID}-{RUN_ID}'

# Main function
def main():
    ####################################################################
    # Config reading
    ####################################################################   
    cfg = load_config_from_directory(Path("src"), "consumer.yaml")
    limit_msg = cfg['limit_msg']
    limit_time = cfg['limit_time']
    poll_ms = cfg['poll_ms']

    ####################################################################
    # Consumer instantiation
    ####################################################################
    logger.info("Consumer instantiation...")
    try:
        consumer = SafeKafkaConsumer(
            bootstrap_servers=broker,
            auto_offset_reset="earliest", #cfg['auto_offset_reset'],
            group_id=f"{cfg['group_id']}-reset",
            enable_auto_commit=False
        )
    except Exception as e:
        errors_counter.inc()
        logger.error("Kafka unavailable at startup. Exiting...")
        time.sleep(10) # Sleep to allow metrics to be scraped before shutdown
        sys.exit(1)
    consumer.subscribe(topics=[faker_topic, stripe_topic, revolut_topic])
    logger.info("Consumer ready, waiting for messages...")

    ####################################################################
    # S3 client and Parquet Writer
    ####################################################################
    s3 = boto3.client(
        's3', 
        endpoint_url=minio_endpoint, 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key
    )

    writer = ConsumerS3ParquetWriter(
        s3_client=s3,
        bucket=bucket_name,
        file_key=FILE_KEY,
        schema=MESSAGES_SCHEMA
    )

    #####################################################################
    # Transactions processing
    #####################################################################
    records = defaultdict(list)
    count = 0
    batch_ids = defaultdict(int)
    start_time = time.time()
    partition_offsets = {}
    global running

    while running:
        msg_pack = consumer.poll(timeout_ms=poll_ms)

        # Collect messages
        for tp, messages in msg_pack.items():
            for msg in messages:
                decoded = json.loads(msg.value.decode())
                logger.debug(f"Got new message! {decoded}")
                try:
                    record, iso_dt, hr = validate_and_normalize_event(decoded)
                except ValueError:
                    errors_counter.inc()
                    continue

                count += 1
                messages_counter.inc()
                records[(record['source'], iso_dt, hr)].append(record)
                partition_offsets[tp] = msg.offset + 1

        # Single flush condition
        if records and (count >= limit_msg or time.time() - start_time >= limit_time):
            logger.debug(f"Flushing {count} messages across {len(records)} partitions...")
            for (source, dt, hr), messages in records.items():
                bid = batch_ids[(source, dt, hr)] 
                writer.write(records=messages, source=source, dt=dt, hr=hr, batch_id=bid)
                batch_ids[(source, dt, hr)] += 1
            
            offsets = {
               tp: OffsetAndMetadata(offset=offset, metadata=None, leader_epoch=-1)
               for tp, offset in partition_offsets.items()
            }

            consumer.commit(offsets=offsets)
            partition_offsets.clear()
            records.clear()
            count = 0
            start_time = time.time()
            

    if records:
        logger.info("Flushing remaining records...")
        for (source, dt, hr), messages in records.items():
            bid = batch_ids[(source, dt, hr)] 
            writer.write(records=messages, source=source, dt=dt, hr=hr, batch_id=bid)

        offsets = {
            tp: OffsetAndMetadata(offset=offset, metadata=None, leader_epoch=-1)
            for tp, offset in partition_offsets.items()
        }

        consumer.commit(offsets=offsets)

    consumer.close()
    logger.info("Consumer finished! Exiting the container now...")


if __name__ == '__main__':
    main()
