####################################################################
# IMPORTS #
####################################################################
import sys
import time
import json
import boto3
import signal
import socket

from kafka import OffsetAndMetadata
from collections import defaultdict

from consumer.config.settings import settings
from consumer.config.load_config import load_config
from consumer.config.logging_config import setup_logging

from consumer.core.writer import ParquetS3Writer
from consumer.core.schema import MESSAGES_SCHEMA
from consumer.core.validate import validate_and_normalize_event
from consumer.core.consumer import SafeKafkaConsumer, KafkaUnavailable

####################################################################
# Logging
####################################################################
logger = setup_logging()
logger.info("Consumer service starting...")

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
stripe_topic = settings.stripe_topic
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
    cfg = load_config()['consumer']
    limit_msg = cfg['limit_msg']
    limit_time = cfg['limit_time']
    poll_ms = cfg['poll_ms']
    
    ####################################################################
    # Consumer instantiation
    ####################################################################
    try:
        logger.info("Consumer instantiation...")
        consumer = SafeKafkaConsumer(
            bootstrap_servers=broker,
            auto_offset_reset=cfg['auto_offset_reset'],
            group_id=cfg['group_id'],
            enable_auto_commit=False
        )
    except KafkaUnavailable:
        logger.error("Kafka unavailable at startup. Exiting...")
        sys.exit(1)
    consumer.subscribe(topics=[topic, stripe_topic])
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

    writer = ParquetS3Writer(
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
                try:
                    record, iso_dt, hr = validate_and_normalize_event(decoded)
                except ValueError:
                    continue

                count += 1
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
