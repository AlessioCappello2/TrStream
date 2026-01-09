####################################################################
# IMPORTS #
####################################################################
import sys
import time
import json
import boto3
import signal
import socket

from consumer.config.settings import settings
from consumer.config.load_config import load_config
from consumer.config.logging_config import setup_logging

from consumer.core.writer import ParquetS3Writer
from consumer.core.schema import TRANSACTION_SCHEMA
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
bucket_name = settings.bucket_name

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
            group_id=cfg['group_id']
        )
    except KafkaUnavailable:
        logger.error("Kafka unavailable at startup. Exiting...")
        sys.exit(1)
    consumer.subscribe(topics=[topic])
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
        schema=TRANSACTION_SCHEMA
    )

    #####################################################################
    # Transactions processing
    #####################################################################
    records = []
    batch_id = 0
    start_time = time.time()
    global running

    while running:
        msg_pack = consumer.poll(timeout_ms=poll_ms)

        # Collect messages
        for _, messages in msg_pack.items():
            for msg in messages:
                records.append(json.loads(msg.value.decode()))

        # Single flush condition
        if records and (len(records) >= limit_msg or time.time() - start_time >= limit_time):
            writer.write(records=records, batch_id=batch_id)
            consumer.commit()
            records.clear()
            batch_id += 1
            start_time = time.time()

    if records:
        logger.info("Flushing remaining records...")
        writer.write(records=records, batch_id=batch_id)
        consumer.commit()

    consumer.close()
    logger.info("Consumer finished! Exiting the container now...")


if __name__ == '__main__':
    main()
