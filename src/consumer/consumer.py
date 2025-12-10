####################################################################
# IMPORTS #
####################################################################
import io 
import os
import time
import json
import boto3
import signal
import socket
import pyarrow as pa
import pyarrow.parquet as pq

from kafka import KafkaConsumer
from dotenv import load_dotenv

####################################################################
# Handle SIGTERM as an Exception
####################################################################
class TerminationException(Exception):
    pass

def handle_termination(signum, frame):
    raise TerminationException()

signal.signal(signal.SIGTERM, handle_termination)

####################################################################
# Env variables
####################################################################
load_dotenv()

broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
topic = os.getenv('KAFKA_TOPIC', 'transactions-trial')
bucket_name = os.getenv('BUCKET_NAME' , 'raw-data')
limit_msg = os.getenv('LIMIT_MSG', 100)
limit_time = os.getenv('LIMIT_TIME', 120)

minio_endpoint = os.getenv('MINIO_ENDPOINT')
access_key = os.getenv('MINIO_ACCESS_KEY')
secret_key = os.getenv('MINIO_SECRET_KEY')

# CONSTANTS
CONTAINER_ID = socket.gethostname()
FILE_KEY = f'test-{CONTAINER_ID}'

####################################################################
# Helper function for uploading a transaction batch
####################################################################
def upload_batch(records, schema, b):
    table = pa.Table.from_pylist(records, schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    s3.put_object(Bucket=bucket_name, Key=f"{FILE_KEY}_{b}.parquet", Body=buffer.getvalue())


if __name__ == '__main__':
    ####################################################################
    # Consumer instantiation
    ####################################################################
    print("Consumer instantiation...", flush=True)
    consumer = KafkaConsumer(bootstrap_servers=broker, auto_offset_reset='latest', group_id=f'transaction-consumers')
    consumer.subscribe(topics=[topic])
    print("Consumer ready, waiting for messages...", flush=True)

    ####################################################################
    # S3 client and schema to enforce for Parquet files
    ####################################################################
    s3 = boto3.client('s3', endpoint_url=minio_endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    records = []
    schema = pa.schema([
        ('transaction_id', pa.string()),
        ('user_id', pa.string()),
        ('card_number', pa.string()),
        ('amount', pa.float32()),
        ('currency', pa.string()),
        ('timestamp', pa.string()),
        ('transaction_type', pa.string()),
        ('status', pa.string())
    ])

    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        print("Bucket already exists. Skipping bucket creation...")

    #####################################################################
    # Transactions processing
    #####################################################################
    try:
        start = time.time()
        processed = b = 0
        
        for message in consumer:
            m = json.loads(message.value.decode())
            print(f"Received message: {m}", flush=True)
            records.append(m)
            processed += 1

            if processed >= limit_msg or time.time() - start >= limit_time:
                if not processed:
                    start = time.time()
                    continue 

                print(f"Uploading batch no. {b} to S3...", flush=True)
                upload_batch(records=records, schema=schema, b=b)
                records, start, processed, b = [], time.time(), 0, b+1
                consumer.commit()
                print(f"Batch no. {b} successfully uploaded to S3.")

    except TerminationException:
        print("Shutting down consumer and writing any left data to S3...", flush=True)
    finally:
        if records:
            upload_batch(records=records, schema=schema, b=b)
            consumer.commit()
            print("Uploaded the latest messages. Exiting container now...", flush=True)
        consumer.close()