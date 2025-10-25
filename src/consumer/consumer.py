####################################################################
# IMPORTS #
####################################################################
import io 
import os
import time
import boto3
import json 
import signal
import socket
import logging
import pyarrow as pa
import pyarrow.parquet as pq

from kafka import KafkaConsumer

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
topic = os.getenv('KAFKA_TOPIC')
broker = os.getenv('KAFKA_BROKER')
limit_msg = os.getenv('LIMIT_MSG', 100)
limit_time = os.getenv('LIMIT_TIME', 120)
container_id = socket.gethostname()
bucket_name = 'raw-data'
file_key = f'test-{container_id}'

if __name__ == '__main__':
    ####################################################################
    # Consumer instantiation
    ####################################################################

    print("Consumer instantiation...", flush=True)
    consumer = KafkaConsumer(bootstrap_servers=broker, auto_offset_reset='latest', group_id=f'transaction-consumers')
    consumer.subscribe(topics=[topic])

    print("Consumer started, waiting for messages...", flush=True)
    s3 = boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'], aws_access_key_id=os.environ['MINIO_ACCESS_KEY'], aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
    records = []
    schema = []# pa.schema([(), ()...])

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
                print(f"Uploading the batch no. {b} to S3...", flush=True)

                table = pa.Table.from_pylist(records)
                buffer = io.BytesIO()
                pq.write_table(table, buffer)

                s3.put_object(Bucket=bucket_name, Key=f"{file_key}_{b}.parquet", Body=buffer.getvalue())
                records, start, processed, b = [], time.time(), 0, b+1
                consumer.commit()

    except TerminationException:
        print("Shutting down consumer and writing data to S3...", flush=True)
    finally:
        if records:
            table = pa.Table.from_pylist(records)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            s3.put_object(Bucket=bucket_name, Key=f"{file_key}_{b}.parquet", Body=buffer.read())
            consumer.commit()
            print("Uploaded the latest messages. Exiting container now...", flush=True)
        consumer.close()