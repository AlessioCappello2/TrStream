####################################################################
# IMPORTS #
####################################################################
import io 
import os
import time
import boto3
import signal
import logging

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
bucket_name = 'mybucket'
file_key = 'test.txt'

####################################################################
# Consumer instantiation
####################################################################

print("Consumer instantiation...", flush=True)
consumer = KafkaConsumer(bootstrap_servers=broker, auto_offset_reset='earliest', group_id='transaction-consumers-test2')
consumer.subscribe(topics=[topic])

print("Consumer started, waiting for messages...", flush=True)
s3 = boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'], aws_access_key_id=os.environ['MINIO_ACCESS_KEY'], aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
buffer = io.StringIO()

#####################################################################
# Transactions processing
#####################################################################
try:
    for message in consumer:
        m = message.value.decode()
        print(f"Received message {m}", flush=True)
        buffer.write(m + "\n")
        consumer.commit()
except TerminationException:
    print("Shutting down consumer and writing data to S3...", flush=True)
finally:
    if buffer.tell() > 0:
        buffer.seek(0)
        try:
            s3.create_bucket(Bucket=bucket_name)
        except Exception:
            print("Bucket already exists. Skipping bucket creation...")
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=buffer.read())
        buffer.close()
    consumer.close()