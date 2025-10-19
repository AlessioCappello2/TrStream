import io 
import os
import time
import boto3
import signal

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

class TerminationException(Exception):
    pass

def handle_termination(signum, frame):
    raise TerminationException()

signal.signal(signal.SIGTERM, handle_termination)

topic = os.getenv('KAFKA_TOPIC')
broker = os.getenv('KAFKA_BROKER')
bucket_name = 'mybucket'
file_key = 'test.txt'

while True:
    try:
        print("Trying to create consumer...", flush=True)
        consumer = KafkaConsumer(bootstrap_servers=broker, auto_offset_reset='earliest', enable_auto_commit=True, group_id='transaction-consumers-test2')
        consumer.subscribe(topics=[topic])
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 2 seconds...", flush=True)
        time.sleep(2)

print("Consumer started, waiting for messages...", flush=True)
s3 = boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'], aws_access_key_id=os.environ['MINIO_ACCESS_KEY'], aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
buffer = io.StringIO()

try:
    for message in consumer:
        m = message.value.decode()
        print(f"Received message {m}", flush=True)
        buffer.write(m + "\n")
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