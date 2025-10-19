import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

topic = os.getenv('KAFKA_TOPIC')
broker = os.getenv('KAFKA_BROKER')

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=broker, linger_ms=1)
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 2 seconds...", flush=True)
        time.sleep(2)

print("Producer ready to send messages...", flush=True)

for i in range(100):
    print(f"Sending message no. {i}", flush=True)
    producer.send(topic, value=f'Messaggio no. {i}'.encode())

producer.close()
print("Producer finished! Exiting the container now...", flush=True)