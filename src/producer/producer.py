####################################################################
# IMPORTS #
####################################################################
import os
import json
import time
import signal
import random

from dotenv import load_dotenv
from kafka import KafkaProducer
from __generate import Generator

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

if __name__ == '__main__':
    ####################################################################
    # Producer instantiation
    ####################################################################
    print("Producer instantiation...", flush=True)
    producer = KafkaProducer(bootstrap_servers=broker, 
                                linger_ms=1, 
                                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print("Producer ready to send messages...", flush=True)

    #####################################################################
    # Transactions generation
    #####################################################################
    generator = Generator()
    try:
        while True:
            print(f"New transaction received!", flush=True)
            transaction = generator.generate_transaction()
            producer.send(topic, key=f"{transaction['transaction_type']}_{transaction['user_id']}".encode(), value=transaction)
            time.sleep(int(random.uniform(1, 10)))  # this line can be commented to produce high volumes of transactions
    except TerminationException:
        print("Shutting down producer...", flush=True)
        producer.close()
        print("Producer finished! Exiting the container now...", flush=True)