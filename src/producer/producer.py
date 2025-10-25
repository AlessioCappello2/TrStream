####################################################################
# IMPORTS #
####################################################################
import os
import json
import time
import signal
import socket
import random
import logging

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
topic = os.getenv('KAFKA_TOPIC')
broker = os.getenv('KAFKA_BROKER')
container_name = socket.gethostname()


if __name__ == '__main__':
    ####################################################################
    # Producer instantiation
    ####################################################################
    print("Producer instantiation...")
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
            print(f"Sending a new message!", flush=True)
            transaction = generator.generate_transaction()
            producer.send(topic, key=f"{transaction['transaction_type']}_{transaction['user_id']}".encode(), value=transaction)
            time.sleep(int(random.uniform(1, 10)))
    except TerminationException:
        print("Shutting down producer...", flush=True)
        producer.close()
        print("Producer finished! Exiting the container now...", flush=True)