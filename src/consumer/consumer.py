import logging

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger("trstream.consumer.kafka")

class KafkaUnavailable(Exception):
    pass

class SafeKafkaConsumer:

    def __init__(self, **kwargs):
        try:
            self._consumer = KafkaConsumer(**kwargs)
        except NoBrokersAvailable as e:
            logger.error("Kafka unavailable during consumer startup")
            raise KafkaUnavailable() from e

    def subscribe(self, topics):
        self._consumer.subscribe(topics)

    def poll(self, timeout_ms):
        return self._consumer.poll(timeout_ms=timeout_ms)

    def commit(self):
        self._consumer.commit()

    def close(self):
        self._consumer.close()
