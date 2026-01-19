import logging

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger("trstream.kafka")

class KafkaUnavailable(RuntimeError):
    pass

class SafeKafkaConsumer:

    def __init__(self, **kwargs):
        try:
            self._consumer = KafkaConsumer(**kwargs)
        except NoBrokersAvailable:
            logger.critical("No Kafka brokers available at startup.")
            raise KafkaUnavailable()
        
        self._closed = False

    def subscribe(self, topics):
        if self._closed:
            return
        self._consumer.subscribe(topics)

    def poll(self, timeout_ms):
        if self._closed:
            return {}
        try:
            return self._consumer.poll(timeout_ms=timeout_ms)
        except Exception as e:
            logger.warning(f"Poll failed: {str(e)}")
            return {}

    def commit(self, **kwargs):
        if self._closed:
            return
        self._consumer.commit(**kwargs)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._consumer.close()
