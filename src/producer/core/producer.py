import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logger = logging.getLogger("trstream.kafka")

class KafkaUnavailable(RuntimeError):
    pass

class SafeKafkaProducer:
    def __init__(self, **kwargs):
        try:
            self._producer = KafkaProducer(**kwargs)
        except NoBrokersAvailable:
            logger.critical("No Kafka brokers available at startup.")
            raise KafkaUnavailable()
        
        self._shutdown = False

    def send(self, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError("Send called after producer shutdown.")
        return self._producer.send(*args, **kwargs)

    def flush(self, timeout=None):
        if self._shutdown:
            return
        
        try:
            self._producer.flush(timeout=timeout)
        except KafkaError:
            logger.warning("Flush failed during shutdown.")

    def close(self, timeout=None):
        if self._shutdown:
            return
        
        self._shutdown = True
        try:
            self._producer.close(timeout=timeout)
        except KafkaError:
            logger.warning("Close failed during shutdown.")
