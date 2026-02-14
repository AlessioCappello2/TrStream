import time
import logging
from typing import Optional, Iterable
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

from ..config.logging_config import setup_logging

logger = setup_logging(
    service_name="trstream.kafka.consumer",
    suppress_loggers={"kafka": logging.CRITICAL}
)


class KafkaUnavailableError(RuntimeError):
    """Raised when Kafka brokers are unavailable at startup."""
    pass


class BaseKafkaConsumer:
    """
    Base Kafka consumer with safe polling and graceful shutdown.
    """
    """
    Base Kafka consumer with safe polling and graceful shutdown.
    """

    def __init__(self, **kwargs):
        self._closed = False

        try:
            self._consumer = KafkaConsumer(**kwargs)
            logger.info("Kafka consumer initialized")
        except NoBrokersAvailable as e:
            logger.critical("No Kafka brokers available at startup")
            raise KafkaUnavailableError("Cannot connect to Kafka brokers") from e
        except Exception as e:
            logger.error(f"Kafka consumer initialization failed: {e}")
            raise

    # --------------------------
    # Core operations
    # --------------------------

    def subscribe(self, topics: Iterable[str]) -> None:
        if self._closed:
            return
        self._consumer.subscribe(topics)
        #logger.info(f"Subscribed to topics: {topics}")

    def poll(self, timeout_ms: int = 1000):
        if self._closed:
            return {}

        try:
            return self._consumer.poll(timeout_ms=timeout_ms)
        except KafkaError as e:
            logger.warning(f"Poll failed: {e}")
            return {}

    def commit(self, **kwargs) -> None:
        if self._closed:
            return
        try:
            self._consumer.commit(**kwargs)
        except KafkaError as e:
            logger.warning(f"Commit failed: {e}")

    # --------------------------
    # Lifecycle
    # --------------------------

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        try:
            logger.info("Closing Kafka consumer")
            self._consumer.close()
        except KafkaError as e:
            logger.warning(f"Close failed: {e}")