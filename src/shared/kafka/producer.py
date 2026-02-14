import time
import json
import logging
from typing import Optional, Callable, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from ..config.logging_config import setup_logging

logger = setup_logging(service_name="trstream.kafka.producer", suppress_loggers={"kafka": logging.CRITICAL})

class KafkaUnavailableError(RuntimeError):
    """Raised when Kafka brokers are unavailable at startup."""
    pass

class BaseKafkaProducer:
    """
    Base Kafka producer with retry logic, batch logging, and graceful shutdown.
    
    Args:
        bootstrap_servers: Kafka broker addresses
        topic: Default topic to send to
        batch_size_log: Log progress every N messages
        max_send_retries: Maximum retry attempts for failed sends
        value_serializer: Function to serialize values (default: JSON)
        **kwargs: Additional KafkaProducer configuration
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        batch_size_log: int = 100,
        max_send_retries: int = 5,
        value_serializer: Optional[Callable] = None,
        **kwargs
    ):
        self.topic = topic
        self.batch_size_log = batch_size_log
        self.max_send_retries = max_send_retries
        self.counter = 0
        self._shutdown = False
        
        # Default JSON serializer
        if value_serializer is None:
            value_serializer = lambda v: json.dumps(v).encode("utf-8")
        
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=value_serializer,
                linger_ms=kwargs.pop('linger_ms', 1),
                **kwargs
            )
            logger.info(f"Kafka producer initialized for topic '{topic}'")
        except NoBrokersAvailable as e:
            logger.critical("No Kafka brokers available at startup")
            raise KafkaUnavailableError("Cannot connect to Kafka brokers") from e
        except Exception as e:
            logger.error(f"Kafka producer initialization failed: {e}")
            raise
    

    def send_with_retry(
        self,
        key: str,
        value: dict,
        topic: Optional[str] = None,
        timeout: int = 5
    ) -> None:
        """
        Send a message to Kafka with retry logic and exponential backoff.
        
        Args:
            key: Message key
            value: Message value (will be serialized)
            topic: Topic to send to (default: self.topic)
            timeout: Timeout for send confirmation
            
        Raises:
            RuntimeError: If producer is shutdown
            KafkaError: If max retries exceeded
        """
        if self._shutdown:
            raise RuntimeError("Cannot send: producer is shutdown")
        
        topic = topic or self.topic
        retry = 0
        
        while retry <= self.max_send_retries:
            try:
                future = self._producer.send(
                    topic=topic,
                    key=key.encode(),
                    value=value
                )
                
                # Wait for confirmation
                future.get(timeout=timeout)
                
                # Update counter and log progress
                self.counter += 1
                if self.counter % self.batch_size_log == 0:
                    self._producer.flush()
                    logger.info(f"Events sent: {self.counter}")
                
                return  # Success
                
            except KafkaError as e:
                retry += 1
                
                if retry > self.max_send_retries:
                    logger.error(
                        f"Max retries ({self.max_send_retries}) exceeded for key '{key}'"
                    )
                    raise
                
                # Exponential backoff with upper bound
                backoff = min(2 ** retry, 30)
                logger.warning(
                    f"Send failed (attempt {retry}/{self.max_send_retries}). "
                    f"Retrying in {backoff}s..."
                )
                time.sleep(backoff)
    

    def flush(self, timeout: Optional[int] = None) -> None:
        """Flush pending messages."""
        if self._shutdown:
            return
        
        try:
            self._producer.flush(timeout=timeout)
            logger.debug("Producer flushed")
        except KafkaError as e:
            logger.warning(f"Flush failed: {e}")
    
    
    def close(self, timeout: Optional[int] = None) -> None:
        """Close the producer gracefully."""
        if self._shutdown:
            return
        
        self._shutdown = True
        try:
            logger.info(f"Closing producer. Total events sent: {self.counter}")
            self._producer.close(timeout=timeout)
        except KafkaError as e:
            logger.warning(f"Close failed: {e}")