# import logging

# from kafka import KafkaProducer
# from kafka.errors import KafkaError, NoBrokersAvailable

# logger = logging.getLogger("trstream.kafka")

# class KafkaUnavailable(RuntimeError):
#     pass

# class SafeKafkaProducer:
#     def __init__(self, **kwargs):
#         try:
#             self._producer = KafkaProducer(**kwargs)
#         except NoBrokersAvailable:
#             logger.critical("No Kafka brokers available at startup.")
#             raise KafkaUnavailable()
        
#         self._shutdown = False

#     def send(self, *args, **kwargs):
#         if self._shutdown:
#             raise RuntimeError("Send called after producer shutdown.")
#         return self._producer.send(*args, **kwargs)

#     def flush(self, timeout=None):
#         if self._shutdown:
#             return
        
#         try:
#             self._producer.flush(timeout=timeout)
#         except KafkaError:
#             logger.warning("Flush failed during shutdown.")

#     def close(self, timeout=None):
#         if self._shutdown:
#             return
        
#         self._shutdown = True
#         try:
#             self._producer.close(timeout=timeout)
#         except KafkaError:
#             logger.warning("Close failed during shutdown.")


from shared.kafka.producer import BaseKafkaProducer
from producer.config.settings import settings

class FakerKafkaProducer(BaseKafkaProducer):
    """Producer for synthetic transaction data."""
    
    def __init__(self, batch_size_log: int = 100, max_send_retries: int = 5, **kwargs):
        super().__init__(
            bootstrap_servers=settings.kafka_broker,
            topic=settings.faker_topic,
            batch_size_log=batch_size_log,
            max_send_retries=max_send_retries,
            **kwargs
        )
    
    def send_transaction(self, transaction: dict) -> None:
        """Send a transaction event."""
        key = f"{transaction['payload']['user_id']}"
        self.send_with_retry(key=key, value=transaction)
