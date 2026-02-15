# import time
# import json

# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from worker.config.settings import settings
# from shared.config.logging_config import setup_logging

# logger = setup_logging()

# class RevolutKafkaProducer:
#     def __init__(self):
#         try:
#             self.producer = KafkaProducer(
#                 bootstrap_servers=settings.kafka_broker,
#                 linger_ms=1,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8")
#             )
#             self.topic = settings.kafka_topic
            
#             self.counter = 0
#             self.batch_size_log = settings.batch_size_log
#             self.max_retries = settings.max_send_retries

#         except Exception as e:
#             logger.error(f"Kafka producer initialization failed: {e}")
#             raise e


#     def send_event(self, event: dict):
#         logger.debug(event)

#         obj = event['payload']['data']
#         retry = 0
#         key = f"revolut_transaction_{obj['id']}" 

#         while retry < self.max_retries:
#             try:
#                 future = self.producer.send(
#                     topic=self.topic,
#                     key=key.encode(),
#                     value=event
#                 )

#                 future.get(timeout=5)
#                 self.counter += 1

#                 if self.counter % self.batch_size_log == 0:
#                     self.producer.flush()
#                     logger.info(f"No. of events sent: {self.counter}")

#                 break
#             except KafkaError as e:
#                 # exponential backoff with upper bound
#                 retry += 1

#                 if retry > self.max_retries:
#                     logger.error("Max retries exceeded! Failed to send event.")
#                     raise e
                
#                 logger.warning(f"Sending to Kafka failed. Retry no. {retry}")
#                 time.sleep(min(2**retry, 30))

    
#     def flush(self):
#         self.producer.flush()
        
from worker.config.settings import settings
from shared.kafka.producer import BaseKafkaProducer

class RevolutKafkaProducer(BaseKafkaProducer):
    """
    Kafka producer for Revolut transaction events.
    
    Extends BaseKafkaProducer with Revolut-specific event handling
    and key generation logic.
    """
    
    def __init__(self, batch_size_log: int = 100, max_send_retries: int = 5):
        """Initialize Revolut Kafka producer with settings."""
        super().__init__(
            bootstrap_servers=settings.kafka_broker,
            topic=settings.revolut_topic,
            batch_size_log=batch_size_log,
            max_send_retries=max_send_retries,
            linger_ms=1
        )
    
    def send_event(self, event: dict) -> None:
        """
        Send a Revolut transaction event to Kafka.
        
        Generates a partition key based on the transaction ID to ensure
        events for the same transaction go to the same partition.
        
        Args:
            event: Enveloped Revolut event with structure:
                   {"source": "revolut", "received_at": ..., "payload": {...}}
        
        Raises:
            KafkaError: If send fails after all retries
        """
        
        # Extract transaction data from event payload
        transaction_data = event['payload']['data']
        
        # Generate key based on transaction ID
        # This ensures events for the same transaction maintain order
        transaction_id = transaction_data['id']
        key = f"revolut_transaction_{transaction_id}"
        
        # Send using base class retry logic
        self.send_with_retry(key=key, value=event)