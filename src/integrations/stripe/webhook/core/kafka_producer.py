# import time
# import json
# import logging 

# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# from ..config.settings import settings

# logger = logging.getLogger("trstream.stripe.webhook")

# class StripeKafkaProducer:
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

#         obj = event['payload']['data']['object']
#         retry = 0
#         key = f"stripe_payment_intent_{obj['payment_intent']}" if obj['object'] == "charge" else f"stripe_payment_intent_{obj['id']}" 

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
        
from shared.kafka.producer import BaseKafkaProducer
from webhook.config.settings import settings 

class StripeKafkaProducer(BaseKafkaProducer):
    """Kafka Producer for Stripe webhook events."""

    def __init__(self, batch_size_log: int = 100, max_send_retries: int = 5, **kwargs):
        super().__init__(
            bootstrap_servers=settings.kafka_broker,
            topic=settings.stripe_topic,
            batch_size_log=batch_size_log,
            max_send_retries=max_send_retries,
            **kwargs
        )

    
    def send_event(self, event: dict):
        """Send a Stripe webhook event to Kafka."""
        obj = event['payload']['data']['object']
        key = f"stripe_payment_intent_{obj['payment_intent']}" if obj['object'] == "charge" \
            else f"stripe_payment_intent_{obj['id']}" 
        self.send_with_retry(key=key, value=event)