import time
import json
import logging 

from kafka import KafkaProducer
from kafka.errors import KafkaError
from ..config.settings import settings

logger = logging.getLogger("trstream.stripe.webhook")

class StripeKafkaProducer:
    def __init__(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_broker,
                linger_ms=1,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            self.topic = settings.kafka_topic
            
            self.counter = 0
            self.batch_size_log = settings.batch_size_log
            self.max_retries = settings.max_send_retries

        except Exception as e:
            logger.error(f"Kafka producer initialization failed: {e}")
            raise e


    def send_event(self, event: dict):
        logger.info(event)

        obj = event['data']['object']
        retry = 0

        while retry < self.max_retries:
            try:
                future = self.producer.send(
                    topic=self.topic,
                    key=f"stripe_{obj['object']}_{obj['id']}".encode(),
                    value=event
                )

                future.get(timeout=5)
                self.counter += 1

                if self.counter % self.batch_size_log == 0:
                    self.producer.flush()
                    logger.info(f"No. of events sent: {self.counter}")

                logger.info("Event sent to Kafka.")
                break
            except KafkaError as e:
                # handle retry/backoff if desired
                retry += 1

                if retry > self.max_retries:
                    logger.error("Max retries exceeded! Failed to send event.")
                    raise e
                
                logger.warning(f"Sending to Kafka failed. Retry no. {retry}")
                time.sleep(min(2**retry, 30))
        
