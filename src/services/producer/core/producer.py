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
