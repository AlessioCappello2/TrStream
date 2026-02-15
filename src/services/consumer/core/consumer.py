from shared.kafka.consumer import BaseKafkaConsumer
from consumer.config.settings import settings

class SafeKafkaConsumer(BaseKafkaConsumer):
    """Consumer wrapper for safe polling and graceful shutdown."""
    
    def __init__(self, **kwargs):
        super().__init__(
            **kwargs
        )