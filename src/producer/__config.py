from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str = "kafka:9092"
    kafka_topic: str = "transactions-trial"

settings = Settings()