from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str = "kafka:9092"
    kafka_topic: str = "transactions-trial"
    batch_size_log: int = 100
    max_retries: int = 5

    class Config:
        env_file = ".env"

settings = Settings()