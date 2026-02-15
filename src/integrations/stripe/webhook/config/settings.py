from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    stripe_topic: str
    stripe_webhook_secret: str

settings = Settings()
