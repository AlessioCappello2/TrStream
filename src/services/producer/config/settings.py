from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    faker_topic: str

settings = Settings()