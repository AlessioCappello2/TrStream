from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    upstash_redis_rest_url: str
    upstash_redis_rest_token: str

    kafka_broker: str
    revolut_topic: str

settings = Settings()