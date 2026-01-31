from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    kafka_topic: str
    upstash_redis_rest_url: str
    upstash_redis_rest_token: str

    batch_size_log: int = 400
    max_send_retries: int = 5

    class Config:
        env_file = "src/worker/.env"

settings = Settings()