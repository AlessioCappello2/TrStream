from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    kafka_topic: str
    stripe_secret_api_key: str
    stripe_webhook_secret: str

    batch_size_log: int = 400
    max_send_retries: int = 5

    class Config:
        env_file = "src/stripe.env"

settings = Settings()
