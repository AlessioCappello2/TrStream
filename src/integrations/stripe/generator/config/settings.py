from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    kafka_topic: str
    stripe_secret_api_key: str
    stripe_webhook_secret: str

    class Config:
        env_file = "src/stripe.env"

settings = Settings()