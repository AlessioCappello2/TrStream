from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    revolut_client_id: str
    revolut_webhook_url: str
    revolut_redirect_auth: str
    revolut_source_account: str
    revolut_target_account: str 

    upstash_redis_rest_url: str
    upstash_redis_rest_token: str

    kafka_broker: str
    kafka_topic: str

    batch_size_log: int = 400
    max_send_retries: int = 5

    class Config:
        env_file = "src/revolut.env"

settings = Settings()
