from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str = "kafka:9092"
    kafka_topic: str = "transactions-trial"
    bucket_name: str = "raw-data"
    limit_msg: int = 1000
    limit_time: int = 180

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin12345"

settings = Settings()