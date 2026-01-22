from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    kafka_topic: str
    stripe_topic: str
    minio_ingestion_bucket: str

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin12345"

settings = Settings()