from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_broker: str
    
    faker_topic: str
    stripe_topic: str
    revolut_topic: str
    
    minio_ingestion_bucket: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

settings = Settings()