from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    minio_ingestion_bucket: str
    minio_processed_bucket: str
    batch_size: int = 100

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin12345"

settings = Settings()