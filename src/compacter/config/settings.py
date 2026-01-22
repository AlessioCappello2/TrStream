from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    minio_processed_bucket: str
    minio_analytics_bucket: str

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str
    minio_secret_key: str

settings = Settings()
