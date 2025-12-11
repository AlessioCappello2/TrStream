from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin12345"

settings = Settings()