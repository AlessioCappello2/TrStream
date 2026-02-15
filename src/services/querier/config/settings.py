from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

settings = Settings()
