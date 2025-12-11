from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    bucket_name: str = "tb-transactions"
    target_size: int = 256 * 1024 * 1024 # 256 MB expressed in Bytes

    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "admin12345"

settings = Settings()