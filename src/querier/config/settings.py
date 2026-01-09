from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str
    minio_secret_key: str

    duckdb_use_ssl: bool = False 
    duckdb_http_keep_alive: bool = False

    class Config:
        env_file = ".env"

settings = Settings()
