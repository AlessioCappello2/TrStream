from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    stripe_secret_api_key: str

settings = Settings()