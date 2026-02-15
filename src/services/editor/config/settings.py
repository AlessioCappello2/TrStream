from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    querier_api_base: str

settings = Settings()