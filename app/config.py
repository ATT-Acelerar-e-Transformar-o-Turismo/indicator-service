from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    ORIGINS: str = Field(default="localhost", env="ORIGINS")
    RABBITMQ_URL: str = Field(default="amqp://guest:guest@rabbitmq/", env="RABBITMQ_URL")
    RESOURCE_DATA_QUEUE: str = Field(default="resource_data", env="RESOURCE_DATA_QUEUE")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
