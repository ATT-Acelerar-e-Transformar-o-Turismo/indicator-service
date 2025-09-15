from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ORIGINS: str = Field(default="localhost", env="ORIGINS")
    MONGO_URI: str = Field(default="mongodb://localhost:27017", env="MONGO_URI")
    RABBITMQ_URL: str = Field(
        default="amqp://guest:guest@rabbitmq/", env="RABBITMQ_URL"
    )
    RESOURCE_DATA_QUEUE: str = Field(default="resource_data", env="RESOURCE_DATA_QUEUE")
    REDIS_URL: str = Field(default="redis://indicators-redis:6379", env="REDIS_URL")
    
    # Cache settings
    CACHE_KEY_PREFIX: str = Field(default="indicator_data:", env="CACHE_KEY_PREFIX")
    CACHE_COUNTER_PREFIX: str = Field(default="indicator_miss:", env="CACHE_COUNTER_PREFIX")
    CACHE_TTL_SECONDS: int = Field(default=3600, env="CACHE_TTL_SECONDS")  # 1 hour
    MISS_COUNTER_TTL: int = Field(default=90, env="MISS_COUNTER_TTL")  # seconds
    MISS_THRESHOLD: int = Field(default=5, env="MISS_THRESHOLD")  # misses before full cache

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
