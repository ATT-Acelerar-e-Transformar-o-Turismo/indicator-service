from redis.asyncio import Redis
import os

redis_client = Redis.from_url(
    os.getenv("REDIS_URL", "redis://indicators-redis:6379"),
    encoding="utf-8",
    decode_responses=True
)
