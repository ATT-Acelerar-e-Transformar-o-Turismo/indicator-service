from redis.asyncio import Redis
from config.settings import settings

redis_client = Redis.from_url(
    settings.REDIS_URL, encoding="utf-8", decode_responses=True
)
