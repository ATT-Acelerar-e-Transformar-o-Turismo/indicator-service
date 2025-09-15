import json
import logging
from typing import Optional
from datetime import datetime
from dependencies.database import db
from dependencies.redis import redis_client
from bson.objectid import ObjectId
from schemas.statistics import IndicatorStatistics
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from config import settings

logger = logging.getLogger(__name__)

STATS_CACHE_PREFIX = "indicator_stats:"


def get_stats_cache_key(indicator_id: str) -> str:
    """Generate cache key for indicator statistics"""
    return f"{STATS_CACHE_PREFIX}{indicator_id}"


async def get_indicator_statistics(indicator_id: str) -> IndicatorStatistics:
    """Get comprehensive statistics for an indicator with caching"""
    cache_key = get_stats_cache_key(indicator_id)
    
    # Try cache first
    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            stats_dict = json.loads(cached_data)
            return IndicatorStatistics(**stats_dict)
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Redis cache error: {e}")

    # Compute statistics from merged indicators
    try:
        pipeline = [
            {"$match": {"indicator_id": ObjectId(indicator_id)}},
            {"$unwind": "$points"},
            {"$group": {
                "_id": None,
                "count": {"$sum": 1},
                "min_x": {"$min": "$points.x"},
                "max_x": {"$max": "$points.x"}, 
                "avg_y": {"$avg": "$points.y"},
                "min_y": {"$min": "$points.y"},
                "max_y": {"$max": "$points.y"}
            }}
        ]
        
        result = await db.merged_indicators.aggregate(pipeline).to_list(1)
        
        if not result:
            stats = IndicatorStatistics(
                count=0,
                min_x=None,
                max_x=None,
                avg_y=None,
                min_y=None,
                max_y=None
            )
        else:
            data = result[0]
            stats = IndicatorStatistics(
                count=data["count"],
                min_x=data.get("min_x"),
                max_x=data.get("max_x"),
                avg_y=data.get("avg_y"),
                min_y=data.get("min_y"),
                max_y=data.get("max_y")
            )

        # Cache the result
        try:
            await redis_client.set(
                cache_key,
                json.dumps(stats.model_dump(), default=str),
                ex=settings.STATS_CACHE_TTL
            )
        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.error(f"Redis cache error: {e}")

        return stats

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"MongoDB error getting statistics: {e}")
        raise