import json
import aio_pika
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
import logging
from dependencies.database import db
from schemas.data_segment import DataSegment, TimePoint, MergedIndicator, DataPoint
from services.indicator_service import get_indicator_by_resource
from dependencies.rabbitmq import consumer
from services.data_propagator import get_cache_key
from dependencies.redis import redis_client
from bson.errors import InvalidId
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def delete_keys_by_prefix(redis_client, prefix):
    keys = []
    cursor = 0
    
    # Use SCAN to find keys with the prefix
    while True:
        cursor, partial_keys = await redis_client.scan(cursor, match=f"{prefix}*")
        keys.extend(partial_keys)
        if cursor == 0:
            break
    
    # Delete the keys if any were found
    if keys:
        return await redis_client.delete(*keys)
    return 0

async def merge_indicator_data(indicator_id: str) -> list[DataPoint]:
    """Merge all segments for an indicator into sorted data points"""
    from typing import Dict, Tuple
    from datetime import datetime
    from bson.objectid import ObjectId

    segments = await db.data_segments.find(
            {"indicator_id": ObjectId(indicator_id)}
            ).to_list(None)

    time_series_points: Dict[datetime, Tuple[float, datetime]] = {}
    numeric_points = []

    for segment in segments:
        segment_timestamp = segment.get("timestamp")
        if not segment_timestamp:
            continue

        for point in segment["points"]:
            if isinstance(point["x"], (str, datetime)):
                x_value = datetime.fromisoformat(point["x"].replace(
                    'Z', '+00:00')) if isinstance(point["x"], str) else point["x"]

                current = time_series_points.get(x_value)
                if not current or segment_timestamp > current[1]:
                    time_series_points[x_value] = (
                            point["y"], segment_timestamp)
            else:
                numeric_points.append(
                        DataPoint(x=float(point["x"]), y=float(point["y"])))

    time_points = [DataPoint(x=x, y=y)
                   for x, (y, _) in time_series_points.items()]

    # Sort all points in ascending order
    all_points = time_points + numeric_points
    sorted_points = sorted(all_points, key=lambda p: p.x)

    return sorted_points


async def store_data_segment(segment: DataSegment):
    """Store data segment in raw collection and update merged collection"""
    try:
        # Store raw segment
        await db.data_segments.insert_one(segment.model_dump())
        logger.info(
                f"Stored data segment for indicator {segment.indicator_id}")

        # Update merged indicator data
        merged_points = await merge_indicator_data(str(segment.indicator_id))

        merged_indicator = MergedIndicator(
                indicator_id=segment.indicator_id,
                points=merged_points
                )

        await db.merged_indicators.update_one(
                {"indicator_id": segment.indicator_id},
                {"$set": merged_indicator.model_dump()},
                upsert=True
                )

        logger.info(
                f"Updated merged data for indicator {segment.indicator_id}")

        # Clear cache for this indicator
        cache_key = get_cache_key(str(segment.indicator_id), "")
        await delete_keys_by_prefix(redis_client, cache_key)

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"Failed to store data segment (MongoDB error): {e}")
        raise
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Failed to store data segment (Redis error): {e}")
        raise

@consumer(settings.RESOURCE_DATA_QUEUE)
async def process_message(message: aio_pika.abc.AbstractIncomingMessage):
    """Process incoming messages from RabbitMQ"""
    try:
        # Parse message
        data = json.loads(message.body.decode())
        logger.info(f"Processing message: {data}")

        resource_id = data.get('resource_id')
        points = data.get('data', [])

        if not resource_id or not points:
            logger.warning("Invalid message format - discarding message")
            await message.ack()  # Acknowledge and discard
            return

        try:
            # Find indicator by resource
            indicator = await get_indicator_by_resource(resource_id)
            if not indicator:
                logger.warning(
                        f"No indicator found for resource {resource_id} - discarding message")
                await message.ack()  # Acknowledge and discard
                return

            # Convert points to TimePoint objects
            data_points = [TimePoint(x=p['x'], y=p['y']) for p in points]

            # Create and store data segment
            segment = DataSegment(
                    indicator_id=indicator["id"],
                    resource_id=resource_id,
                    points=data_points
                    )
            await store_data_segment(segment)

            logger.info(
                    f"Successfully processed data for resource {resource_id}")
            await message.ack()

        except InvalidId as e:
            logger.error(f"Invalid ObjectId format - discarding message: {e}")
            await message.ack()  # Acknowledge and discard invalid IDs
            return

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format - discarding message: {e}")
        await message.ack()  # Acknowledge and discard invalid JSON

    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logger.error(f"Error processing message (MongoDB error): {e}")
        # Only requeue for unexpected errors
        await message.reject(requeue=True)
    except (RedisConnectionError, RedisTimeoutError) as e:
        logger.error(f"Error processing message (Redis error): {e}")
        # Only requeue for unexpected errors
        await message.reject(requeue=True)
