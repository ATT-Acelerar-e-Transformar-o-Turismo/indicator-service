import json
import aio_pika
import logging
from dependencies.database import db
from schemas.data_segment import DataSegment, TimePoint
from services.indicator_service import get_indicator_by_resource
from dependencies.rabbitmq import consumer
from services.data_propagator import get_cache_key
from dependencies.redis import redis_client
from bson.errors import InvalidId
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def store_data_segment(segment: DataSegment):
    """Store data segment in MongoDB and clear cache"""
    try:
        # Store in MongoDB
        await db.data_segments.insert_one(segment.model_dump())
        logger.info(
            f"Stored data segment for indicator {segment.indicator_id}")

        # Clear cache for this indicator
        cache_key = get_cache_key(str(segment.indicator_id))
        await redis_client.delete(cache_key)

    except Exception as e:
        logger.error(f"Failed to store data segment: {e}")
        raise

@consumer(settings.RESOURCE_DATA_QUEUE)
async def process_message(message: aio_pika.abc.AbstractIncomingMessage):
    """Process incoming messages from RabbitMQ"""
    try:
        # Parse message
        data = json.loads(message.body.decode())
        logger.info(f"Processing message: {data}")

        resource_id = data.get('resource')
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

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Only requeue for unexpected errors
        await message.reject(requeue=True)
