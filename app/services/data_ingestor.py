import json
import aio_pika
import logging
from dependencies.database import db
from schemas.data_segment import DataSegment, DataPoint
from services.indicator_service import get_indicator_by_id
from dependencies.rabbitmq import RabbitMQConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def store_data_segment(segment: DataSegment):
    """Store data segment in MongoDB"""
    try:
        await db.data_segments.insert_one(segment.model_dump())
        logger.info(
            f"Stored data segment for indicator {segment.indicator_id}")
    except Exception as e:
        logger.error(f"Failed to store data segment: {e}")
        raise


async def process_message(message: aio_pika.IncomingMessage):
    """Process incoming messages from RabbitMQ"""
    async with message.process():
        # Parse message
        data = json.loads(message.body.decode())
        resource_id = data.get('resource')
        points = data.get('data', [])

        if not resource_id or not points:
            logger.warning("Invalid message format")
            return

        # Find indicator by resource
        indicator = await get_indicator_by_id(resource_id)
        if not indicator:
            raise ValueError(
                f"No indicator found for resource {resource_id}")

        # Convert points to DataPoint objects
        data_points = [DataPoint(x=p['x'], y=p['y']) for p in points]

        # Create and store data segment
        segment = DataSegment(
            indicator_id=indicator["id"],
            points=data_points
        )
        await store_data_segment(segment)

        logger.info(
            f"Successfully processed data for resource {resource_id}")


async def start_consuming(connection: RabbitMQConnection):
    """Start consuming messages from RabbitMQ"""
    queue = await connection.get_queue()

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await process_message(message)
