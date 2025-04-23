import asyncio
import aio_pika
import json
from datetime import datetime, timedelta
import random
import os
import aiohttp
from typing import List, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
API_URL = os.getenv("API_URL", "http://indicator-service:8080")
QUEUE_NAME = "resource_data"


async def fetch_indicators() -> List[Dict]:
    """Fetch all indicators from the API"""
    logger.info(f"Fetching indicators from {API_URL}/indicators")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/indicators") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Found {len(data)} indicators")
                    return data
                else:
                    text = await response.text()
                    logger.error(
                        f"API returned status {response.status}: {text}")
                    raise Exception(
                        f"Failed to fetch indicators: {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Connection error: {e}")
            raise


async def fetch_indicator_resources(indicator_id: str) -> List[str]:
    """Fetch resources for a specific indicator"""
    logger.info(f"Fetching resources for indicator {indicator_id}")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_URL}/indicators/{indicator_id}") as response:
                if response.status == 200:
                    data = await response.json()
                    resources = data.get("resources", [])
                    logger.info(f"Found {len(resources)} resources")
                    return resources
                else:
                    text = await response.text()
                    logger.error(
                        f"API returned status {response.status}: {text}")
                    raise Exception(
                        f"Failed to fetch resources: {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Connection error: {e}")
            raise


async def generate_sample_data(resource_id: str, num_points: int = 10) -> dict:
    """Generate sample time series data"""
    base_time = datetime.now()
    points = []

    for i in range(num_points):
        point_time = base_time + timedelta(hours=i)
        points.append({
            "x": point_time.isoformat(),
            "y": random.uniform(0, 100)
        })

    return {
        "resource": resource_id,
        "data": points
    }


async def send_data():
    # Establish connection with retries
    for _ in range(5):  # 5 attempts
        try:
            logger.info(f"Connecting to RabbitMQ at {RABBITMQ_URL}")
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            break
        except Exception as e:
            logger.error(f"Failed to connect, retrying... Error: {e}")
            await asyncio.sleep(5)
    else:
        raise Exception("Failed to connect to RabbitMQ after 5 attempts")

    logger.info("Successfully connected to RabbitMQ")

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(
            QUEUE_NAME,
            durable=True
        )

        while True:  # Continuous loop
            try:
                # Fetch all indicators
                indicators = await fetch_indicators()

                for indicator in indicators:
                    # Fetch resources for each indicator
                    resources = await fetch_indicator_resources(indicator["id"])

                    if not resources:
                        logger.warning(
                            f"No resources for indicator {indicator['id']}")
                        continue

                    logger.info(
                        f"Generating data for indicator {indicator['id']} with {len(resources)} resources")

                    # Generate and send data for each resource
                    for resource_id in resources:
                        data = await generate_sample_data(resource_id, num_points=24)
                        message = aio_pika.Message(
                            body=json.dumps(data).encode(),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        )

                        await channel.default_exchange.publish(
                            message,
                            routing_key=queue.name
                        )

                        logger.info(
                            f"Sent data for indicator {indicator['id']}, resource: {resource_id}")
                        await asyncio.sleep(1)

                logger.info("Completed data generation cycle")
                await asyncio.sleep(6)

            except Exception as e:
                logger.error(f"Error in data generation cycle: {e}")
                await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(send_data())
