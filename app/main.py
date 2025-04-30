from fastapi import FastAPI
from routes import router as api_router
from dependencies.rabbitmq import RabbitMQConnection
from services.data_ingestor import start_consuming
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

app = FastAPI()

app.include_router(api_router)

# Store the consumer task and connection
consumer_task = None
rabbitmq_connection = None

# RabbitMQ Configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
QUEUE_NAME = "resource_data"


@app.on_event("startup")
async def startup_event():
    """Start the RabbitMQ consumer when the application starts"""
    global consumer_task, rabbitmq_connection
    try:
        # Create and connect RabbitMQ
        rabbitmq_connection = RabbitMQConnection(
            url=RABBITMQ_URL,
            queue_name=QUEUE_NAME
        )
        await rabbitmq_connection.connect()
        logger.info("Connected to RabbitMQ")

        # Start consumer in background task
        consumer_task = asyncio.create_task(
            start_consuming(rabbitmq_connection))
        logger.info("Started RabbitMQ consumer")
    except Exception as e:
        logger.error(f"Failed to start RabbitMQ consumer: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup when the application shuts down"""
    global consumer_task, rabbitmq_connection
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("Stopped RabbitMQ consumer")

    if rabbitmq_connection:
        await rabbitmq_connection.close()
        logger.info("Closed RabbitMQ connection")

# Command to run the application:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
