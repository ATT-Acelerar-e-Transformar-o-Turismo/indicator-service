from fastapi import FastAPI
from routes import router as api_router
from dependencies.rabbitmq import RabbitMQConnection
from dependencies.database import connect_to_mongo, close_mongo_connection, db
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
    """Start RabbitMQ consumer and connect to MongoDB"""
    global consumer_task, rabbitmq_connection
    try:
        # Connect to MongoDB
        await connect_to_mongo()
        logger.info("Connected to MongoDB")

        # Connect to RabbitMQ
        rabbitmq_connection = RabbitMQConnection(
            url=RABBITMQ_URL,
            queue_name=QUEUE_NAME
        )
        await rabbitmq_connection.connect()
        logger.info("Connected to RabbitMQ")

        # Start RabbitMQ consumer
        consumer_task = asyncio.create_task(
            start_consuming(rabbitmq_connection))
        logger.info("Started RabbitMQ consumer")
    except Exception as e:
        logger.error(f"Startup failure: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup RabbitMQ and MongoDB connections on shutdown"""
    global consumer_task, rabbitmq_connection

    # Stop RabbitMQ
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

    # Close MongoDB
    await close_mongo_connection()
    logger.info("Closed MongoDB connection")

# Command to run the application:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
