from fastapi import FastAPI
from routes import router as api_router
from dependencies.rabbitmq import RabbitMQConnection
from services.data_ingestor import start_consuming
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.include_router(api_router)

# Initialize RabbitMQ Connection
rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
rabbitmq = RabbitMQConnection(rabbitmq_url, queue_name="resource_data")


@app.on_event("startup")
async def startup_event():
    # Start the consumer in the background
    asyncio.create_task(start_consuming(rabbitmq))


@app.on_event("shutdown")
async def shutdown_event():
    await rabbitmq.close()

# Command to run the application:
# uvicorn app.main:app --host 0.0.0.0 --port 8000
