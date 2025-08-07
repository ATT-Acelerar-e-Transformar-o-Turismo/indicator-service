from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as api_router
from dependencies.rabbitmq import rabbitmq_client
import logging
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from config import settings
import services.data_ingestor

load_dotenv()

logger = logging.getLogger(__name__)

app = FastAPI()

# CORS Configuration
origins = settings.ORIGINS
logger.warning(f"Origins: {origins}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
QUEUE_NAME = "resource_data"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await rabbitmq_client.connect()
    logger.info("Connected to RabbitMQ")

    # Start consumers (all consumers must be declared and registered on this phase)
    await rabbitmq_client.start_consumers()
    logger.info("Started RabbitMQ consumers")

    try:
        yield
    finally:
        if rabbitmq_client:
            await rabbitmq_client.close()
            logger.info("Closed RabbitMQ connection")

app.router.lifespan_context = lifespan
