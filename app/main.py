import asyncio
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as api_router
from dependencies.rabbitmq import rabbitmq_client
import logging
from contextlib import asynccontextmanager
from config import settings
import services.data_ingestor  # noqa
import services.resource_cleanup  # noqa
import services.series_translations  # noqa

logger = logging.getLogger(__name__)

# Watchdog: probe the MQ connection every WATCHDOG_INTERVAL seconds; if it
# fails FAILURE_THRESHOLD times in a row, exit the process so Docker (restart
# policy) re-creates the container with fresh connections. Backstop for
# aio_pika's robust-connection failing silently with a wedged iterator.
WATCHDOG_INTERVAL = 30
WATCHDOG_FAILURE_THRESHOLD = 3


async def _mq_watchdog():
    failures = 0
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            ok = await rabbitmq_client.health_probe()
            if ok:
                if failures:
                    logger.info(f"Watchdog: connection recovered after {failures} failure(s)")
                failures = 0
                continue
            failures += 1
            logger.warning(
                f"Watchdog: probe failed ({failures}/{WATCHDOG_FAILURE_THRESHOLD})"
            )
            if failures >= WATCHDOG_FAILURE_THRESHOLD:
                logger.error("Watchdog: MQ unrecoverable, exiting for Docker restart")
                os._exit(1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Watchdog: unexpected error: {e}")


app = FastAPI()

# CORS Configuration
origins = settings.ORIGINS.split(",")
logger.info(f"Origins: {origins}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await rabbitmq_client.connect()
    logger.info("Connected to RabbitMQ")

    # Start consumers (all consumers must be declared and registered on this phase)
    await rabbitmq_client.start_consumers()
    logger.info("Started RabbitMQ consumers")

    watchdog_task = asyncio.create_task(_mq_watchdog())

    try:
        yield
    finally:
        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass
        if rabbitmq_client:
            await rabbitmq_client.close()
            logger.info("Closed RabbitMQ connection")


app.router.lifespan_context = lifespan
