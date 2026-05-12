import asyncio
import json
import aio_pika
from aio_pika.exceptions import AMQPConnectionError, AMQPChannelError
import logging
from typing import Callable, Awaitable, Dict, List, Optional
from config import settings

logger = logging.getLogger(__name__)

class RabbitMQClient:
    def __init__(self, url: str, pool_size: int = 5):
        self.url = url
        self.pool_size = pool_size
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel_pool: Optional[asyncio.Queue[aio_pika.abc.AbstractChannel]] = None
        self.consumers: Dict[str, Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]] = {}
        self.consumer_tasks: List[asyncio.Task] = []

    async def connect(self):
        # heartbeat=30 / reconnect_interval=5 make aio_pika notice silent
        # transport failures within ~60s instead of the default permissive
        # window — without these, the broker can drop us with no frames and
        # the consumer loop hangs on a dead iterator for minutes.
        self.connection = await aio_pika.connect_robust(
            self.url,
            heartbeat=30,
            reconnect_interval=5,
        )
        self.channel_pool = asyncio.Queue()

        for _ in range(self.pool_size):
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=10)
            await self.channel_pool.put(channel)
        logger.info("Connected and initialized channel pool")

    async def health_probe(self, timeout: float = 5.0) -> bool:
        """Active liveness check — open and close a fresh channel under a deadline.

        Pool channels can go stale across a robust reconnect, so we deliberately
        bypass the pool. A timeout here means the underlying transport is wedged
        even if `connection.is_closed` still reads False.
        """
        if not self.connection or self.connection.is_closed:
            return False
        try:
            channel = await asyncio.wait_for(self.connection.channel(), timeout=timeout)
        except (asyncio.TimeoutError, AMQPConnectionError, AMQPChannelError, RuntimeError) as e:
            logger.warning(f"Health probe: open channel failed: {e}")
            return False
        try:
            await asyncio.wait_for(channel.close(), timeout=timeout)
        except (asyncio.TimeoutError, AMQPConnectionError, AMQPChannelError, RuntimeError):
            pass
        return True

    async def close(self):
        for task in self.consumer_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self.connection:
            await self.connection.close()
            logger.info("Connection closed")

    def register_consumer(self, queue_name: str, handler: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]):
        if queue_name in self.consumers:
            raise ValueError(f"Consumer for queue '{queue_name}' already registered.")
        self.consumers[queue_name] = handler

    async def _consume(self, queue_name: str, handler: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]):
        if self.channel_pool is None:
            raise RuntimeError("Channel pool not initialized. Please use .connect() before start consuming")
        channel = await self.channel_pool.get()
        try:
            queue = await channel.declare_queue(queue_name, durable=True)
            logger.info(f"Starting consumer for '{queue_name}'")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        await handler(message)
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"Invalid message format in queue '{queue_name}': {e}")
                        await message.reject(requeue=False)
                    except (AMQPConnectionError, AMQPChannelError) as e:
                        logger.error(f"RabbitMQ connection error in queue '{queue_name}': {e}")
                        await message.nack(requeue=True)
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error(f"Consumer for queue '{queue_name}' failed: {e}")
            raise
        finally:
            await self.channel_pool.put(channel)

    async def start_consumers(self):
        logger.info(f"Starting {len(self.consumers)} consumers: {list(self.consumers.keys())}")
        for queue_name, handler in self.consumers.items():
            try:
                task = asyncio.create_task(self._consume(queue_name, handler))
                self.consumer_tasks.append(task)
                logger.info(f"Created consumer task for queue '{queue_name}'")
            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.error(f"Failed to create consumer task for queue '{queue_name}': {e}")
                raise

    async def publish(self, queue_name: str, message: str | bytes, retries: int = 3):
        """Safe publishing method with connection retries"""
        if self.channel_pool is None:
            raise RuntimeError("Channel pool not initialized. Please use .connect() before publishing")
        attempt = 0
        channel = await self.channel_pool.get()
        while attempt < retries:
            try:
                await channel.declare_queue(queue_name, durable=True)  # idempotent

                body = message.encode() if isinstance(message, str) else message
                msg = aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
                await channel.default_exchange.publish(msg, routing_key=queue_name)
                logger.info(f"Published message to '{queue_name}'")
                await self.channel_pool.put(channel)
                return
            except (aio_pika.exceptions.AMQPError, ConnectionError) as e:
                logger.warning(f"Publish attempt {attempt+1} failed: {e}")
                if 'channel' in locals():
                    await self.channel_pool.put(channel)
                attempt += 1
                await asyncio.sleep(2)
        raise RuntimeError(f"Failed to publish to '{queue_name}' after {retries} attempts.")

rabbitmq_client = RabbitMQClient(url=settings.RABBITMQ_URL)

def consumer(queue_name: str):
    def decorator(func: Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]):
        rabbitmq_client.register_consumer(queue_name, func)
        logger.info(f"Consumer '{func.__name__}' registered successfully.")
        return func
    return decorator
