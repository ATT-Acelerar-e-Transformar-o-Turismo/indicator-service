from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
client = None
db = None

async def connect_to_mongo():
    global client, db
    connection_string = os.getenv('MONGO_URI')

    if not connection_string:
        logger.error("Missing MONGO_URI environment variable")
        return

    for _ in range(5):  # Retry up to 5 times
        try:
            client = AsyncIOMotorClient(connection_string)
            await client.admin.command('ping')  # async ping!
            db = client.get_default_database()
            logger.info("Successfully connected to MongoDB")
            return
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.info("Retrying in 5 seconds...")
            await asyncio.sleep(5)

async def close_mongo_connection():
    if client:
        client.close()
        logger.info("MongoDB connection closed")

