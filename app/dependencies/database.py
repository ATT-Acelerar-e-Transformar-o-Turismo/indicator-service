from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
import asyncio
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
client = None
db = None

def get_database():
    """Get the database connection. Raises an exception if not connected."""
    if db is None:
        raise Exception("Database not connected. Make sure connect_to_mongo() was called successfully.")
    return db

async def connect_to_mongo():
    global client, db
    connection_string = os.getenv('MONGO_URI')

    if not connection_string:
        logger.error("Missing MONGO_URI environment variable")
        raise Exception("Missing MONGO_URI environment variable")

    for attempt in range(5):  # Retry up to 5 times
        try:
            logger.info(f"Attempting MongoDB connection (attempt {attempt + 1}/5)")
            client = AsyncIOMotorClient(connection_string)
            await client.admin.command('ping')  # async ping!
            db = client.get_default_database()
            logger.info("Successfully connected to MongoDB")
            return
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB (attempt {attempt + 1}/5): {e}")
            if attempt < 4:  # Don't sleep on the last attempt
                logger.info("Retrying in 5 seconds...")
                await asyncio.sleep(5)
            else:
                logger.error("Failed to connect to MongoDB after 5 attempts")
                raise Exception(f"Failed to connect to MongoDB after 5 attempts: {e}")

async def close_mongo_connection():
    global client, db
    if client:
        client.close()
        logger.info("MongoDB connection closed")
        client = None
        db = None

