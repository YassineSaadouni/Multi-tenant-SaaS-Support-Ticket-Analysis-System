from motor.motor_asyncio import AsyncIOMotorClient
import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
DB_NAME = "support_saas"

async def get_db():
    """
    Returns a database instance.
    """
    client = AsyncIOMotorClient(MONGO_URL)
    return client[DB_NAME]