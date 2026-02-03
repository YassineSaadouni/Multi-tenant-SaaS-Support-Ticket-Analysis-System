from motor.motor_asyncio import AsyncIOMotorClient
from contextlib import asynccontextmanager
import os
from src.core.config import settings
from src.core.logging import logger

DB_NAME = "support_saas_db"

# Global connection pool (singleton pattern)
_client: AsyncIOMotorClient = None


def get_client() -> AsyncIOMotorClient:
    """
    Get or create the MongoDB client with connection pooling.
    
    Connection pool configuration:
    - maxPoolSize: Maximum concurrent connections (prevents thread starvation)
    - minPoolSize: Minimum keep-alive connections (reduces latency)
    - maxIdleTimeMS: Connection idle timeout (prevents resource leaks)
    - waitQueueTimeoutMS: Checkout timeout (prevents deadlock under load)
    - serverSelectionTimeoutMS: Server discovery timeout
    - connectTimeoutMS: Initial connection timeout
    - socketTimeoutMS: Read/write operation timeout
    """
    global _client
    
    if _client is None:
        logger.info(
            f"Initializing MongoDB connection pool: "
            f"maxPoolSize={settings.MONGO_MAX_POOL_SIZE}, "
            f"minPoolSize={settings.MONGO_MIN_POOL_SIZE}, "
            f"waitQueueTimeout={settings.MONGO_WAIT_QUEUE_TIMEOUT_MS}ms"
        )
        
        _client = AsyncIOMotorClient(
            settings.MONGO_URL,
            maxPoolSize=settings.MONGO_MAX_POOL_SIZE,
            minPoolSize=settings.MONGO_MIN_POOL_SIZE,
            maxIdleTimeMS=settings.MONGO_MAX_IDLE_TIME_MS,
            waitQueueTimeoutMS=settings.MONGO_WAIT_QUEUE_TIMEOUT_MS,
            serverSelectionTimeoutMS=settings.MONGO_SERVER_SELECTION_TIMEOUT_MS,
            connectTimeoutMS=settings.MONGO_CONNECT_TIMEOUT_MS,
            socketTimeoutMS=settings.MONGO_SOCKET_TIMEOUT_MS,
            # Connection pool monitoring
            appname="support-saas-api",
            # Retry configuration
            retryWrites=True,
            retryReads=True,
        )
    
    return _client


async def get_db():
    """
    Returns a database instance using the shared connection pool.
    
    This function maintains backward compatibility while using
    the connection pool under the hood.
    """
    client = get_client()
    return client[DB_NAME]


@asynccontextmanager
async def get_db_context():
    """
    Context manager for database access with proper resource management.
    
    Usage:
        async with get_db_context() as db:
            result = await db.tickets.find_one({"tenant_id": "tenant_a"})
    
    This ensures connections are properly returned to the pool even if
    an exception occurs.
    """
    db = await get_db()
    try:
        yield db
    finally:
        # Motor handles connection pooling automatically
        # No explicit cleanup needed, connection returns to pool
        pass


async def close_db_connection():
    """
    Close the MongoDB connection pool.
    Should be called during application shutdown.
    """
    global _client
    
    if _client is not None:
        logger.info("Closing MongoDB connection pool")
        _client.close()
        _client = None


async def ping_db() -> bool:
    """
    Ping the database to verify connectivity.
    Used for health checks.
    
    Returns:
        bool: True if connection is healthy, False otherwise
    """
    try:
        client = get_client()
        await client.admin.command('ping', maxTimeMS=2000)
        return True
    except Exception as e:
        logger.error(f"Database ping failed: {e}")
        return False