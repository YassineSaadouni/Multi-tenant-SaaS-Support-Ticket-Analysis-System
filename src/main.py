from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import time
from contextlib import asynccontextmanager
from src.api.routes import router
from src.db.indexes import create_indexes
from src.db.mongo import get_client, close_db_connection, ping_db
from src.core.logging import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle with proper resource management.
    
    Startup:
    - Initialize connection pool
    - Create database indexes
    - Verify database connectivity
    
    Shutdown:
    - Close connection pool gracefully
    - Clean up resources
    """
    # Startup
    logger.info("Starting application...")
    try:
        # Initialize connection pool
        client = get_client()
        logger.info("MongoDB connection pool initialized")
        
        # Verify connectivity
        if await ping_db():
            logger.info("Database connectivity verified")
        else:
            logger.error("Failed to connect to database")
            raise Exception("Database connection failed")
        
        # Create indexes
        await create_indexes()
        logger.info("Database indexes created")
        
        yield
        
    finally:
        # Shutdown
        logger.info("Shutting down application...")
        await close_db_connection()
        logger.info("Connection pool closed")


app = FastAPI(
    title="Support Ticket Analysis System",
    lifespan=lifespan
)


@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    """Enforce request timeout for analytics endpoints."""
    if request.url.path.endswith("/stats"):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        if process_time > 2.0:
            return JSONResponse(
                status_code=504,
                content={"detail": "Performance Limit Exceeded: Aggregation took too long (> 2s)"}
            )
        return response
    return await call_next(request)


app.include_router(router)


@app.get("/health")
async def health_check():
    """Basic health check endpoint."""
    return {"status": "ok"}
