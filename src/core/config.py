import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGO_URL: str = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
    EXTERNAL_API_URL: str = os.getenv("EXTERNAL_API_URL", "http://mock-external-api:9000")
    
    # Connection Pool Configuration (Production-Ready)
    MONGO_MAX_POOL_SIZE: int = int(os.getenv("MONGO_MAX_POOL_SIZE", "50"))  # Max concurrent connections
    MONGO_MIN_POOL_SIZE: int = int(os.getenv("MONGO_MIN_POOL_SIZE", "10"))  # Keep-alive connections
    MONGO_MAX_IDLE_TIME_MS: int = int(os.getenv("MONGO_MAX_IDLE_TIME_MS", "45000"))  # 45 seconds
    MONGO_WAIT_QUEUE_TIMEOUT_MS: int = int(os.getenv("MONGO_WAIT_QUEUE_TIMEOUT_MS", "10000"))  # 10 seconds
    MONGO_SERVER_SELECTION_TIMEOUT_MS: int = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))  # 5 seconds
    MONGO_CONNECT_TIMEOUT_MS: int = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "10000"))  # 10 seconds
    MONGO_SOCKET_TIMEOUT_MS: int = int(os.getenv("MONGO_SOCKET_TIMEOUT_MS", "30000"))  # 30 seconds
    
    # Query Timeout Configuration
    ANALYTICS_QUERY_TIMEOUT_MS: int = int(os.getenv("ANALYTICS_QUERY_TIMEOUT_MS", "5000"))  # 5 seconds
    DEFAULT_QUERY_TIMEOUT_MS: int = int(os.getenv("DEFAULT_QUERY_TIMEOUT_MS", "10000"))  # 10 seconds
    
    # Pagination Configuration
    MAX_PAGE_SIZE: int = int(os.getenv("MAX_PAGE_SIZE", "1000"))
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "100"))
    
    class Config:
        env_file = ".env"

settings = Settings()
