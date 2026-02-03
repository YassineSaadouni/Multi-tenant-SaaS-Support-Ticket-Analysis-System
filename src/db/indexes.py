from src.db.mongo import get_db
import pymongo
from src.core.logging import logger


async def _safe_create_index(collection, keys, **kwargs):
    """
    Create an index, handling conflicts with existing indexes.
    If an index with the same keys but different name exists, skip creation.
    """
    try:
        await collection.create_index(keys, **kwargs)
    except pymongo.errors.OperationFailure as e:
        if e.code == 85:  # IndexOptionsConflict - index exists with different name
            # This is okay - the index with same keys already exists
            index_name = kwargs.get('name', 'unnamed')
            logger.debug(f"Index {index_name} already exists with different name, skipping")
        else:
            raise


async def create_indexes():
    """
    Create MongoDB indexes required for common query patterns and to keep
    the dataset manageable over time (e.g. compound indexes, unique
    constraints, TTL on old data).
    """
    db = await get_db()
    tickets = db.tickets

    # ============================================================
    # OPTIMIZED INDEXES for Analytics (Task: Performance Optimization)
    # ============================================================
    
    # Primary index for tenant filtering with soft delete support
    # Used by: GET /tickets, GET /tenants/{tenant_id}/stats (initial $match)
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("deleted_at", pymongo.ASCENDING)
    ], name="tenant_deleted_idx")
    
    # Compound index for analytics aggregations
    # Covers: $group by status, urgency, sentiment within a tenant
    # This allows MongoDB to use index for initial filtering AND grouping
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("deleted_at", pymongo.ASCENDING),
        ("status", pymongo.ASCENDING),
        ("urgency", pymongo.ASCENDING),
        ("sentiment", pymongo.ASCENDING)
    ], name="tenant_analytics_idx")
    
    # Index for at-risk customer queries (urgency + sentiment filtering)
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("deleted_at", pymongo.ASCENDING),
        ("urgency", pymongo.ASCENDING),
        ("sentiment", pymongo.ASCENDING),
        ("customer_id", pymongo.ASCENDING)
    ], name="tenant_atrisk_idx")
    
    # Index for hourly trend queries (created_at within tenant)
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("deleted_at", pymongo.ASCENDING),
        ("created_at", pymongo.DESCENDING)
    ], name="tenant_trend_idx")

    # Unique index for idempotency (tenant_id + external_id)
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("external_id", pymongo.ASCENDING)
    ], unique=True, name="tenant_external_unique_idx")
    
    # Index for updated_at to support incremental sync
    await _safe_create_index(tickets, [
        ("tenant_id", pymongo.ASCENDING),
        ("updated_at", pymongo.DESCENDING)
    ], name="tenant_updated_idx")

    # ingestion_jobs collection indexes
    ingestion_jobs = db.ingestion_jobs
    await _safe_create_index(ingestion_jobs, [("tenant_id", pymongo.ASCENDING)])
    await _safe_create_index(ingestion_jobs, [("status", pymongo.ASCENDING)])

    # ingestion_logs collection indexes (Task 6: Persistent Logging)
    # Optimized for common query patterns:
    # 1. Recent logs for a tenant (tenant_id + started_at DESC)
    # 2. Failed runs for a tenant (tenant_id + status + started_at DESC)
    # 3. Lookup by job_id for traceability
    ingestion_logs = db.ingestion_logs
    await _safe_create_index(ingestion_logs, [
        ("tenant_id", pymongo.ASCENDING),
        ("started_at", pymongo.DESCENDING)
    ])
    await _safe_create_index(ingestion_logs, [
        ("tenant_id", pymongo.ASCENDING),
        ("status", pymongo.ASCENDING),
        ("started_at", pymongo.DESCENDING)
    ])
    await _safe_create_index(
        ingestion_logs,
        [("job_id", pymongo.ASCENDING)],
        unique=True,
        name="job_id_unique_idx"
    )
    
    # distributed_locks collection indexes (Task 8: Concurrency Control)
    # Distributed locks for preventing concurrent ingestion per tenant
    distributed_locks = db.distributed_locks
    
    # Unique index on resource_id - ensures only one lock per resource
    await _safe_create_index(
        distributed_locks,
        [("resource_id", pymongo.ASCENDING)],
        unique=True,
        name="resource_id_unique"
    )
    
    # TTL index on expires_at - automatically cleanup expired locks
    # MongoDB will delete documents where expires_at < current time
    await _safe_create_index(
        distributed_locks,
        [("expires_at", pymongo.ASCENDING)],
        expireAfterSeconds=0,  # Delete immediately when expires_at is reached
        name="expires_at_ttl"
    )
    
    # Index on owner_id for lock ownership queries
    await _safe_create_index(
        distributed_locks,
        [("owner_id", pymongo.ASCENDING)],
        name="owner_id_idx"
    )
    
    # ticket_history collection indexes (Task 12: Change History)
    # Optimized for audit queries and ticket history retrieval
    ticket_history = db.ticket_history
    
    # Primary query pattern: get history for a specific ticket
    await _safe_create_index(ticket_history, [
        ("ticket_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])
    
    # Query pattern: get history by external_id and tenant
    await _safe_create_index(ticket_history, [
        ("tenant_id", pymongo.ASCENDING),
        ("external_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])
    
    # Query pattern: recent changes across tenant
    await _safe_create_index(ticket_history, [
        ("tenant_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])
    
    logger.info("Database indexes created/verified successfully")


# ============================================================
# Hint: Example of good index design
# ============================================================
# The commented-out indexes below illustrate better patterns.
# To address Debug Task E, replace the inefficient indexes above
# with indexes that follow these patterns.
#
# # Unique index for idempotency
# await tickets.create_index(
#     [("tenant_id", pymongo.ASCENDING), ("external_id", pymongo.ASCENDING)],
#     unique=True
# )
#
# # Efficient composite index (tenant_id first, then created_at)
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # Composite index for multi-condition queries
# await tickets.create_index([
#     ("tenant_id", pymongo.ASCENDING),
#     ("status", pymongo.ASCENDING),
#     ("created_at", pymongo.DESCENDING)
# ])
#
# # TTL index (automatic cleanup of old data)
# await tickets.create_index(
#     [("created_at", pymongo.ASCENDING)],
#     expireAfterSeconds=60 * 60 * 24 * 90  # 90 days
# )
