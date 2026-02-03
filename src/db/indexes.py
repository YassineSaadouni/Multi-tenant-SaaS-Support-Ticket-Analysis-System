from src.db.mongo import get_db
import pymongo


async def create_indexes():
    """
    Create MongoDB indexes required for common query patterns and to keep
    the dataset manageable over time (e.g. compound indexes, unique
    constraints, TTL on old data).
    """
    db = await get_db()
    tickets = db.tickets

     # 🐛 DEBUG TASK E: Inefficient indexes
    # The indexes below are intentionally misaligned with real query
    # patterns and will cause performance issues.
    # ============================================================

    # 🐛 Issue 1: Index on created_at without tenant_id
    # Most queries filter by tenant_id, so this index is rarely used.
    await tickets.create_index([("created_at", pymongo.ASCENDING)])

    # 🐛 Issue 2: Single-field index on a low-cardinality field
    # status has only three values (open/closed/pending), so selectivity is low.
    await tickets.create_index([("status", pymongo.ASCENDING)])

    # 🐛 Issue 3: Single-field index on urgency (also low cardinality)
    await tickets.create_index([("urgency", pymongo.ASCENDING)])

     # 🐛 Issue 4: Wrong order in composite index
    # Queries typically filter by tenant_id and then sort by created_at,
    # but this index uses the reverse order.
    await tickets.create_index([
        ("created_at", pymongo.DESCENDING),
        ("tenant_id", pymongo.ASCENDING)
    ])

    # 🐛 Issue 5: Missing unique index for idempotency
    # The (tenant_id, external_id) pair should be unique to prevent duplicates.
    # FIXED: Adding unique index for idempotency
    await tickets.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("external_id", pymongo.ASCENDING)
    ], unique=True)
    
    # Index for soft delete queries - exclude deleted tickets from normal queries
    await tickets.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("deleted_at", pymongo.ASCENDING)
    ])
    
    # Index for updated_at to support incremental sync
    await tickets.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("updated_at", pymongo.DESCENDING)
    ])

    # ingestion_jobs 컬렉션 인덱스
    ingestion_jobs = db.ingestion_jobs
    await ingestion_jobs.create_index([("tenant_id", pymongo.ASCENDING)])
    await ingestion_jobs.create_index([("status", pymongo.ASCENDING)])

    # ingestion_logs 컬렉션 인덱스 (Task 6: Persistent Logging)
    # Optimized for common query patterns:
    # 1. Recent logs for a tenant (tenant_id + started_at DESC)
    # 2. Failed runs for a tenant (tenant_id + status + started_at DESC)
    # 3. Lookup by job_id for traceability
    ingestion_logs = db.ingestion_logs
    await ingestion_logs.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("started_at", pymongo.DESCENDING)
    ])
    await ingestion_logs.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("status", pymongo.ASCENDING),
        ("started_at", pymongo.DESCENDING)
    ])
    # Unique index on job_id with explicit name to avoid conflicts
    try:
        await ingestion_logs.create_index(
            [("job_id", pymongo.ASCENDING)],
            unique=True,
            name="job_id_unique_idx"
        )
    except Exception:
        # Index might already exist or conflict - drop and recreate
        try:
            await ingestion_logs.drop_index("job_id_1")
        except Exception:
            pass
        await ingestion_logs.create_index(
            [("job_id", pymongo.ASCENDING)],
            unique=True,
            name="job_id_unique_idx"
        )
    
    # distributed_locks collection indexes (Task 8: Concurrency Control)
    # Distributed locks for preventing concurrent ingestion per tenant
    distributed_locks = db.distributed_locks
    
    # Unique index on resource_id - ensures only one lock per resource
    await distributed_locks.create_index(
        [("resource_id", pymongo.ASCENDING)],
        unique=True,
        name="resource_id_unique"
    )
    
    # TTL index on expires_at - automatically cleanup expired locks
    # MongoDB will delete documents where expires_at < current time
    await distributed_locks.create_index(
        [("expires_at", pymongo.ASCENDING)],
        expireAfterSeconds=0,  # Delete immediately when expires_at is reached
        name="expires_at_ttl"
    )
    
    # Index on owner_id for lock ownership queries
    await distributed_locks.create_index(
        [("owner_id", pymongo.ASCENDING)],
        name="owner_id_idx"
    )
    
    # ticket_history collection indexes (Task 12: Change History)
    # Optimized for audit queries and ticket history retrieval
    ticket_history = db.ticket_history
    
    # Primary query pattern: get history for a specific ticket
    await ticket_history.create_index([
        ("ticket_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])
    
    # Query pattern: get history by external_id and tenant
    await ticket_history.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("external_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])
    
    # Query pattern: recent changes across tenant
    await ticket_history.create_index([
        ("tenant_id", pymongo.ASCENDING),
        ("changed_at", pymongo.DESCENDING)
    ])


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
