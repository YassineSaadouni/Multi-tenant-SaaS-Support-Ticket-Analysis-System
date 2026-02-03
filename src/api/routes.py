from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from typing import List, Optional
from datetime import datetime
from src.db.models import TicketResponse, TenantStats
from src.db.mongo import get_db, ping_db
from src.services.ingest_service import IngestService
from src.services.analytics_service import AnalyticsService
from src.services.lock_service import LockService
from src.services.circuit_breaker import get_circuit_breaker
from src.core.config import settings

router = APIRouter()


# ============================================================
# Ticket APIs
# ============================================================

@router.get("/tickets", response_model=List[TicketResponse])
async def list_tickets(
    tenant_id: str,
    status: Optional[str] = None,
    urgency: Optional[str] = None,
    source: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=1000)
):
    """
    List tickets with resource-aware pagination.
    
    Resource Management:
    - Page size capped at MAX_PAGE_SIZE to prevent memory exhaustion
    - Query timeout enforced
    - Connection automatically returned to pool
    """
    db = await get_db()
    
    # Enforce max page size limit
    if page_size > settings.MAX_PAGE_SIZE:
        page_size = settings.MAX_PAGE_SIZE
    
    query: dict = {}

    # ============================================================
    # 🐛 DEBUG TASK A: Multi-tenant isolation bug
    # The tenant_id filter is missing here.
    # This can expose tickets that belong to other tenants.
    # ============================================================
    # NOTE: initial starter implementation; you are expected to review and adjust
    # the filtering and scoping as needed.
    if status:
        query["status"] = status
    if urgency:
        query["urgency"] = urgency
    if source:
        query["source"] = source

    # 🐛 TODO: Add tenant_id scoping to the query.
    # 🐛 TODO: Filter out tickets with a non-null deleted_at (soft delete).

    # Execute query with timeout to prevent resource exhaustion
    try:
        cursor = db.tickets.find(
            query,
            max_time_ms=settings.DEFAULT_QUERY_TIMEOUT_MS
        ).skip((page - 1) * page_size).limit(page_size)
        docs = await cursor.to_list(length=page_size)
        return docs
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query failed or timed out: {str(e)}"
        )


@router.get("/tickets/urgent", response_model=List[TicketResponse])
async def list_urgent_tickets(tenant_id: str):
    # TODO: Implement fetching urgent tickets for a tenant
    return []


@router.get("/tickets/{ticket_id}", response_model=TicketResponse)
async def get_ticket(ticket_id: str, tenant_id: str):
    # TODO: Implement fetching a single ticket
    return None


# ============================================================
# Health Check API (Task 5)
# ============================================================

@router.get("/health")
async def health_check():
    """
    System health check with dependency verification.
    
    Checks:
    - MongoDB connectivity (using connection pool)
    - Notification service (external API)
    - Circuit breaker status
    
    Returns:
    - 200 OK if all dependencies are healthy
    - 503 Service Unavailable if any dependency fails
    """
    import httpx
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": {}
    }
    
    all_healthy = True
    
    # Check MongoDB connectivity using connection pool
    try:
        is_healthy = await ping_db()
        if is_healthy:
            health_status["dependencies"]["mongodb"] = {
                "status": "healthy",
                "message": "Connection pool healthy"
            }
        else:
            all_healthy = False
            health_status["dependencies"]["mongodb"] = {
                "status": "unhealthy",
                "message": "Connection failed"
            }
    except Exception as e:
        all_healthy = False
        health_status["dependencies"]["mongodb"] = {
            "status": "unhealthy",
            "message": f"Connection failed: {str(e)}"
        }
    
    # Check notification service (external API) with proper timeout and resource cleanup
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(2.0, connect=1.0),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5)
        ) as client:
            response = await client.get(f"{settings.EXTERNAL_API_URL}/health")
            if response.status_code == 200:
                health_status["dependencies"]["notification_service"] = {
                    "status": "healthy",
                    "message": "Service reachable"
                }
            else:
                all_healthy = False
                health_status["dependencies"]["notification_service"] = {
                    "status": "unhealthy",
                    "message": f"Service returned {response.status_code}"
                }
    except httpx.TimeoutException:
        all_healthy = False
        health_status["dependencies"]["notification_service"] = {
            "status": "unhealthy",
            "message": "Connection timeout (>2s)"
        }
    except Exception as e:
        all_healthy = False
        health_status["dependencies"]["notification_service"] = {
            "status": "unhealthy",
            "message": f"Connection failed: {str(e)}"
        }
    
    # Check circuit breaker status
    try:
        cb = get_circuit_breaker("notify_api")
        cb_status = cb.get_status()
        health_status["dependencies"]["circuit_breaker"] = {
            "status": "healthy" if cb_status["state"] != "open" else "degraded",
            "state": cb_status["state"],
            "failure_rate": cb_status["recent_failure_rate"]
        }
        # Circuit open is degraded, not unhealthy
    except Exception as e:
        health_status["dependencies"]["circuit_breaker"] = {
            "status": "unknown",
            "message": f"Check failed: {str(e)}"
        }
    
    # Verify critical routes exist
    health_status["routes"] = {
        "ingestion": "/ingest/run",
        "analytics": "/tenants/{tenant_id}/stats",
        "tickets": "/tickets",
        "health": "/health",
        "circuit_status": "/circuit/{name}/status"
    }
    
    # Set final status
    if not all_healthy:
        health_status["status"] = "unhealthy"
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content=health_status
        )
    
    return health_status


# ============================================================
# Analytics API (Task 3)
# ============================================================

@router.get("/tenants/{tenant_id}/stats", response_model=TenantStats)
async def get_tenant_stats(
    tenant_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    analytics_service: AnalyticsService = Depends()
):
    """
    Retrieve analytics and statistics for a given tenant.

    TODO: Implement using MongoDB's Aggregation Pipeline:
    - Avoid Python for-loops over large result sets.
    - Ensure it can respond within ~500ms for 10,000+ tickets.
    """
    return await analytics_service.get_tenant_stats(tenant_id, from_date, to_date)


# ============================================================
# Ingestion APIs (Task 1, 8, 9)
# ============================================================

@router.post("/ingest/run")
async def run_ingestion(
    tenant_id: str,
    background_tasks: BackgroundTasks,
    ingest_service: IngestService = Depends(),
    lock_service: LockService = Depends()
):
    """
    Trigger a ticket ingestion run for a tenant.

    Concurrency Control (Task 8):
    - Uses distributed lock to prevent concurrent ingestion per tenant
    - Returns 409 Conflict if ingestion already running
    - Lock expires after 60 seconds to prevent deadlocks
    """
    import uuid
    
    # Generate unique job ID for this ingestion
    job_id = str(uuid.uuid4())
    resource_id = f"ingest:{tenant_id}"
    
    # Attempt to acquire distributed lock
    lock_acquired = await lock_service.acquire_lock(resource_id, job_id)
    
    if not lock_acquired:
        # Check if there's an active lock
        lock_status = await lock_service.get_lock_status(resource_id)
        if lock_status and not lock_status["is_expired"]:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Ingestion already running for this tenant",
                    "tenant_id": tenant_id,
                    "active_job_id": lock_status["owner_id"],
                    "started_at": lock_status["acquired_at"].isoformat(),
                    "expires_at": lock_status["expires_at"].isoformat()
                }
            )
        else:
            raise HTTPException(
                status_code=409,
                detail="Failed to acquire lock. Please try again."
            )
    
    # Lock acquired - run ingestion and ensure lock is released
    try:
        result = await ingest_service.run_ingestion(tenant_id)
        return {"status": "ingestion_started", "result": result}
    finally:
        # Always release the lock, even if ingestion fails
        await lock_service.release_lock(resource_id, job_id)


@router.get("/ingest/status")
async def get_ingestion_status(
    tenant_id: str,
    ingest_service: IngestService = Depends(),
    lock_service: LockService = Depends()
):
    """
    Get the current ingestion status for the given tenant (Task 8).

    Returns comprehensive status including:
    - Current lock status (if job is actively running)
    - Last completed job details from ingestion_logs
    - Whether a job is currently active
    """
    from src.db.mongo import get_db
    
    resource_id = f"ingest:{tenant_id}"
    
    # Check if there's an active lock
    lock_status = await lock_service.get_lock_status(resource_id)
    
    response = {
        "tenant_id": tenant_id,
        "is_running": False,
        "current_job": None,
        "last_completed": None
    }
    
    # Check for active lock
    if lock_status and not lock_status["is_expired"]:
        response["is_running"] = True
        response["current_job"] = {
            "job_id": lock_status["owner_id"],
            "started_at": lock_status["acquired_at"].isoformat(),
            "expires_at": lock_status["expires_at"].isoformat(),
            "status": "running"
        }
    
    # Get last completed job from ingestion_logs
    db = await get_db()
    last_log = await db.ingestion_logs.find_one(
        {"tenant_id": tenant_id},
        sort=[("ended_at", -1)]
    )
    
    if last_log:
        response["last_completed"] = {
            "job_id": last_log.get("job_id"),
            "status": last_log.get("status"),
            "started_at": last_log.get("started_at").isoformat() if last_log.get("started_at") else None,
            "ended_at": last_log.get("ended_at").isoformat() if last_log.get("ended_at") else None,
            "duration_seconds": last_log.get("duration_seconds"),
            "metrics": last_log.get("metrics", {})
        }
    
    return response


@router.get("/ingest/progress/{job_id}")
async def get_ingestion_progress(
    job_id: str,
    ingest_service: IngestService = Depends()
):
    """
    Retrieve ingestion job progress by `job_id` (Task 9).

    TODO: Implement:
    - Look up the job by `job_id`.
    - Return progress information (e.g., total_pages, processed_pages, status).
    """
    status = await ingest_service.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return status


@router.get("/ingest/logs/{tenant_id}")
async def get_ingestion_logs(
    tenant_id: str,
    limit: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None, pattern="^(success|partial_success|failure|cancelled)$")
):
    """
    Retrieve ingestion logs for a specific tenant (Task 6).
    
    Returns a history of ingestion runs with full traceability including:
    - Start/end timestamps
    - Status (success, partial_success, failure, cancelled)
    - Processing metrics (total tickets, high-priority count, retry attempts)
    - Error context (if applicable)
    """
    db = await get_db()
    
    query = {"tenant_id": tenant_id}
    if status:
        query["status"] = status
    
    cursor = db.ingestion_logs.find(query).sort("started_at", -1).limit(limit)
    logs = await cursor.to_list(length=limit)
    
    # Convert ObjectId to string for JSON serialization
    for log in logs:
        log["_id"] = str(log["_id"])
        log["started_at"] = log["started_at"].isoformat()
        log["ended_at"] = log["ended_at"].isoformat()
    
    return {
        "tenant_id": tenant_id,
        "logs": logs,
        "count": len(logs)
    }


@router.delete("/ingest/{job_id}")
async def cancel_ingestion(
    job_id: str,
    ingest_service: IngestService = Depends()
):
    """
    Cancel a running ingestion job (Task 9).

    TODO: Implement graceful cancellation:
    - Stop further processing while keeping already ingested data.
    """
    success = await ingest_service.cancel_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found or already completed")
    return {"status": "cancelled", "job_id": job_id}


# ============================================================
# Lock Status API (Task 8)
# ============================================================

@router.get("/ingest/lock/{tenant_id}")
async def get_lock_status(
    tenant_id: str,
    lock_service: LockService = Depends()
):
    """
    Get the current ingestion lock status for a tenant (Task 8).
    
    Returns detailed lock information including:
    - Whether a lock exists
    - Lock owner (job_id)
    - Lock acquisition and expiration times
    - Whether the lock has expired
    """
    resource_id = f"ingest:{tenant_id}"
    lock_status = await lock_service.get_lock_status(resource_id)
    
    if not lock_status:
        return {
            "tenant_id": tenant_id,
            "locked": False,
            "message": "No active lock for this tenant"
        }
    
    return {
        "tenant_id": tenant_id,
        "locked": not lock_status["is_expired"],
        "lock_details": {
            "owner_id": lock_status["owner_id"],
            "acquired_at": lock_status["acquired_at"].isoformat(),
            "expires_at": lock_status["expires_at"].isoformat(),
            "is_expired": lock_status["is_expired"]
        }
    }
    lock_service = LockService()
    status = await lock_service.get_lock_status(f"ingest:{tenant_id}")
    if not status:
        return {"locked": False, "tenant_id": tenant_id}
    return {"locked": not status["is_expired"], **status}


# ============================================================
# Circuit Breaker Status API (Task 11)
# ============================================================

@router.get("/circuit/{name}/status")
async def get_circuit_status(name: str):
    """
    Get the current status of a Circuit Breaker instance (Task 11).

    Example: `GET /circuit/notify/status`.
    """
    cb = get_circuit_breaker(name)
    return cb.get_status()


@router.post("/circuit/{name}/reset")
async def reset_circuit(name: str):
    """
    Reset the Circuit Breaker state (for debugging/testing).
    """
    cb = get_circuit_breaker(name)
    cb.reset()
    return {"status": "reset", "name": name}


# ============================================================
# Ticket History API (Task 12)
# ============================================================

@router.get("/tickets/{ticket_id}/history")
async def get_ticket_history(
    ticket_id: str,
    tenant_id: str,
    limit: int = Query(50, ge=1, le=200)
):
    """
    Retrieve the change history for a ticket (Task 12).
    """
    from src.services.sync_service import SyncService
    sync_service = SyncService()
    history = await sync_service.get_ticket_history(ticket_id, tenant_id, limit)
    return {"ticket_id": ticket_id, "history": history}
