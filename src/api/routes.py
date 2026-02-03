from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from typing import List, Optional
from datetime import datetime
from src.db.models import TicketResponse, TenantStats
from src.db.mongo import get_db
from src.services.ingest_service import IngestService
from src.services.analytics_service import AnalyticsService
from src.services.lock_service import LockService
from src.services.circuit_breaker import get_circuit_breaker

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
    page_size: int = Query(20, ge=1, le=100)
):
    db = await get_db()
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

    cursor = db.tickets.find(query).skip((page - 1) * page_size).limit(page_size)
    docs = await cursor.to_list(length=page_size)
    return docs


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
    System health check.

    TODO: Implement:
    - Check MongoDB connectivity.
    - Check external API/service connectivity.
    - Return a non-200 status code if any dependency is unhealthy.
    """
    # Basic health check endpoint.
    # TODO: implement dependency checks (e.g., DB, external services).
    return {"status": "ok"}


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
    ingest_service: IngestService = Depends()
):
    """
    Trigger a ticket ingestion run for a tenant.

    TODO: Implement:
    - Task 8: Prevent concurrent ingestion using a distributed lock.
    - If ingestion is already running for this tenant, return 409 Conflict.
    """
    # TODO: Attempt to acquire a distributed lock before starting ingestion.
    # lock_service = LockService()
    # if not await lock_service.acquire_lock(f"ingest:{tenant_id}", job_id):
    #     raise HTTPException(status_code=409, detail="Ingestion already running")

    result = await ingest_service.run_ingestion(tenant_id)
    return {"status": "ingestion_started", "result": result}


@router.get("/ingest/status")
async def get_ingestion_status(
    tenant_id: str,
    ingest_service: IngestService = Depends()
):
    """
    Get the current ingestion status for the given tenant (Task 8).

    Returns the current ingestion job state for this tenant.
    """
    status = await ingest_service.get_ingestion_status(tenant_id)
    if not status:
        return {"status": "idle", "tenant_id": tenant_id}
    return status


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
async def get_lock_status(tenant_id: str):
    """
    Get the current ingestion lock status for a tenant (Task 8).
    """
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
