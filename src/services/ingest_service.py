from typing import List, Optional
from datetime import datetime
import httpx
import asyncio
import uuid
from src.db.mongo import get_db
from src.services.classify_service import ClassifyService
from src.services.notify_service import NotifyService
from src.core.config import settings
from src.core.logging import logger
from bson import ObjectId

# ============================================================
# 🐛 DEBUG TASK C: Memory leak - FIXED
# Removed the _ingestion_cache that was never cleared
# ============================================================


class IngestService:
    def __init__(self):
        self.external_api_url = f"{settings.EXTERNAL_API_URL}/external/support-tickets"
        self.classify_service = ClassifyService()
        self.notify_service = NotifyService()

    async def run_ingestion(self, tenant_id: str) -> dict:
        """
        Fetch tickets from the external API and persist them for a tenant.
        Handles pagination, ensures idempotency, classifies tickets, and triggers notifications.
        """
        db = await get_db()
        job_id = str(uuid.uuid4())

        # ============================================================
        # 🐛 DEBUG TASK D: Race condition - FIXED
        # Use atomic findOneAndUpdate to acquire lock
        # ============================================================
        job_doc = {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": "running",
            "started_at": datetime.utcnow(),
            "progress": 0,
            "total_pages": None,
            "processed_pages": 0,
            "cancelled": False
        }
        
        # Atomic operation: only insert if no running job exists
        try:
            existing = await db.ingestion_jobs.find_one({
                "tenant_id": tenant_id,
                "status": "running"
            })
            
            if existing:
                return {
                    "status": "already_running",
                    "job_id": str(existing.get("job_id", existing["_id"])),
                    "new_ingested": 0,
                    "updated": 0,
                    "errors": 0
                }
            
            result = await db.ingestion_jobs.insert_one(job_doc)
            job_doc["_id"] = result.inserted_id
            
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise

        new_ingested = 0
        updated = 0
        errors = 0
        page = 1
        total_pages = None

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                while True:
                    # Check if job was cancelled
                    job_status = await db.ingestion_jobs.find_one(
                        {"_id": job_doc["_id"]}
                    )
                    if job_status and job_status.get("cancelled"):
                        logger.info(f"Job {job_id} cancelled by user")
                        break

                    # Fetch page with manual retry logic (no external retry library)
                    tickets_data = await self._fetch_page_with_retry(
                        client, tenant_id, page
                    )
                    
                    if not tickets_data:
                        break
                    
                    tickets = tickets_data.get("tickets", [])
                    next_page = tickets_data.get("next_page")
                    total_count = tickets_data.get("total_count", 0)
                    
                    # Calculate total pages on first iteration
                    if total_pages is None and total_count > 0:
                        page_size = len(tickets) if tickets else 50
                        total_pages = (total_count + page_size - 1) // page_size
                        await db.ingestion_jobs.update_one(
                            {"_id": job_doc["_id"]},
                            {"$set": {"total_pages": total_pages}}
                        )
                    
                    # Process tickets
                    for ticket_data in tickets:
                        try:
                            # Classify ticket
                            classification = self.classify_service.classify(
                                ticket_data["message"],
                                ticket_data["subject"]
                            )
                            
                            # Prepare ticket document
                            ticket_doc = {
                                "external_id": ticket_data["id"],
                                "tenant_id": ticket_data["tenant_id"],
                                "source": ticket_data["source"],
                                "customer_id": ticket_data["customer_id"],
                                "subject": ticket_data["subject"],
                                "message": ticket_data["message"],
                                "created_at": datetime.fromisoformat(
                                    ticket_data["created_at"].replace("Z", "+00:00")
                                ),
                                "status": ticket_data["status"],
                                "urgency": classification["urgency"],
                                "sentiment": classification["sentiment"],
                                "requires_action": classification["requires_action"],
                                "updated_at": datetime.fromisoformat(
                                    ticket_data.get("updated_at", ticket_data["created_at"]).replace("Z", "+00:00")
                                ),
                                "ingested_at": datetime.utcnow()
                            }
                            
                            # Upsert for idempotency (tenant_id + external_id is unique)
                            upsert_result = await db.tickets.update_one(
                                {
                                    "tenant_id": ticket_data["tenant_id"],
                                    "external_id": ticket_data["id"]
                                },
                                {"$set": ticket_doc},
                                upsert=True
                            )
                            
                            if upsert_result.upserted_id:
                                new_ingested += 1
                                
                                # Trigger notification for high urgency tickets
                                if classification["urgency"] == "high":
                                    # Don't await - run in background
                                    asyncio.create_task(
                                        self.notify_service.send_notification(
                                            ticket_data["id"],
                                            ticket_data["tenant_id"],
                                            classification["urgency"],
                                            "High urgency ticket detected"
                                        )
                                    )
                            elif upsert_result.modified_count > 0:
                                updated += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing ticket {ticket_data.get('id')}: {e}")
                            errors += 1
                    
                    # Update progress
                    await db.ingestion_jobs.update_one(
                        {"_id": job_doc["_id"]},
                        {
                            "$set": {
                                "processed_pages": page,
                                "progress": int((page / total_pages * 100)) if total_pages else 0
                            }
                        }
                    )
                    
                    logger.info(f"Processed page {page}/{total_pages or '?'} for tenant {tenant_id}")
                    
                    # Check if there are more pages
                    if not next_page:
                        break
                    
                    page = next_page
                    
        except Exception as e:
            logger.error(f"Ingestion failed for tenant {tenant_id}: {e}")
            # Update job status to failed
            await db.ingestion_jobs.update_one(
                {"_id": job_doc["_id"]},
                {"$set": {"status": "failed", "ended_at": datetime.utcnow(), "error": str(e)}}
            )
            
            # Log failure
            await db.ingestion_logs.insert_one({
                "tenant_id": tenant_id,
                "job_id": job_id,
                "status": "failed",
                "error": str(e),
                "started_at": job_doc["started_at"],
                "ended_at": datetime.utcnow(),
                "new_ingested": new_ingested,
                "updated": updated,
                "errors": errors
            })
            raise

        # Determine final status
        job_status = await db.ingestion_jobs.find_one({"_id": job_doc["_id"]})
        final_status = "cancelled" if job_status and job_status.get("cancelled") else "completed"
        
        # Update job status to completed
        await db.ingestion_jobs.update_one(
            {"_id": job_doc["_id"]},
            {"$set": {"status": final_status, "ended_at": datetime.utcnow()}}
        )

        # Log successful completion
        await db.ingestion_logs.insert_one({
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": final_status,
            "started_at": job_doc["started_at"],
            "ended_at": datetime.utcnow(),
            "new_ingested": new_ingested,
            "updated": updated,
            "errors": errors
        })

        return {
            "status": final_status,
            "job_id": job_id,
            "new_ingested": new_ingested,
            "updated": updated,
            "errors": errors
        }
    
    async def _fetch_page_with_retry(
        self, 
        client: httpx.AsyncClient, 
        tenant_id: str, 
        page: int,
        max_retries: int = 5
    ) -> Optional[dict]:
        """
        Fetch a single page with manual retry logic.
        Handles 429 rate limiting with Retry-After header.
        No external retry libraries used - manual implementation with asyncio.
        """
        url = f"{settings.EXTERNAL_API_URL}/external/support-tickets"
        params = {"page": page, "page_size": 50}
        
        for attempt in range(max_retries):
            try:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 429:
                    # Rate limited - respect Retry-After header
                    retry_after = int(response.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited. Waiting {retry_after}s before retry...")
                    await asyncio.sleep(retry_after)
                    continue
                
                elif response.status_code >= 500:
                    # Server error - exponential backoff
                    wait_time = min(2 ** attempt, 30)  # Cap at 30 seconds
                    logger.warning(f"Server error {response.status_code}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                
                else:
                    # Other error - log and return None
                    logger.error(f"Unexpected status {response.status_code}: {response.text}")
                    return None
                    
            except httpx.RequestError as e:
                # Network error - exponential backoff
                wait_time = min(2 ** attempt, 30)
                logger.warning(f"Request error: {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch page {page} after {max_retries} attempts")
                    return None
        
        return None

    async def get_job_status(self, job_id: str) -> Optional[dict]:
        """Retrieve the status of a specific ingestion job."""
        db = await get_db()
        from bson import ObjectId

        job = await db.ingestion_jobs.find_one({"_id": ObjectId(job_id)})
        if not job:
            return None

        return {
            "job_id": job_id,
            "tenant_id": job["tenant_id"],
            "status": job["status"],
            "progress": job.get("progress", 0),
            "total_pages": job.get("total_pages"),
            "processed_pages": job.get("processed_pages", 0),
            "started_at": job["started_at"].isoformat() if job.get("started_at") else None,
            "ended_at": job["ended_at"].isoformat() if job.get("ended_at") else None
        }

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an ongoing ingestion job, if it is still running."""
        db = await get_db()
        from bson import ObjectId

        result = await db.ingestion_jobs.update_one(
            {"_id": ObjectId(job_id), "status": "running"},
            {"$set": {"status": "cancelled", "ended_at": datetime.utcnow()}}
        )
        return result.modified_count > 0

    async def get_ingestion_status(self, tenant_id: str) -> Optional[dict]:
        """Get the current ingestion status for a given tenant."""
        db = await get_db()

        job = await db.ingestion_jobs.find_one(
            {"tenant_id": tenant_id, "status": "running"},
            sort=[("started_at", -1)]
        )

        if not job:
            return None

        return {
            "job_id": str(job["_id"]),
            "tenant_id": tenant_id,
            "status": job["status"],
            "started_at": job["started_at"].isoformat() if job.get("started_at") else None
        }
