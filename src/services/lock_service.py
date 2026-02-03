"""
Task 8: Distributed lock service.

Implement a distributed lock using MongoDB atomic operations.
Do not use external distributed lock libraries (redis-lock, pottery, etc.).

Requirements:
1. Prevent concurrent ingestion for the same tenant.
2. Return 409 Conflict when lock acquisition fails.
3. Automatically release locks when they are not refreshed within 60 seconds (zombie lock prevention).
4. Provide lock status inspection APIs.
"""

from datetime import datetime, timedelta
from typing import Optional
from src.db.mongo import get_db


class LockService:
    """
    MongoDB-based distributed lock service.

    Hints:
    - Use `findOneAndUpdate` with `upsert` to acquire locks.
    - Use TTL-like behaviour for automatic expiration.
    - Acquire/release locks atomically.
    """

    LOCK_COLLECTION = "distributed_locks"
    LOCK_TTL_SECONDS = 60

    async def acquire_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Attempt to acquire a lock using MongoDB atomic operations.

        Args:
            resource_id: ID of the resource to lock (e.g., tenant_id).
            owner_id: Lock owner identifier (e.g., job_id).

        Returns:
            True if lock acquired, False otherwise.

        Implementation:
        - Uses findOneAndUpdate with atomic operations
        - Only acquires if no lock exists OR existing lock is expired
        - Sets TTL of 60 seconds to prevent deadlocks from crashed processes
        """
        from pymongo.errors import DuplicateKeyError
        
        db = await get_db()
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=self.LOCK_TTL_SECONDS)
        
        try:
            # Atomic operation: only acquire if no lock exists or existing lock is expired
            result = await db[self.LOCK_COLLECTION].find_one_and_update(
                {
                    "resource_id": resource_id,
                    "$or": [
                        {"expires_at": {"$lt": now}},  # Expired lock
                        {"expires_at": {"$exists": False}}  # No expiration (shouldn't happen)
                    ]
                },
                {
                    "$set": {
                        "resource_id": resource_id,
                        "owner_id": owner_id,
                        "acquired_at": now,
                        "expires_at": expires_at,
                        "last_refreshed_at": now
                    }
                },
                upsert=True,
                return_document=True  # Return the updated document
            )
        except DuplicateKeyError:
            # Lock already exists and isn't expired - couldn't acquire
            return False
        
        # Check if we successfully acquired the lock
        if result and result.get("owner_id") == owner_id:
            return True
        
        # Lock exists and is not expired - check if it belongs to another owner
        existing_lock = await db[self.LOCK_COLLECTION].find_one({"resource_id": resource_id})
        if existing_lock:
            # Lock is held by someone else and hasn't expired
            if existing_lock.get("expires_at", now) > now:
                return False
            # Lock expired, try to acquire it
            result = await db[self.LOCK_COLLECTION].find_one_and_update(
                {
                    "resource_id": resource_id,
                    "expires_at": {"$lt": now}
                },
                {
                    "$set": {
                        "owner_id": owner_id,
                        "acquired_at": now,
                        "expires_at": expires_at,
                        "last_refreshed_at": now
                    }
                },
                return_document=True
            )
            return result is not None and result.get("owner_id") == owner_id
        
        return False

    async def release_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Release a lock atomically.

        Args:
            resource_id: ID of the resource to unlock.
            owner_id: Lock owner identifier (only the owner may release).

        Returns:
            True if lock released, False otherwise.

        Implementation:
        - Only releases if owner_id matches (prevents unauthorized release)
        - Uses atomic delete operation
        """
        db = await get_db()
        
        # Only delete the lock if it's owned by the specified owner
        result = await db[self.LOCK_COLLECTION].delete_one({
            "resource_id": resource_id,
            "owner_id": owner_id
        })
        
        return result.deleted_count > 0

    async def refresh_lock(self, resource_id: str, owner_id: str) -> bool:
        """
        Refresh a lock's TTL to prevent expiration.

        Args:
            resource_id: ID of the lock to refresh.
            owner_id: Lock owner identifier.

        Returns:
            True if lock refreshed, False otherwise.

        Implementation:
        - Extends expiration time by LOCK_TTL_SECONDS
        - Only refreshes if owner_id matches
        - Useful for long-running ingestion jobs
        """
        db = await get_db()
        now = datetime.utcnow()
        new_expires_at = now + timedelta(seconds=self.LOCK_TTL_SECONDS)
        
        # Only refresh if the lock is owned by the specified owner
        result = await db[self.LOCK_COLLECTION].update_one(
            {
                "resource_id": resource_id,
                "owner_id": owner_id
            },
            {
                "$set": {
                    "expires_at": new_expires_at,
                    "last_refreshed_at": now
                }
            }
        )
        
        return result.modified_count > 0

    async def get_lock_status(self, resource_id: str) -> Optional[dict]:
        """
        Get current lock status for a resource.

        Returns:
            A dict describing the lock or None if no lock exists:
            {
                "resource_id": str,
                "owner_id": str,
                "acquired_at": datetime,
                "expires_at": datetime,
                "is_expired": bool
            }
        """
        db = await get_db()
        lock = await db[self.LOCK_COLLECTION].find_one({"resource_id": resource_id})

        if not lock:
            return None

        now = datetime.utcnow()
        expires_at = lock.get("expires_at", now)

        return {
            "resource_id": lock["resource_id"],
            "owner_id": lock["owner_id"],
            "acquired_at": lock.get("acquired_at"),
            "expires_at": expires_at,
            "is_expired": now > expires_at
        }

    async def cleanup_expired_locks(self) -> int:
        """
        Clean up expired locks (optional helper).

        Returns:
            Number of deleted locks.
        """
        db = await get_db()
        result = await db[self.LOCK_COLLECTION].delete_many({
            "expires_at": {"$lt": datetime.utcnow()}
        })
        return result.deleted_count
