import httpx
import asyncio
from typing import Optional
from src.core.logging import logger
from src.core.config import settings
from src.services.circuit_breaker import get_circuit_breaker, CircuitBreakerOpenError


class NotifyService:
    """
    Asynchronous notification service with retry logic and circuit breaker.
    Handles high-priority ticket notifications without blocking the main flow.
    """
    
    def __init__(self):
        self.notify_url = f"{settings.EXTERNAL_API_URL}/notify"
        self.circuit_breaker = get_circuit_breaker("notify_api")
        self.max_retries = 5
        self.initial_backoff = 1.0  # seconds
        self.max_backoff = 30.0  # seconds

    async def send_notification(
        self, 
        ticket_id: str, 
        tenant_id: str, 
        urgency: str, 
        reason: str
    ) -> bool:
        """
        Send a notification asynchronously with retry logic.
        This is a fire-and-forget operation that doesn't block the caller.
        
        Args:
            ticket_id: Ticket identifier
            tenant_id: Tenant identifier
            urgency: Urgency level
            reason: Notification reason
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        try:
            await self._send_with_retry(ticket_id, tenant_id, urgency, reason)
            return True
        except Exception as e:
            logger.error(f"Failed to send notification for ticket {ticket_id}: {e}")
            return False

    async def _send_with_retry(
        self, 
        ticket_id: str, 
        tenant_id: str, 
        urgency: str, 
        reason: str
    ) -> None:
        """
        Send notification with exponential backoff retry for 5xx and timeouts.
        No retry for 4xx errors (client errors).
        """
        payload = {
            "ticket_id": ticket_id,
            "tenant_id": tenant_id,
            "urgency": urgency,
            "reason": reason
        }
        
        for attempt in range(self.max_retries):
            try:
                # Use circuit breaker to protect against service instability
                await self.circuit_breaker.call(
                    self._make_request,
                    payload
                )
                logger.info(f"Notification sent successfully for ticket {ticket_id}")
                return
                
            except CircuitBreakerOpenError as e:
                logger.warning(
                    f"Circuit breaker is OPEN for notifications. "
                    f"Retry after {e.retry_after:.1f}s. Ticket: {ticket_id}"
                )
                # Don't retry when circuit is open
                raise
                
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                
                # 4xx errors: don't retry (client error)
                if 400 <= status_code < 500:
                    logger.warning(
                        f"Client error {status_code} sending notification "
                        f"for ticket {ticket_id}. Not retrying."
                    )
                    raise
                
                # 5xx errors: retry with exponential backoff
                if 500 <= status_code < 600:
                    if attempt < self.max_retries - 1:
                        wait_time = min(
                            self.initial_backoff * (2 ** attempt),
                            self.max_backoff
                        )
                        logger.warning(
                            f"Server error {status_code} sending notification "
                            f"for ticket {ticket_id}. Retrying in {wait_time}s "
                            f"(attempt {attempt + 1}/{self.max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"Failed to send notification for ticket {ticket_id} "
                            f"after {self.max_retries} attempts"
                        )
                        raise
                        
            except (httpx.TimeoutException, httpx.RequestError) as e:
                # Timeout or network error: retry with exponential backoff
                if attempt < self.max_retries - 1:
                    wait_time = min(
                        self.initial_backoff * (2 ** attempt),
                        self.max_backoff
                    )
                    logger.warning(
                        f"Network error sending notification for ticket {ticket_id}: {e}. "
                        f"Retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Failed to send notification for ticket {ticket_id} "
                        f"after {self.max_retries} attempts: {e}"
                    )
                    raise

    async def _make_request(self, payload: dict) -> None:
        """
        Make the actual HTTP request to the notification endpoint.
        This method is wrapped by the circuit breaker.
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(self.notify_url, json=payload)
            response.raise_for_status()
