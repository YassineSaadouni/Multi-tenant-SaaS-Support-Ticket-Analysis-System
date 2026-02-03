import httpx
import asyncio
import random
from src.core.logging import logger

class NotifyService:
    def __init__(self):
        self.notify_url = "http://mock-external-api:9000/notify"

    async def send_notification(self, ticket_id: str, tenant_id: str, urgency: str, reason: str):
        """
        Sends a notification to the external service.
        
        This should include some form of retry/backoff and must not block
        normal request handling. External retry helper libraries should not
        be required for this exercise.
        """
        # TODO: implement asynchronous notification with retry / backoff
        pass
