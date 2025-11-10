import logging
from .base_client import BaseApiClient
from .app_context import NotificationContext

logger = logging.getLogger(__name__)

"""
TODO this class is not yet implemented
"""
class ServiceNowClient(BaseApiClient):
    """
    Class used to generate service now incident in case of job failure
    """
    def __init__(self, ctx: NotificationContext):
        super().__init__()
        self.ctx = ctx
        self.base_url = ctx.base_url
        self.auth = (ctx.username, ctx.password)

    def send_incident(self, short_description: str, description: str):
        try:
            payload = {
                "short_description": short_description,
                "description": description,
            }
            logger.info("Sending incident to ServiceNow...")
            self.post(f"{self.base_url}/api/now/table/incident", json=payload, auth=self.auth)
            logger.info("Incident successfully sent to ServiceNow.")
        except Exception as e:
            logger.error(f"Failed to send incident: {e}")
