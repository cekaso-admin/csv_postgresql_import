"""
Webhook callback handler for job completion notifications.

Sends HTTP POST requests to callback URLs with job results.
Supports retry logic for transient failures.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

import httpx

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries


@dataclass
class WebhookPayload:
    """
    Payload sent to webhook callback URL.

    Attributes:
        job_id: Unique job identifier
        project: Project name
        status: Job status (completed, failed, partial)
        files_processed: Number of successfully processed files
        files_failed: Number of failed files
        total_inserted: Total rows inserted
        total_updated: Total rows updated
        errors: List of error messages
        duration_seconds: Job duration
    """
    job_id: str
    project: str
    status: str
    files_processed: int = 0
    files_failed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    errors: List[str] = field(default_factory=list)
    duration_seconds: Optional[float] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "job_id": self.job_id,
            "project": self.project,
            "status": self.status,
            "files_processed": self.files_processed,
            "files_failed": self.files_failed,
            "total_inserted": self.total_inserted,
            "total_updated": self.total_updated,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


def send_webhook(
    url: str,
    payload: WebhookPayload,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> bool:
    """
    Send webhook callback to URL with retry logic.

    Args:
        url: Callback URL to POST to
        payload: WebhookPayload with job results
        timeout: Request timeout in seconds
        retries: Number of retry attempts

    Returns:
        True if webhook sent successfully, False otherwise
    """
    data = payload.to_dict()

    for attempt in range(retries):
        try:
            logger.debug(
                f"Sending webhook to {url} (attempt {attempt + 1}/{retries})"
            )

            with httpx.Client(timeout=timeout) as client:
                response = client.post(
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"},
                )

                if response.status_code >= 200 and response.status_code < 300:
                    logger.info(
                        f"Webhook sent successfully to {url} "
                        f"(status: {response.status_code})"
                    )
                    return True

                logger.warning(
                    f"Webhook returned non-success status: {response.status_code} "
                    f"(attempt {attempt + 1}/{retries})"
                )

        except httpx.TimeoutException as e:
            logger.warning(
                f"Webhook timeout (attempt {attempt + 1}/{retries}): {e}"
            )

        except httpx.RequestError as e:
            logger.warning(
                f"Webhook request error (attempt {attempt + 1}/{retries}): {e}"
            )

        except Exception as e:
            logger.error(
                f"Unexpected webhook error (attempt {attempt + 1}/{retries}): {e}",
                exc_info=True
            )

        # Wait before retry (except on last attempt)
        if attempt < retries - 1:
            time.sleep(RETRY_DELAY)

    logger.error(f"Webhook failed after {retries} attempts: {url}")
    return False


async def send_webhook_async(
    url: str,
    payload: WebhookPayload,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> bool:
    """
    Send webhook callback asynchronously with retry logic.

    Args:
        url: Callback URL to POST to
        payload: WebhookPayload with job results
        timeout: Request timeout in seconds
        retries: Number of retry attempts

    Returns:
        True if webhook sent successfully, False otherwise
    """
    import asyncio

    data = payload.to_dict()

    for attempt in range(retries):
        try:
            logger.debug(
                f"Sending async webhook to {url} (attempt {attempt + 1}/{retries})"
            )

            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    url,
                    json=data,
                    headers={"Content-Type": "application/json"},
                )

                if response.status_code >= 200 and response.status_code < 300:
                    logger.info(
                        f"Webhook sent successfully to {url} "
                        f"(status: {response.status_code})"
                    )
                    return True

                logger.warning(
                    f"Webhook returned non-success status: {response.status_code} "
                    f"(attempt {attempt + 1}/{retries})"
                )

        except httpx.TimeoutException as e:
            logger.warning(
                f"Webhook timeout (attempt {attempt + 1}/{retries}): {e}"
            )

        except httpx.RequestError as e:
            logger.warning(
                f"Webhook request error (attempt {attempt + 1}/{retries}): {e}"
            )

        except Exception as e:
            logger.error(
                f"Unexpected webhook error (attempt {attempt + 1}/{retries}): {e}",
                exc_info=True
            )

        # Wait before retry (except on last attempt)
        if attempt < retries - 1:
            await asyncio.sleep(RETRY_DELAY)

    logger.error(f"Webhook failed after {retries} attempts: {url}")
    return False
