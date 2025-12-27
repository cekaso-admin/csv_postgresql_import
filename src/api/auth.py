"""
Simple API Key authentication for the CSV Import API.

Usage:
    from src.api.auth import require_api_key

    @router.get("/protected")
    async def protected_route(api_key: str = Depends(require_api_key)):
        return {"message": "authenticated"}
"""

import os
from typing import Optional

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

# API Key header scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_api_key() -> Optional[str]:
    """Get the API key from environment variable."""
    return os.getenv("API_KEY")


async def require_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Dependency that requires a valid API key.

    Checks the X-API-Key header against the API_KEY environment variable.

    Raises:
        HTTPException 401: If no API key provided
        HTTPException 403: If API key is invalid

    Returns:
        The validated API key
    """
    expected_key = get_api_key()

    # If no API_KEY is configured, reject all requests
    if not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_KEY not configured on server",
        )

    # Check if API key was provided
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Provide X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Validate API key
    if api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

    return api_key
