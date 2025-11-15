"""Health check endpoint."""

import logging
from fastapi import APIRouter, HTTPException
from app import db
from app.models import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint that verifies DuckDB connectivity.
    
    Returns:
        HealthResponse with status "ok" if healthy.
        
    Raises:
        HTTPException: 503 if DuckDB connection is not available.
    """
    try:
        # Verify DuckDB connectivity
        conn = db.get_connection()
        conn.execute("SELECT 1").fetchone()
        
        return HealthResponse(status="ok")
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Service unavailable: DuckDB connection error"
        )
