"""Query execution endpoint."""

import logging
from fastapi import APIRouter, HTTPException, status
from app.models import QueryRequest, QueryResult, QueryError
from app.services.query_service import QueryService, ReadOnlyQueryError, QueryTimeoutError

logger = logging.getLogger(__name__)

router = APIRouter(tags=["query"])


@router.post(
    "/query",
    response_model=QueryResult,
    responses={
        400: {"model": QueryError, "description": "Invalid or non-read-only query"},
        422: {"description": "Validation error"},
        504: {"model": QueryError, "description": "Query timeout"},
        500: {"model": QueryError, "description": "Internal server error"}
    }
)
async def execute_query(request: QueryRequest) -> QueryResult:
    """Execute a read-only SQL query against DuckDB.
    
    Args:
        request: Query request with SQL and optional parameters.
        
    Returns:
        QueryResult with columns, rows, and execution metadata.
        
    Raises:
        HTTPException: Various status codes for different error conditions.
    """
    try:
        logger.info(f"Executing query (length: {len(request.sql)} chars)")
        logger.debug(f"SQL: {request.sql[:200]}...")  # Log first 200 chars
        
        result = await QueryService.execute_query(
            sql=request.sql,
            params=request.params,
            max_rows=request.max_rows
        )
        
        return result
    
    except ReadOnlyQueryError as e:
        logger.warning(f"Read-only validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    
    except QueryTimeoutError as e:
        logger.error(f"Query timeout: {e}")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=str(e)
        )
    
    except Exception as e:
        logger.error(f"Query execution failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during query execution"
        )
