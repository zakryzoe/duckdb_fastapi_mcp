"""FastAPI application initialization and lifecycle management."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app import db
from app.api.routes_health import router as health_router
from app.api.routes_query import router as query_router
from app.config import settings
from app.fabric_client import FabricClient
from app.services.query_service import ReadOnlyQueryError, QueryTimeoutError

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown events."""
    # Startup
    logger.info("Starting DuckDB Query API")
    logger.info(f"Configuration: {settings.app_name} v{settings.app_version}")
    
    try:
        # Initialize Fabric client
        logger.info("Initializing Fabric client")
        fabric_client = FabricClient(settings)
        
        # Store fabric_client in app state
        app.state.fabric_client = fabric_client
        
        # Initialize DuckDB connection and register tables as views
        logger.info("Initializing DuckDB connection")
        db.initialize_duckdb(settings, fabric_client)
        
        logger.info("Application startup complete")
        logger.info(f"Registered tables: {', '.join(settings.tables_list) if settings.tables_list else 'None'}")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down DuckDB Query API")
    try:
        db.close_connection()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Read-only SQL query API for Microsoft Fabric Lakehouse via DuckDB",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)


# Exception handlers
@app.exception_handler(ReadOnlyQueryError)
async def read_only_query_error_handler(request: Request, exc: ReadOnlyQueryError):
    """Handle read-only query validation errors."""
    return JSONResponse(
        status_code=400,
        content={
            "detail": str(exc),
            "error_type": "ReadOnlyViolation"
        }
    )


@app.exception_handler(QueryTimeoutError)
async def query_timeout_error_handler(request: Request, exc: QueryTimeoutError):
    """Handle query timeout errors."""
    return JSONResponse(
        status_code=504,
        content={
            "detail": str(exc),
            "error_type": "QueryTimeout"
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors with safe error messages."""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": f"An unexpected error occurred. {exc}",
            "error_type": "InternalServerError"
        }
    )


# Include routers
app.include_router(health_router)
app.include_router(query_router)


# Root endpoint
@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint redirects to API docs."""
    return {
        "message": f"{settings.app_name} v{settings.app_version}",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )
