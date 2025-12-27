"""
FastAPI application for CSV to PostgreSQL import service.

This is the main entry point for the API server.

Run with:
    uvicorn src.main:app --reload

Or for production:
    uvicorn src.main:app --host 0.0.0.0 --port 8000
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import (
    connections_router,
    health_router,
    import_router,
    jobs_router,
    projects_router,
    sources_router,
)
from src.api.schedule_routes import schedules_router
from src.db.management import (
    close_management_pool,
    init_management_schema,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.

    - On startup: Initialize management database schema, start scheduler
    - On shutdown: Close database connections, shutdown scheduler
    """
    # Startup
    logger.info("Starting CSV Import API...")

    try:
        init_management_schema()
        logger.info("Management database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize management database: {e}")
        raise

    # Start scheduler
    scheduler_service = None
    try:
        from src.services.scheduler import SchedulerService
        scheduler_service = SchedulerService()
        scheduler_service.start()
        logger.info("Scheduler started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        # Don't raise - API can still work without scheduler

    yield

    # Shutdown
    logger.info("Shutting down CSV Import API...")

    # Shutdown scheduler
    if scheduler_service:
        try:
            scheduler_service.shutdown()
            logger.info("Scheduler shutdown")
        except Exception as e:
            logger.error(f"Error shutting down scheduler: {e}")

    close_management_pool()
    logger.info("Database connections closed")


# Create FastAPI app
app = FastAPI(
    title="CSV Import API",
    description="Import CSV files to PostgreSQL with SFTP support and job monitoring",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS
cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health_router)
app.include_router(connections_router)
app.include_router(sources_router)
app.include_router(projects_router)
app.include_router(import_router)
app.include_router(jobs_router)
app.include_router(schedules_router)


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "name": "CSV Import API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("RELOAD", "false").lower() == "true",
    )
