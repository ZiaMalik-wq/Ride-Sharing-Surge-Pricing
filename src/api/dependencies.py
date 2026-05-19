"""Dependency injection and application lifespan management.

The ``get_service`` function is used as a FastAPI ``Depends()`` provider.
Tests can replace it via ``app.dependency_overrides[get_service]``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI

from src.api.services import SurgeDataService


@lru_cache
def get_service() -> SurgeDataService:
    """Return a cached singleton of the service layer.

    Using ``Depends(get_service)`` in route signatures makes the dependency
    explicit and easily replaceable in tests via ``app.dependency_overrides``.
    """
    return SurgeDataService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan context manager.

    Replaces the deprecated ``@app.on_event("startup"/"shutdown")`` pattern.
    """
    # Startup — nothing extra needed (service is lazy-initialised on first request)
    yield
    # Shutdown — release Cassandra / Redis connections
    get_service().close()
