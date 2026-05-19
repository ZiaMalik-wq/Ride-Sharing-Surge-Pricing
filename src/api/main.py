"""Surge Pricing API — application assembly.

This module creates the FastAPI application instance and wires together:
- Lifespan management (``dependencies.py``)
- CORS middleware
- Custom exception handlers (``exceptions.py``)
- Route definitions (``routes.py``)
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.api.dependencies import lifespan
from src.api.exceptions import ServiceUnavailableError, ZoneNotFoundError
from src.api.routes import router

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Surge Pricing API",
    description="Real-time surge pricing engine with live and historical analytics endpoints",
    version="1.0.0",
    lifespan=lifespan,
)

# TODO: Lock down origins to the actual dashboard domain(s) before production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Custom exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(ZoneNotFoundError)
async def zone_not_found_handler(request, exc: ZoneNotFoundError):
    raise HTTPException(status_code=404, detail=str(exc))


@app.exception_handler(ServiceUnavailableError)
async def service_unavailable_handler(request, exc: ServiceUnavailableError):
    raise HTTPException(status_code=503, detail=str(exc))


# ---------------------------------------------------------------------------
# Mount routes
# ---------------------------------------------------------------------------

app.include_router(router)
