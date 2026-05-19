"""API route definitions for the Surge Pricing API.

All routes are defined on an ``APIRouter`` and mounted in ``main.py``.
Each route receives the service layer via FastAPI's ``Depends()`` mechanism.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_service
from src.api.response_models import (
    GenericDataResponse,
    HealthResponse,
    HistoryResponse,
    SurgeDetailResponse,
    SurgeListResponse,
    ZoneListResponse,
)
from src.api.services import SurgeDataService

router = APIRouter()


@router.get("/")
def root():
    return {"status": "online", "message": "Surge Pricing API is running."}


@router.get("/surge/current", response_model=SurgeListResponse)
def get_current_surge(service: SurgeDataService = Depends(get_service)):
    return {"data": service.get_all_current_surge()}


@router.get("/surge", response_model=SurgeListResponse)
def get_all_surge_prices(service: SurgeDataService = Depends(get_service)):
    return {"data": service.get_all_current_surge()}


@router.get("/surge/{zone_id}", response_model=SurgeDetailResponse)
def get_surge_by_zone(
    zone_id: str,
    service: SurgeDataService = Depends(get_service),
):
    return {"data": service.get_current_surge(zone_id)}


@router.get("/zones", response_model=ZoneListResponse)
def get_active_zones(service: SurgeDataService = Depends(get_service)):
    return {"data": service.get_live_zone_ids()}


@router.get("/analytics/summary", response_model=GenericDataResponse)
def get_analytics_summary(service: SurgeDataService = Depends(get_service)):
    return {"data": service.get_summary()}


@router.get("/analytics/zones/{zone_id}/history", response_model=HistoryResponse)
def get_zone_history(
    zone_id: str,
    limit: int = Query(100, ge=1, le=1000),
    days: int = Query(1, ge=1, le=30),
    service: SurgeDataService = Depends(get_service),
):
    return {"data": service.get_zone_history(zone_id=zone_id, limit=limit, days=days)}


@router.get("/analytics/zones/{zone_id}/trend", response_model=GenericDataResponse)
def get_zone_trend(
    zone_id: str,
    limit: int = Query(120, ge=1, le=1000),
    service: SurgeDataService = Depends(get_service),
):
    return {"data": service.get_zone_trend(zone_id=zone_id, limit=limit)}


@router.get("/analytics/top-zones", response_model=SurgeListResponse)
def get_top_zones(
    limit: int = Query(10, ge=1, le=100),
    service: SurgeDataService = Depends(get_service),
):
    return {"data": service.get_top_zones(limit=limit)}


@router.get("/analytics/anomalies", response_model=SurgeListResponse)
def get_anomalies(
    threshold: float = Query(2.0, ge=1.0),
    limit: int = Query(50, ge=1, le=1000),
    service: SurgeDataService = Depends(get_service),
):
    return {"data": service.get_anomalies(threshold=threshold, limit=limit)}


@router.get("/analytics/system/health", response_model=HealthResponse)
def get_system_health(service: SurgeDataService = Depends(get_service)):
    return {"data": service.get_system_health()}
