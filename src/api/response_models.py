"""Pydantic response models for the Surge Pricing API.

These models power FastAPI's auto-generated OpenAPI/Swagger docs and
provide runtime validation of outgoing responses.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class SurgeRecord(BaseModel):
    zone_id: str
    window_start: str | None = None
    window_end: str | None = None
    demand: int = 0
    supply: int = 0
    surge_multiplier: float = 1.0
    updated_at: str | None = None
    message: str | None = None


class SurgeListResponse(BaseModel):
    data: list[SurgeRecord]


class SurgeDetailResponse(BaseModel):
    data: SurgeRecord


class ZoneListResponse(BaseModel):
    data: list[str]


class SummaryPayload(BaseModel):
    active_zones: int = 0
    average_surge: float = 1.0
    max_surge: float = 1.0
    total_demand: int = 0
    total_supply: int = 0
    surge_volatility: float = 0.0
    freshness_seconds: float | None = None
    latest_update: str | None = None
    top_zone: SurgeRecord | None = None


class SummaryResponse(BaseModel):
    data: SummaryPayload


class HistoryResponse(BaseModel):
    data: list[SurgeRecord]


class TrendPayload(BaseModel):
    zone_id: str
    points: list[SurgeRecord] = []
    average_surge: float = 1.0
    max_surge: float = 1.0
    total_demand: int = 0
    total_supply: int = 0
    surge_volatility: float = 0.0
    surge_change_rate: float = 0.0
    freshness_seconds: float | None = None
    latest_update: str | None = None


class TrendResponse(BaseModel):
    data: TrendPayload


class HealthPayload(BaseModel):
    redis: str = "down"
    cassandra: str = "down"
    latest_update: str | None = None


class HealthResponse(BaseModel):
    data: HealthPayload


class GenericDataResponse(BaseModel):
    data: Any = None
