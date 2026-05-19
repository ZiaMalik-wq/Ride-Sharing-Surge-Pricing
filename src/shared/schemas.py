from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        # Normalise existing datetimes to UTC-aware
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if value is None:
        raise ValueError("Datetime value is required")

    raw = str(value).strip()

    # Detect whether the source string carried an explicit UTC indicator.
    # Any string ending with 'Z' or containing a '+'/'-' offset is treated as
    # UTC once the marker is stripped (ISO-8601 convention).
    is_utc = raw.endswith("Z") or "+" in raw[10:] or (raw.count("-") > 2)

    # Normalise to "YYYY-MM-DD HH:MM:SS[.ffffff]" for strptime
    s = raw.replace("T", " ").replace("Z", "")
    # Strip a trailing UTC offset like '+00:00' if present
    if "+" in s[10:]:
        s = s[: s.index("+", 10)]

    try:
        if "." in s:
            # Handle variable sub-second precision by padding/truncating to 6 digits
            base, micros = s.split(".")
            micros = (micros + "000000")[:6]
            s = f"{base}.{micros}"
            parsed = datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
        else:
            parsed = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        raise ValueError(f"Invalid datetime value: {value!r}") from e

    # Attach UTC timezone so all datetimes in the pipeline are tz-aware
    if is_utc or parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed


def _format_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


@dataclass(slots=True)
class RideEvent:
    event_id: str = field(default_factory=lambda: str(uuid4()))
    event_type: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    zone_id: str = ""
    user_id: str | None = None
    driver_id: str | None = None

    @classmethod
    def ride_request(cls, *, timestamp: Any, zone_id: str, user_id: str) -> "RideEvent":
        return cls(event_type="ride_request", timestamp=_parse_datetime(timestamp), zone_id=str(zone_id), user_id=str(user_id))

    @classmethod
    def driver_available(cls, *, timestamp: Any, zone_id: str, driver_id: str) -> "RideEvent":
        return cls(event_type="driver_available", timestamp=_parse_datetime(timestamp), zone_id=str(zone_id), driver_id=str(driver_id))

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": _format_datetime(self.timestamp),
            "zone_id": self.zone_id,
        }

        if self.user_id is not None:
            payload["user_id"] = self.user_id
        if self.driver_id is not None:
            payload["driver_id"] = self.driver_id

        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(slots=True)
class SurgeWindowRecord:
    zone_id: str
    window_start: datetime
    window_end: datetime
    demand: int = 0
    supply: int = 0
    surge_multiplier: float = 1.0
    updated_at: datetime | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SurgeWindowRecord":
        window_end = payload.get("window_end") or payload.get("ts")
        window_start = payload.get("window_start") or window_end
        updated_at = payload.get("updated_at") or window_end

        if window_start is None or window_end is None:
            raise ValueError("window_start and window_end are required")

        return cls(
            zone_id=str(payload["zone_id"]),
            window_start=_parse_datetime(window_start),
            window_end=_parse_datetime(window_end),
            demand=int(payload.get("demand", 0)),
            supply=int(payload.get("supply", 0)),
            surge_multiplier=float(payload.get("surge_multiplier", 1.0)),
            updated_at=_parse_datetime(updated_at) if updated_at is not None else None,
        )

    @classmethod
    def from_json(cls, payload: str) -> "SurgeWindowRecord":
        return cls.from_dict(json.loads(payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "zone_id": self.zone_id,
            "window_start": _format_datetime(self.window_start),
            "window_end": _format_datetime(self.window_end),
            "demand": self.demand,
            "supply": self.supply,
            "surge_multiplier": self.surge_multiplier,
            "updated_at": _format_datetime(self.updated_at),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())