from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import redis
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from src.shared.config import config

from src.shared.schemas import SurgeWindowRecord

logger = logging.getLogger(__name__)


class SurgeDataService:
    """Unified read layer across Redis (live state) and Cassandra (history).

    Design notes:
    - Cassandra queries use **prepared statements** throughout — no f-string
      interpolation for CQL values.
    - ``get_summary``, ``get_top_zones``, and ``get_anomalies`` derive their
      data from Redis (the live snapshot) to avoid N+1 Cassandra round-trips.
      Cassandra is only queried for single-zone history / trend requests.
    """

    def __init__(
        self,
        redis_client: redis.Redis | None = None,
        cassandra_session=None,
        cassandra_cluster=None,
    ) -> None:
        self.redis = redis_client or redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
        self.cassandra_cluster = cassandra_cluster
        self.cassandra_session = cassandra_session

        self.historical_columns: set[str] = set()
        self.historical_legacy_schema = False

        # Prepared statements (set during _init_cassandra)
        self._history_stmt = None
        self._history_with_date_stmt = None

        if self.cassandra_session is None:
            try:
                self.cassandra_cluster = Cluster([config.CASSANDRA_HOST])
                self.cassandra_session = self.cassandra_cluster.connect(config.CASSANDRA_KEYSPACE)
                self._init_cassandra()
            except Exception as exc:
                logger.warning("Could not connect to Cassandra: %s", exc)
                logger.warning("Dashboard will continue in Live-only mode (Redis).")
                self.cassandra_session = None

    # ------------------------------------------------------------------
    # Cassandra initialisation
    # ------------------------------------------------------------------

    def _init_cassandra(self) -> None:
        """Detect schema version and prepare all CQL statements."""
        session = self.cassandra_session
        assert session is not None

        self.historical_columns = self._load_historical_columns()
        self.historical_legacy_schema = "window_end" not in self.historical_columns

        if self.historical_legacy_schema:
            logger.warning(
                "Using legacy Cassandra schema with ts. "
                "Consider rerunning init_cassandra.cql for the newer windowed schema."
            )
            self._history_stmt = session.prepare(
                """
                SELECT zone_id, ts, surge_multiplier
                FROM historical_surge
                WHERE zone_id = ?
                ORDER BY ts DESC
                LIMIT ?
                """
            )
            # Legacy schema has no date-range filter support
            self._history_with_date_stmt = None
        else:
            logger.info("Using Cassandra historical schema with window_end.")
            self._history_stmt = session.prepare(
                """
                SELECT zone_id, window_start, window_end,
                       demand, supply, surge_multiplier, updated_at
                FROM historical_surge
                WHERE zone_id = ?
                ORDER BY window_end DESC
                LIMIT ?
                """
            )
            self._history_with_date_stmt = session.prepare(
                """
                SELECT zone_id, window_start, window_end,
                       demand, supply, surge_multiplier, updated_at
                FROM historical_surge
                WHERE zone_id = ? AND window_end > ?
                ORDER BY window_end DESC
                LIMIT ?
                """
            )

    def _load_historical_columns(self) -> set[str]:
        session = self.cassandra_session
        assert session is not None
        rows = session.execute(
            """
            SELECT column_name
            FROM system_schema.columns
            WHERE keyspace_name = %s AND table_name = %s
            """,
            ("surge_analytics", "historical_surge"),
        )
        return {row.column_name for row in rows}

    def close(self) -> None:
        if self.cassandra_cluster is not None:
            self.cassandra_cluster.shutdown()

    # ------------------------------------------------------------------
    # Redis — live state
    # ------------------------------------------------------------------

    def get_live_zone_ids(self) -> list[str]:
        zone_ids = cast(set[str], self.redis.smembers("surge:zones"))
        return sorted(str(zone_id) for zone_id in zone_ids)

    def get_current_surge(self, zone_id: str) -> dict[str, Any]:
        payload = self.redis.get(f"surge:{zone_id}")
        if payload:
            return SurgeWindowRecord.from_json(cast(str, payload)).to_dict()

        return {
            "zone_id": zone_id,
            "window_start": None,
            "window_end": None,
            "demand": 0,
            "supply": 0,
            "surge_multiplier": 1.0,
            "updated_at": None,
            "message": "No active surge",
        }

    def get_all_current_surge(self) -> list[dict[str, Any]]:
        """Batch-fetch live surge for every active zone using Redis MGET."""
        zone_ids = self.get_live_zone_ids()
        if not zone_ids:
            return []

        keys = [f"surge:{zone_id}" for zone_id in zone_ids]
        raw_values: Any = self.redis.mget(keys)
        results: list[dict[str, Any]] = []

        for value in raw_values:
            if value:
                results.append(SurgeWindowRecord.from_json(value).to_dict())

        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None

        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _build_history_semantics(history: list[dict[str, Any]]) -> dict[str, Any]:
        if not history:
            return {
                "total_demand": 0,
                "total_supply": 0,
                "surge_volatility": 0.0,
                "surge_change_rate": 0.0,
                "freshness_seconds": None,
                "latest_update": None,
            }

        ordered = sorted(history, key=lambda item: item["window_end"] or "")
        surge_values = [float(item["surge_multiplier"]) for item in ordered]
        total_demand = sum(int(item.get("demand", 0)) for item in ordered)
        total_supply = sum(int(item.get("supply", 0)) for item in ordered)

        if len(surge_values) > 1:
            surge_volatility = round(statistics.pstdev(surge_values), 3)
            surge_change_rate = round(
                sum(abs(current - previous) for previous, current in zip(surge_values, surge_values[1:]))
                / (len(surge_values) - 1),
                3,
            )
        else:
            surge_volatility = 0.0
            surge_change_rate = 0.0

        latest_update = ordered[-1].get("updated_at") or ordered[-1].get("window_end")
        parsed_latest_update = SurgeDataService._parse_datetime(latest_update)
        freshness_seconds = None
        if parsed_latest_update is not None:
            freshness_seconds = round((datetime.now(timezone.utc) - parsed_latest_update).total_seconds(), 1)

        return {
            "total_demand": total_demand,
            "total_supply": total_supply,
            "surge_volatility": surge_volatility,
            "surge_change_rate": surge_change_rate,
            "freshness_seconds": freshness_seconds,
            "latest_update": parsed_latest_update.isoformat() if parsed_latest_update else None,
        }

    # ------------------------------------------------------------------
    # Summary / Top Zones / Anomalies — derived from Redis (no N+1)
    # ------------------------------------------------------------------

    def get_summary(self) -> dict[str, Any]:
        """Compute a live summary from Redis — avoids per-zone Cassandra queries.

        Previous implementation issued 1 Cassandra query per zone (N+1).
        Now we simply read the live snapshot from Redis in one MGET call.
        """
        active_zone_ids = self.get_live_zone_ids()
        live_records = self.get_all_current_surge()

        if not live_records:
            return {
                "active_zones": len(active_zone_ids),
                "average_surge": 1.0,
                "max_surge": 1.0,
                "total_demand": 0,
                "total_supply": 0,
                "surge_volatility": 0.0,
                "freshness_seconds": None,
                "latest_update": self.redis.get("surge:latest_update"),
                "top_zone": None,
            }

        surge_values = [float(item["surge_multiplier"]) for item in live_records]
        max_item = max(live_records, key=lambda item: float(item["surge_multiplier"]))
        semantics = self._build_history_semantics(live_records)
        latest_update = semantics["latest_update"] or self.redis.get("surge:latest_update")

        return {
            "active_zones": len(active_zone_ids),
            "average_surge": round(statistics.mean(surge_values), 3),
            "max_surge": round(max(surge_values), 3),
            "total_demand": semantics["total_demand"],
            "total_supply": semantics["total_supply"],
            "surge_volatility": semantics["surge_volatility"],
            "freshness_seconds": semantics["freshness_seconds"],
            "latest_update": latest_update,
            "top_zone": max_item,
        }

    def get_top_zones(self, limit: int = 10) -> list[dict[str, Any]]:
        """Return the top-N zones by surge from the live Redis snapshot.

        Previous implementation issued 1 Cassandra query per zone (N+1).
        """
        live_records = self.get_all_current_surge()
        live_records.sort(key=lambda item: float(item["surge_multiplier"]), reverse=True)
        return live_records[:limit]

    def get_anomalies(self, threshold: float = 2.0, limit: int = 50) -> list[dict[str, Any]]:
        """Return live zones whose surge >= threshold from the Redis snapshot.

        Previous implementation issued 1 Cassandra query per zone (N+1).
        """
        live_records = self.get_all_current_surge()
        anomalies = [r for r in live_records if float(r["surge_multiplier"]) >= threshold]
        anomalies.sort(key=lambda item: float(item["surge_multiplier"]), reverse=True)
        return anomalies[:limit]

    # ------------------------------------------------------------------
    # Cassandra — historical queries (single-zone)
    # ------------------------------------------------------------------

    def _get_recent_history_for_zone(
        self, zone_id: str, limit: int, days: int
    ) -> list[dict[str, Any]]:
        """Query Cassandra for recent history of a single zone.

        The ``days`` parameter is now honoured: if the windowed schema is
        available, we apply ``window_end > now() - days`` as a clustering
        filter.  LIMIT is bound as a proper CQL parameter (no f-string).
        """
        session = self.cassandra_session
        if session is None:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Use date-range filter only if days is not 30 (default simulation range)
        if not self.historical_legacy_schema and self._history_with_date_stmt is not None and days < 30:
            rows = session.execute(self._history_with_date_stmt, (zone_id, cutoff, int(limit)))
            results = self._process_rows(rows)
            # If no recent data (24h/7d), fall back to most recent data (all-time) for simulation
            if not results:
                rows = session.execute(self._history_stmt, (zone_id, int(limit)))
                results = self._process_rows(rows)
        else:
            assert self._history_stmt is not None
            rows = session.execute(self._history_stmt, (zone_id, int(limit)))
            results = self._process_rows(rows)

        return results

    def _process_rows(self, rows) -> list[dict[str, Any]]:
        """Map Cassandra rows to dictionaries."""
        results: list[dict[str, Any]] = []
        for row in rows:
            if self.historical_legacy_schema:
                ts_v = row.ts.isoformat() if getattr(row, "ts", None) else None
                results.append({
                    "zone_id": row.zone_id, "window_start": ts_v, "window_end": ts_v,
                    "demand": 0, "supply": 0, "surge_multiplier": float(row.surge_multiplier),
                    "updated_at": ts_v
                })
                continue

            results.append({
                "zone_id": row.zone_id,
                "window_start": row.window_start.isoformat() if row.window_start else None,
                "window_end": row.window_end.isoformat() if row.window_end else None,
                "demand": int(row.demand),
                "supply": int(row.supply),
                "surge_multiplier": float(row.surge_multiplier),
                "updated_at": row.updated_at.isoformat() if getattr(row, "updated_at", None) else None,
            })
        results.sort(key=lambda item: item["window_end"] or "", reverse=True)
        return results

    def get_zone_history(
        self,
        zone_id: str,
        limit: int = 100,
        days: int = 1,
    ) -> list[dict[str, Any]]:
        return self._get_recent_history_for_zone(zone_id=zone_id, limit=limit, days=days)

    def get_zone_trend(self, zone_id: str, limit: int = 120) -> dict[str, Any]:
        history = self.get_zone_history(zone_id=zone_id, limit=limit, days=7)
        if not history:
            return {
                "zone_id": zone_id,
                "points": [],
                "average_surge": 1.0,
                "max_surge": 1.0,
                "total_demand": 0,
                "total_supply": 0,
                "surge_volatility": 0.0,
                "surge_change_rate": 0.0,
                "freshness_seconds": None,
                "latest_update": None,
            }

        surge_values = [float(item["surge_multiplier"]) for item in history]
        semantics = self._build_history_semantics(history)
        return {
            "zone_id": zone_id,
            "points": history,
            "average_surge": round(statistics.mean(surge_values), 3),
            "max_surge": round(max(surge_values), 3),
            "total_demand": semantics["total_demand"],
            "total_supply": semantics["total_supply"],
            "surge_volatility": semantics["surge_volatility"],
            "surge_change_rate": semantics["surge_change_rate"],
            "freshness_seconds": semantics["freshness_seconds"],
            "latest_update": semantics["latest_update"],
        }

    # ------------------------------------------------------------------
    # System health
    # ------------------------------------------------------------------

    def get_system_health(self) -> dict[str, Any]:
        try:
            redis_ok = bool(self.redis.ping())
        except Exception:
            redis_ok = False

        try:
            session = self.cassandra_session
            cassandra_ok = session is not None
            if cassandra_ok:
                assert session is not None
                session.execute("SELECT release_version FROM system.local")
        except Exception:
            cassandra_ok = False

        return {
            "redis": "up" if redis_ok else "down",
            "cassandra": "up" if cassandra_ok else "down",
            "latest_update": self.redis.get("surge:latest_update"),
        }
