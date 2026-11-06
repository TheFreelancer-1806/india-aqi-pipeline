"""Static configuration and environment-driven settings for the AQI pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
SQL_DIR: Path = PROJECT_ROOT / "sql"
DDL_SQL_PATH: Path = SQL_DIR / "01_ddl_schema.sql"
TRANSFORM_SQL_PATH: Path = SQL_DIR / "02_transform_load.sql"
ANALYTICAL_SQL_PATH: Path = SQL_DIR / "03_analytical_queries.sql"
DOCS_DIR: Path = PROJECT_ROOT / "docs"

# ---------------------------------------------------------------------------
# Source API — Open-Meteo Air Quality. No API key, no registration, CC-BY 4.0.
# Docs: https://open-meteo.com/en/docs/air-quality-api
# ---------------------------------------------------------------------------
API_URL: str = "https://air-quality-api.open-meteo.com/v1/air-quality"
API_TIMEZONE: str = "Asia/Kolkata"

#: Requested hourly variables, in Open-Meteo's naming.
HOURLY_METRICS: List[str] = [
    "pm10",
    "pm2_5",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
    "us_aqi",
]

#: Open-Meteo variable name -> our column name.
METRIC_COLUMN_MAP: Dict[str, str] = {
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "carbon_monoxide": "co",
    "us_aqi": "us_aqi",
}

#: Columns of aqi_pipeline.stg_air_quality that the loader populates.
STAGING_COLUMNS: List[str] = [
    "city",
    "latitude",
    "longitude",
    "observation_time",
    "pm2_5",
    "pm10",
    "no2",
    "so2",
    "co",
    "us_aqi",
]

POLLUTANT_COLUMNS: List[str] = ["pm2_5", "pm10", "no2", "so2", "co", "us_aqi"]

# ---------------------------------------------------------------------------
# Cities. "zone" groups them by the airshed that drives their behaviour, which
# is what the findings in docs/FINDINGS.md compare.
# ---------------------------------------------------------------------------
CITIES: Dict[str, Dict[str, Any]] = {
    "Delhi":     {"lat": 28.6139, "lon": 77.2090, "zone": "Indo-Gangetic Plain"},
    "Kolkata":   {"lat": 22.5726, "lon": 88.3639, "zone": "Indo-Gangetic Plain"},
    "Mumbai":    {"lat": 19.0760, "lon": 72.8777, "zone": "Western Coastal"},
    "Chennai":   {"lat": 13.0827, "lon": 80.2707, "zone": "Eastern Coastal"},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "zone": "Deccan Plateau"},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867, "zone": "Deccan Plateau"},
}

CITY_ZONES: Dict[str, str] = {city: meta["zone"] for city, meta in CITIES.items()}

# ---------------------------------------------------------------------------
# Air quality standards, 24-hour averaging period, ug/m3
# ---------------------------------------------------------------------------
#: CPCB National Ambient Air Quality Standard, 24-h PM2.5 (India).
CPCB_PM25_SAFE_LIMIT: float = 60.0
#: WHO 2021 Global Air Quality Guideline, 24-h PM2.5.
WHO_PM25_SAFE_LIMIT: float = 15.0
#: CPCB National AQI 24-h PM2.5 sub-index breakpoints (upper bound -> label).
CPCB_PM25_BREAKPOINTS: List[tuple] = [
    (30.0, "Good"),
    (60.0, "Satisfactory"),
    (90.0, "Moderate"),
    (120.0, "Poor"),
    (250.0, "Very Poor"),
    (float("inf"), "Severe"),
]
#: Z-score above the trailing 30-day baseline that marks an acute episode.
ANOMALY_Z_THRESHOLD: float = 2.0

#: Highest physically plausible hourly concentration (ug/m3). Anything above is
#: treated as a sensor or encoding fault and clipped.
MAX_PLAUSIBLE_CONCENTRATION: float = 2500.0


@dataclass(frozen=True)
class Settings:
    """Pipeline settings resolved from the environment."""

    backfill_days: int = 90
    incremental_days: int = 4
    min_hours_per_day: int = 6
    staging_retention_days: int = 14
    request_timeout_seconds: int = 30
    inter_request_pause_seconds: float = 0.3

    @classmethod
    def from_env(cls) -> "Settings":
        def _int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(f"{name} must be an integer, got {raw!r}") from exc

        settings = cls(
            # Open-Meteo serves at most 92 past days on the air-quality endpoint.
            backfill_days=min(_int("BACKFILL_DAYS", 90), 92),
            incremental_days=_int("INCREMENTAL_DAYS", 4),
            min_hours_per_day=_int("MIN_HOURS_PER_DAY", 6),
            staging_retention_days=_int("STAGING_RETENTION_DAYS", 14),
            request_timeout_seconds=_int("REQUEST_TIMEOUT_SECONDS", 30),
        )
        if not 1 <= settings.min_hours_per_day <= 24:
            raise ValueError("MIN_HOURS_PER_DAY must be between 1 and 24")
        if settings.incremental_days < 1:
            raise ValueError("INCREMENTAL_DAYS must be at least 1")
        return settings
