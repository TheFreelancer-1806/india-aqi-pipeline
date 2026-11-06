"""Extract -> clean -> stage -> transform, for one scheduled pipeline run.

Usage
-----
    python -m src.etl                 # incremental (or first-run backfill)
    python -m src.etl --backfill      # force the full backfill window
    python -m src.etl --past-days 10  # explicit extraction window
    python -m src.etl --dry-run       # hit the API only, touch no database
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from sqlalchemy.engine import Connection, Engine
from urllib3.util.retry import Retry

from src import config
from src.config import (
    API_TIMEZONE,
    API_URL,
    CITIES,
    HOURLY_METRICS,
    MAX_PLAUSIBLE_CONCENTRATION,
    METRIC_COLUMN_MAP,
    POLLUTANT_COLUMNS,
    STAGING_COLUMNS,
    TRANSFORM_SQL_PATH,
    Settings,
)
from src.db import (
    SCHEMA,
    apply_schema,
    apply_session_settings,
    count_clean_rows,
    create_db_engine,
    finish_run,
    run_sql_file,
    start_run,
)

logger = logging.getLogger("aqi.etl")


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def build_http_session() -> requests.Session:
    """HTTP session with exponential backoff on throttling and 5xx."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "india-aqi-pipeline/1.0 (+github actions)"})
    return session


def _hourly_to_frame(
    city: str, lat: float, lon: float, hourly: Dict[str, list]
) -> pd.DataFrame:
    """Build a flat frame from Open-Meteo's column-oriented ``hourly`` block.

    Series are aligned to ``hourly.time`` and padded, so a variable the API
    omits or truncates becomes NULL instead of raising a length mismatch.
    """
    times = hourly.get("time") or []
    n = len(times)

    frame: Dict[str, object] = {
        "city": city,
        "latitude": lat,
        "longitude": lon,
        "observation_time": times,
    }
    for api_name, column in METRIC_COLUMN_MAP.items():
        series = hourly.get(api_name) or []
        if len(series) < n:
            series = list(series) + [None] * (n - len(series))
        frame[column] = series[:n]

    return pd.DataFrame(frame, columns=STAGING_COLUMNS)


def fetch_city_aqi(
    city: str,
    lat: float,
    lon: float,
    past_days: int,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> pd.DataFrame:
    """Extract hourly air quality for one city from Open-Meteo."""
    http = session or build_http_session()
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_METRICS),
        "timezone": API_TIMEZONE,
        "past_days": past_days,
        "forecast_days": 1,  # supplies today's elapsed hours; future hours are dropped
    }
    response = http.get(API_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if payload.get("error"):
        raise ValueError(f"Open-Meteo reported an error for {city}: {payload.get('reason')}")

    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or "time" not in hourly:
        raise ValueError(f"Malformed payload for {city}: missing 'hourly.time'")

    return _hourly_to_frame(city, lat, lon, hourly)


# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
def _coerce_physical_range(series: pd.Series) -> pd.Series:
    """Coerce to numeric and blank out physically impossible readings.

    Out-of-range values become NaN rather than being clamped to a bound: a
    negative reading is a sensor or encoding fault, and clamping it to 0 would
    pull the daily mean down as if clean air had been measured. As NaN it is
    excluded from the average instead.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.where(
        (numeric >= 0) & (numeric <= MAX_PLAUSIBLE_CONCENTRATION)
    )


def clean_dataframe(
    df: pd.DataFrame, reference_now: Optional[pd.Timestamp] = None
) -> pd.DataFrame:
    """Validate and clean one city's hourly extract.

    Steps: parse timestamps, drop forecast hours that have not happened yet,
    drop rows carrying no particulate reading at all, blank out impossible
    values, and de-duplicate on (city, observation_time) keeping the newest.
    """
    if df.empty:
        return df.reindex(columns=STAGING_COLUMNS)

    cleaned = df.copy()

    # Open-Meteo always returns ISO-8601. Pinning the format avoids pandas
    # falling back to per-element dateutil parsing on ~13k rows, and anything
    # that is not ISO-8601 becomes NaT and is dropped on the next line.
    cleaned["observation_time"] = pd.to_datetime(
        cleaned["observation_time"], format="ISO8601", errors="coerce"
    )
    cleaned = cleaned.dropna(subset=["observation_time"])

    # Open-Meteo returns IST wall-clock strings (timezone=Asia/Kolkata), so the
    # cutoff is "now" in IST with the offset stripped.
    if reference_now is None:
        reference_now = pd.Timestamp.now(tz=API_TIMEZONE).tz_localize(None)
    cleaned = cleaned[cleaned["observation_time"] <= reference_now]

    for column in POLLUTANT_COLUMNS:
        cleaned[column] = _coerce_physical_range(cleaned[column])

    # us_aqi is an integer index, not a concentration.
    cleaned["us_aqi"] = cleaned["us_aqi"].round()

    # A row with neither PM2.5 nor PM10 carries nothing this pipeline reports on.
    cleaned = cleaned.dropna(subset=["pm2_5", "pm10"], how="all")

    cleaned = cleaned.drop_duplicates(subset=["city", "observation_time"], keep="last")

    return (
        cleaned.sort_values(["city", "observation_time"])
        .reset_index(drop=True)
        .reindex(columns=STAGING_COLUMNS)
    )


def extract_and_clean_all(
    past_days: int, settings: Settings
) -> tuple[pd.DataFrame, List[str], Dict[str, str]]:
    """Run extract + clean across every configured city.

    A city that fails is logged and skipped; the run continues so one bad
    endpoint cannot cost the whole day's data.
    """
    session = build_http_session()
    frames: List[pd.DataFrame] = []
    succeeded: List[str] = []
    failures: Dict[str, str] = {}

    for city, meta in CITIES.items():
        try:
            raw = fetch_city_aqi(
                city=city,
                lat=meta["lat"],
                lon=meta["lon"],
                past_days=past_days,
                session=session,
                timeout=settings.request_timeout_seconds,
            )
            cleaned = clean_dataframe(raw)
            if cleaned.empty:
                raise ValueError("no valid hourly rows survived cleaning")
            frames.append(cleaned)
            succeeded.append(city)
            logger.info(
                "[%s] %d raw -> %d valid hourly rows (%s .. %s)",
                city,
                len(raw),
                len(cleaned),
                cleaned["observation_time"].min(),
                cleaned["observation_time"].max(),
            )
        except Exception as exc:  # noqa: BLE001 - one city must not sink the run
            failures[city] = f"{type(exc).__name__}: {exc}"
            logger.error("[%s] extraction failed: %s", city, exc)
        time.sleep(settings.inter_request_pause_seconds)

    combined = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=STAGING_COLUMNS)
    )
    return combined, succeeded, failures


# ---------------------------------------------------------------------------
# Load + transform
# ---------------------------------------------------------------------------
def _prepare_for_load(df: pd.DataFrame) -> pd.DataFrame:
    """Make the frame safe for psycopg2: us_aqi becomes a Python int or None.

    Kept as an explicit object-dtype column: a float column would reach the
    INTEGER target as ``198.0`` and rely on Postgres' numeric-to-int assignment
    cast, and a pandas nullable Int64 column would hand psycopg2 ``pd.NA``.
    """
    out = df.copy()
    out["us_aqi"] = pd.Series(
        [None if pd.isna(value) else int(value) for value in out["us_aqi"]],
        index=out.index,
        dtype=object,
    )
    return out


def load_staging(conn: Connection, df: pd.DataFrame) -> int:
    """Append cleaned hourly rows to the staging table."""
    if df.empty:
        return 0
    _prepare_for_load(df).to_sql(
        name="stg_air_quality",
        schema=SCHEMA,
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    return len(df)


def transform_staging_to_clean(conn: Connection, settings: Settings) -> None:
    """Run the staging -> clean upsert and the staging retention prune."""
    apply_session_settings(conn, settings)
    run_sql_file(conn, TRANSFORM_SQL_PATH)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def decide_window(existing_clean_rows: int, settings: Settings) -> tuple[int, str]:
    """Backfill on an empty warehouse, otherwise take a small overlapping window."""
    if existing_clean_rows == 0:
        return settings.backfill_days, "backfill"
    return settings.incremental_days, "incremental"


def run_pipeline(
    engine: Optional[Engine] = None,
    past_days_override: Optional[int] = None,
    force_backfill: bool = False,
    settings: Optional[Settings] = None,
) -> Dict[str, object]:
    """Execute one full pipeline run and return a summary."""
    settings = settings or Settings.from_env()
    engine = engine or create_db_engine()
    started = time.perf_counter()

    apply_schema(engine)

    with engine.connect() as conn:
        existing_rows = count_clean_rows(conn)

    if past_days_override is not None:
        past_days, mode = past_days_override, "manual"
    elif force_backfill:
        past_days, mode = settings.backfill_days, "backfill"
    else:
        past_days, mode = decide_window(existing_rows, settings)

    logger.info(
        "clean_daily_aqi holds %d rows -> mode=%s, past_days=%d, cities=%d",
        existing_rows,
        mode,
        past_days,
        len(CITIES),
    )

    run_id = start_run(engine, mode=mode, past_days=past_days, cities_requested=len(CITIES))

    try:
        combined, succeeded, failures = extract_and_clean_all(past_days, settings)

        if combined.empty:
            raise RuntimeError(
                "no city returned usable data; "
                + ("; ".join(f"{c}: {m}" for c, m in failures.items()) or "no detail")
            )

        # One transaction: staging insert and the transform either both land or
        # neither does, so the clean layer is never built on a partial load.
        with engine.begin() as conn:
            rows_staged = load_staging(conn, combined)
            logger.info("Staged %d hourly rows into %s.stg_air_quality.", rows_staged, SCHEMA)
            transform_staging_to_clean(conn, settings)
            clean_total = count_clean_rows(conn)

        status = "success" if not failures else "partial"
        message = (
            "; ".join(f"{c}: {m}" for c, m in failures.items())
            if failures
            else "all cities extracted"
        )
        finish_run(
            engine,
            run_id,
            status=status,
            cities_succeeded=len(succeeded),
            rows_staged=rows_staged,
            clean_rows_total=clean_total,
            message=message,
        )

        elapsed = time.perf_counter() - started
        logger.info(
            "Run %d %s in %.1fs | staged=%d | clean_daily_aqi=%d (+%d) | cities %d/%d",
            run_id,
            status,
            elapsed,
            rows_staged,
            clean_total,
            clean_total - existing_rows,
            len(succeeded),
            len(CITIES),
        )
        return {
            "run_id": run_id,
            "status": status,
            "mode": mode,
            "past_days": past_days,
            "rows_staged": rows_staged,
            "clean_rows_total": clean_total,
            "clean_rows_added": clean_total - existing_rows,
            "cities_succeeded": succeeded,
            "cities_failed": failures,
            "duration_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        finish_run(
            engine,
            run_id,
            status="failed",
            cities_succeeded=0,
            message=f"{type(exc).__name__}: {exc}",
        )
        logger.exception("Run %d failed.", run_id)
        raise


def dry_run(past_days: int, settings: Settings) -> Dict[str, object]:
    """Exercise extract + clean only. Useful without a database."""
    combined, succeeded, failures = extract_and_clean_all(past_days, settings)
    if combined.empty:
        raise RuntimeError("dry run produced no rows")

    daily = (
        combined.assign(obs_date=combined["observation_time"].dt.date)
        .groupby(["city", "obs_date"], as_index=False)
        .agg(hours=("pm2_5", "count"), avg_pm2_5=("pm2_5", "mean"))
    )
    logger.info(
        "Dry run: %d hourly rows, %d city-days, %d/%d cities ok.",
        len(combined),
        len(daily),
        len(succeeded),
        len(CITIES),
    )
    print(
        daily.sort_values(["city", "obs_date"])
        .tail(20)
        .to_string(index=False, float_format=lambda v: f"{v:.2f}")
    )
    return {
        "hourly_rows": len(combined),
        "city_days": len(daily),
        "cities_succeeded": succeeded,
        "cities_failed": failures,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="India metro AQI ETL pipeline.")
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--past-days",
        type=int,
        help="Explicit extraction window in days (Open-Meteo allows up to 92).",
    )
    window.add_argument(
        "--backfill",
        action="store_true",
        help="Force the full BACKFILL_DAYS window even if the warehouse is populated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and clean only; do not connect to the database.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    configure_logging(args.verbose)
    settings = Settings.from_env()

    if args.dry_run:
        dry_run(args.past_days or settings.incremental_days, settings)
        return 0

    summary = run_pipeline(
        past_days_override=args.past_days,
        force_backfill=args.backfill,
        settings=settings,
    )
    return 0 if summary["status"] in ("success", "partial") else 1


if __name__ == "__main__":
    raise SystemExit(main())
