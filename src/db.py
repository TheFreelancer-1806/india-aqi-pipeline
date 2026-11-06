"""Database access: engine construction, SQL file execution and run auditing."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from src.config import DDL_SQL_PATH, Settings

logger = logging.getLogger(__name__)

SCHEMA = "aqi_pipeline"


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
def normalize_database_url(url: str) -> str:
    """Normalize a connection string to an explicit SQLAlchemy psycopg2 URL.

    Hosted providers hand out ``postgres://`` URLs, which SQLAlchemy 2.x no
    longer accepts as a dialect name.
    """
    url = url.strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://") :]
    return url


def get_database_url(env_var: str = "DATABASE_URL") -> str:
    """Read and normalize a connection string from the environment."""
    raw = os.environ.get(env_var)
    if not raw or not raw.strip():
        raise EnvironmentError(
            f"{env_var} is not set. Copy .env.example to .env and fill it in, "
            f"or set it as a GitHub Actions secret / Streamlit secret."
        )
    return normalize_database_url(raw)


def create_db_engine(url: Optional[str] = None, env_var: str = "DATABASE_URL") -> Engine:
    """Build a pooled SQLAlchemy engine against Postgres."""
    resolved = normalize_database_url(url) if url else get_database_url(env_var)
    return create_engine(
        resolved,
        pool_pre_ping=True,   # serverless Postgres idles connections out
        pool_recycle=300,
        future=True,
    )


# ---------------------------------------------------------------------------
# SQL files
# ---------------------------------------------------------------------------
def run_sql_file(conn: Connection, path: Path) -> None:
    """Execute a multi-statement .sql file verbatim.

    ``exec_driver_sql`` hands the text straight to psycopg2, so ``::`` casts and
    ``:`` inside string literals are not mistaken for bind parameters.
    """
    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        raise ValueError(f"SQL file is empty: {path}")
    conn.exec_driver_sql(sql)


def apply_schema(engine: Engine) -> None:
    """Create the schema, tables, indexes and views if they are not there."""
    with engine.begin() as conn:
        run_sql_file(conn, DDL_SQL_PATH)
    logger.info("Schema ensured: %s (staging -> clean -> mart).", SCHEMA)


def apply_session_settings(conn: Connection, settings: Settings) -> None:
    """Publish pipeline knobs as session GUCs for the transform SQL to read.

    SET LOCAL scopes them to the surrounding transaction, which keeps
    02_transform_load.sql free of string interpolation.
    """
    conn.exec_driver_sql(
        f"SET LOCAL aqi.min_hours_per_day = '{int(settings.min_hours_per_day)}'"
    )
    conn.exec_driver_sql(
        f"SET LOCAL aqi.staging_retention_days = '{int(settings.staging_retention_days)}'"
    )


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert psycopg2 ``Decimal`` columns to float.

    Postgres ``NUMERIC`` arrives as ``decimal.Decimal``, which lands in pandas as
    object dtype — rounding, plotting and aggregation all break on it.
    """
    out = df.copy()
    for column in out.columns:
        if out[column].dtype != "object":
            continue

        values = out[column].dropna()
        if values.empty:
            continue
        # Booleans are numeric to pandas but not to us: coercing them would turn
        # is_cpcb_breach into 1.0 / 0.0 and lose the type.
        if values.map(lambda v: isinstance(v, bool)).all():
            continue

        converted = pd.to_numeric(out[column], errors="coerce")
        # Accept only a lossless conversion, so dates and text stay as they are.
        if converted.notna().sum() == len(values):
            out[column] = converted.astype(float)
    return out


def read_sql_frame(sql: str, connectable) -> pd.DataFrame:
    """Read a query into a DataFrame with Postgres numerics coerced to float."""
    return coerce_numeric(pd.read_sql_query(sql, connectable))


def count_clean_rows(conn: Connection) -> int:
    """Row count of the clean daily layer."""
    result = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.clean_daily_aqi")).scalar()
    return int(result or 0)


# ---------------------------------------------------------------------------
# Run auditing
#
# These use their own short transactions on purpose. If the load transaction
# rolls back, the run log must still record the failure.
# ---------------------------------------------------------------------------
def start_run(engine: Engine, mode: str, past_days: int, cities_requested: int) -> int:
    """Open a row in pipeline_runs and return its id."""
    with engine.begin() as conn:
        run_id = conn.execute(
            text(
                f"""
                INSERT INTO {SCHEMA}.pipeline_runs
                    (status, mode, past_days, cities_requested)
                VALUES ('running', :mode, :past_days, :cities_requested)
                RETURNING run_id
                """
            ),
            {"mode": mode, "past_days": past_days, "cities_requested": cities_requested},
        ).scalar_one()
    return int(run_id)


def finish_run(
    engine: Engine,
    run_id: int,
    status: str,
    cities_succeeded: Optional[int] = None,
    rows_staged: Optional[int] = None,
    clean_rows_total: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Close out a pipeline_runs row, computing its duration in the database."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {SCHEMA}.pipeline_runs
                   SET finished_at      = CURRENT_TIMESTAMP,
                       status           = :status,
                       cities_succeeded = :cities_succeeded,
                       rows_staged      = :rows_staged,
                       clean_rows_total = :clean_rows_total,
                       duration_seconds = ROUND(
                           EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - started_at))::NUMERIC, 2
                       ),
                       message          = :message
                 WHERE run_id = :run_id
                """
            ),
            {
                "status": status,
                "cities_succeeded": cities_succeeded,
                "rows_staged": rows_staged,
                "clean_rows_total": clean_rows_total,
                "message": (message or "")[:2000] or None,
                "run_id": run_id,
            },
        )
