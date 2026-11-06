"""Shared fixtures. The SQL tests need a real Postgres; they skip without one."""

from __future__ import annotations

import os
from urllib.parse import urlparse

import pytest
from dotenv import load_dotenv
from sqlalchemy.engine import Engine, make_url

from src.db import apply_schema, create_db_engine

load_dotenv()

SCHEMA = "aqi_pipeline"


@pytest.fixture(scope="session")
def pg_engine() -> Engine:
    """Engine against a throwaway Postgres named by TEST_DATABASE_URL.

    The fixture drops and recreates the ``aqi_pipeline`` schema, so it refuses
    any database whose name does not contain "test". That guard is the only
    thing standing between a mistyped environment variable and someone's real
    warehouse.
    """
    url = os.environ.get("TEST_DATABASE_URL")
    if not url or not url.strip():
        pytest.skip(
            "TEST_DATABASE_URL is not set — skipping Postgres integration tests. "
            "Start a local server with scripts/local_pg.ps1 start."
        )

    database = (urlparse(url).path or "").lstrip("/")
    if "test" not in database.lower():
        pytest.fail(
            f"Refusing to run destructive tests against database {database!r}: "
            "TEST_DATABASE_URL must point at a database with 'test' in its name."
        )

    engine = create_db_engine(url)
    safe = make_url(engine.url).render_as_string(hide_password=True)
    print(f"\nSQL integration tests running against {safe}")

    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    apply_schema(engine)

    yield engine
    engine.dispose()


@pytest.fixture
def empty_warehouse(pg_engine: Engine) -> Engine:
    """A schema-present, data-free warehouse for a single test."""
    with pg_engine.begin() as conn:
        conn.exec_driver_sql(
            f"TRUNCATE {SCHEMA}.stg_air_quality, "
            f"{SCHEMA}.clean_daily_aqi, {SCHEMA}.pipeline_runs"
        )
    return pg_engine
