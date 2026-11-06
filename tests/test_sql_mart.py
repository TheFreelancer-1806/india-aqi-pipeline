"""Integration tests for the SQL layer, against a real PostgreSQL server.

These are the tests that matter most in this project: the rolling averages, LAG
deltas, completeness gates and anomaly flags all live in SQL, so unit-testing
the Python would leave the actual logic uncovered.

They skip unless TEST_DATABASE_URL points at a throwaway database. Locally:

    .\\scripts\\local_pg.ps1 start
    $env:TEST_DATABASE_URL = 'postgresql://postgres@127.0.0.1:55432/aqi_test'
    pytest tests/test_sql_mart.py -v

In CI a postgres service container provides it (.github/workflows/ci.yml).
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Optional, Sequence

import pandas as pd
import pytest
from sqlalchemy.engine import Engine

from src.config import STAGING_COLUMNS, Settings
from src.db import SCHEMA, finish_run, read_sql_frame, start_run
from src.etl import load_staging, transform_staging_to_clean

pytestmark = pytest.mark.usefixtures("empty_warehouse")

BASE_DATE = date(2026, 1, 1)
SETTINGS = Settings(min_hours_per_day=6, staging_retention_days=14)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _hourly_rows(
    city: str,
    day: date,
    pm2_5: float,
    hours: int = 24,
    pm10: Optional[float] = None,
    pm10_hours: Optional[int] = None,
    no2: Optional[float] = 30.0,
) -> list[dict]:
    """Build `hours` identical hourly observations for one city-day.

    Identical values make the expected daily mean exact, so a test failure is
    unambiguously the SQL and not floating point.
    """
    rows = []
    pm10_value = pm2_5 * 2 if pm10 is None else pm10
    pm10_limit = hours if pm10_hours is None else pm10_hours
    for hour in range(hours):
        rows.append(
            {
                "city": city,
                "latitude": 28.6139,
                "longitude": 77.2090,
                "observation_time": pd.Timestamp(day) + pd.Timedelta(hours=hour),
                "pm2_5": round(pm2_5, 2),
                "pm10": round(pm10_value, 2) if hour < pm10_limit else None,
                "no2": no2,
                "so2": 10.0,
                "co": 500.0,
                "us_aqi": int(round(pm2_5 * 2)),
            }
        )
    return rows


def stage(engine: Engine, rows: Iterable[dict]) -> int:
    frame = pd.DataFrame(list(rows), columns=STAGING_COLUMNS)
    with engine.begin() as conn:
        return load_staging(conn, frame)


def transform(engine: Engine, settings: Settings = SETTINGS) -> None:
    with engine.begin() as conn:
        transform_staging_to_clean(conn, settings)


def stage_series(
    engine: Engine,
    city: str,
    values: Sequence[Optional[float]],
    start: date = BASE_DATE,
    hours: int = 24,
) -> None:
    """Stage one day per value, starting at `start`. A None value skips that day."""
    rows: list[dict] = []
    for offset, value in enumerate(values):
        if value is None:
            continue
        rows.extend(_hourly_rows(city, start + timedelta(days=offset), value, hours=hours))
    stage(engine, rows)


def clean_rows(engine: Engine, city: str) -> pd.DataFrame:
    return read_sql_frame(
        f"SELECT * FROM {SCHEMA}.clean_daily_aqi "
        f"WHERE city = '{city}' ORDER BY obs_date",
        engine,
    )


def mart_rows(engine: Engine, city: str) -> pd.DataFrame:
    return read_sql_frame(
        f"SELECT * FROM {SCHEMA}.mart_aqi_analytics "
        f"WHERE city = '{city}' ORDER BY obs_date",
        engine,
    )


# ---------------------------------------------------------------------------
# staging -> clean
# ---------------------------------------------------------------------------
def test_daily_grain_aggregates_hourly_rows(empty_warehouse: Engine):
    staged = stage(empty_warehouse, _hourly_rows("Testville", BASE_DATE, 80.0))
    assert staged == 24

    transform(empty_warehouse)
    clean = clean_rows(empty_warehouse, "Testville")

    assert len(clean) == 1
    row = clean.iloc[0]
    assert row["obs_date"] == BASE_DATE
    assert row["avg_pm2_5"] == pytest.approx(80.0)
    assert row["min_pm2_5"] == pytest.approx(80.0)
    assert row["max_pm2_5"] == pytest.approx(80.0)
    assert row["avg_pm10"] == pytest.approx(160.0)
    assert row["valid_hours_count"] == 24


def test_min_max_reflect_spread_within_the_day(empty_warehouse: Engine):
    rows = []
    for hour, value in enumerate([40.0] * 12 + [120.0] * 12):
        rows.extend(_hourly_rows("Spread", BASE_DATE, value, hours=1))
        rows[-1]["observation_time"] = pd.Timestamp(BASE_DATE) + pd.Timedelta(hours=hour)
    stage(empty_warehouse, rows)
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "Spread").iloc[0]
    assert row["min_pm2_5"] == pytest.approx(40.0)
    assert row["max_pm2_5"] == pytest.approx(120.0)
    assert row["avg_pm2_5"] == pytest.approx(80.0)


def test_day_below_min_hours_is_not_promoted(empty_warehouse: Engine):
    stage(empty_warehouse, _hourly_rows("Sparse", BASE_DATE, 90.0, hours=3))
    stage(empty_warehouse, _hourly_rows("Sparse", BASE_DATE + timedelta(days=1), 90.0, hours=6))
    transform(empty_warehouse)

    clean = clean_rows(empty_warehouse, "Sparse")
    assert len(clean) == 1, "a 3-hour day must not reach the clean layer"
    assert clean.iloc[0]["obs_date"] == BASE_DATE + timedelta(days=1)
    assert clean.iloc[0]["valid_hours_count"] == 6


def test_transform_is_idempotent(empty_warehouse: Engine):
    stage(empty_warehouse, _hourly_rows("Idem", BASE_DATE, 70.0))
    transform(empty_warehouse)
    first = clean_rows(empty_warehouse, "Idem")

    transform(empty_warehouse)
    transform(empty_warehouse)
    third = clean_rows(empty_warehouse, "Idem")

    assert len(third) == 1
    pd.testing.assert_frame_equal(
        first.drop(columns=["updated_at"]), third.drop(columns=["updated_at"])
    )


def test_duplicate_hour_keeps_most_recent_ingest(empty_warehouse: Engine):
    """The API revises recent hours; the newest ingest must win."""
    stage(empty_warehouse, _hourly_rows("Revised", BASE_DATE, 100.0))
    stage(empty_warehouse, _hourly_rows("Revised", BASE_DATE, 200.0))  # same hours, later
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "Revised").iloc[0]
    assert row["avg_pm2_5"] == pytest.approx(200.0)
    assert row["valid_hours_count"] == 24, "dedup must not inflate the hour count"


def test_partial_day_cannot_overwrite_a_complete_day(empty_warehouse: Engine):
    """The regression guard in the ON CONFLICT clause.

    Once staging has been pruned, a later run can see only a slice of an old
    day. Without the guard, that slice would replace a full 24-hour average.
    """
    stage(empty_warehouse, _hourly_rows("Guarded", BASE_DATE, 150.0))
    transform(empty_warehouse)
    assert clean_rows(empty_warehouse, "Guarded").iloc[0]["valid_hours_count"] == 24

    with empty_warehouse.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {SCHEMA}.stg_air_quality")
    stage(empty_warehouse, _hourly_rows("Guarded", BASE_DATE, 10.0, hours=8))
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "Guarded").iloc[0]
    assert row["valid_hours_count"] == 24
    assert row["avg_pm2_5"] == pytest.approx(150.0), "partial day overwrote a complete one"


def test_more_complete_day_does_overwrite(empty_warehouse: Engine):
    """The mirror of the guard: a better day must still be allowed through."""
    stage(empty_warehouse, _hourly_rows("Improving", BASE_DATE, 10.0, hours=8))
    transform(empty_warehouse)
    assert clean_rows(empty_warehouse, "Improving").iloc[0]["valid_hours_count"] == 8

    with empty_warehouse.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {SCHEMA}.stg_air_quality")
    stage(empty_warehouse, _hourly_rows("Improving", BASE_DATE, 150.0, hours=24))
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "Improving").iloc[0]
    assert row["valid_hours_count"] == 24
    assert row["avg_pm2_5"] == pytest.approx(150.0)


def test_missing_copollutant_shortens_the_sample_not_the_mean(empty_warehouse: Engine):
    """AVG must skip NULL PM10 rather than treat them as zero."""
    stage(
        empty_warehouse,
        _hourly_rows("Partial10", BASE_DATE, 100.0, pm10=200.0, pm10_hours=12),
    )
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "Partial10").iloc[0]
    assert row["avg_pm10"] == pytest.approx(200.0), (
        "NULL PM10 hours were coalesced to 0 and dragged the mean down"
    )
    assert row["valid_hours_count"] == 24


def test_all_null_copollutant_becomes_zero_and_null_ratio(empty_warehouse: Engine):
    stage(empty_warehouse, _hourly_rows("NoPm10", BASE_DATE, 100.0, pm10_hours=0))
    transform(empty_warehouse)

    assert clean_rows(empty_warehouse, "NoPm10").iloc[0]["avg_pm10"] == pytest.approx(0.0)
    assert pd.isna(mart_rows(empty_warehouse, "NoPm10").iloc[0]["pm25_to_pm10_ratio"])


def test_row_without_pm2_5_is_excluded(empty_warehouse: Engine):
    rows = _hourly_rows("NullPm25", BASE_DATE, 100.0)
    for row in rows[:12]:
        row["pm2_5"] = None
    stage(empty_warehouse, rows)
    transform(empty_warehouse)

    row = clean_rows(empty_warehouse, "NullPm25").iloc[0]
    assert row["valid_hours_count"] == 12
    assert row["avg_pm2_5"] == pytest.approx(100.0)


def test_staging_retention_prunes_old_ingests_only(empty_warehouse: Engine):
    stage(empty_warehouse, _hourly_rows("Retained", BASE_DATE, 80.0))
    with empty_warehouse.begin() as conn:
        # Age half the rows past the retention horizon.
        conn.exec_driver_sql(
            f"""
            UPDATE {SCHEMA}.stg_air_quality
               SET ingested_at = CURRENT_TIMESTAMP - INTERVAL '30 days'
             WHERE observation_time < '{BASE_DATE} 12:00:00'
            """
        )
    transform(empty_warehouse)

    remaining = read_sql_frame(
        f"SELECT COUNT(*) AS n FROM {SCHEMA}.stg_air_quality", empty_warehouse
    ).iloc[0]["n"]
    assert remaining == 12, "retention should prune only the aged ingests"
    # The clean row was still built from all 24 hours before pruning.
    assert clean_rows(empty_warehouse, "Retained").iloc[0]["valid_hours_count"] == 24


# ---------------------------------------------------------------------------
# mart: window functions
# ---------------------------------------------------------------------------
def test_rolling_7d_matches_a_pandas_reference(empty_warehouse: Engine):
    values = [40.0, 55.0, 62.0, 48.0, 90.0, 120.0, 75.0, 66.0, 51.0, 88.0, 102.0, 44.0]
    stage_series(empty_warehouse, "Rolling", values)
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Rolling")
    expected = pd.Series(values).rolling(window=7, min_periods=1).mean()

    assert len(mart) == len(values)
    for idx, want in enumerate(expected):
        assert mart.iloc[idx]["rolling_7d_pm2_5"] == pytest.approx(want, abs=0.01), (
            f"day {idx} rolling average"
        )
    assert mart["rolling_7d_days"].tolist() == [1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 7]
    assert mart["rolling_7d_is_full_window"].tolist() == [False] * 6 + [True] * 6


def test_rolling_window_is_calendar_aware_across_a_gap(empty_warehouse: Engine):
    """A missing week must shorten the frame, not reach further back in time.

    Days 1-10 are present, days 11-15 are missing, day 16 returns. A ROWS-based
    frame would average day 16 with days 5-10; the RANGE frame used here only
    reaches back to day 10.
    """
    values: list[Optional[float]] = [100.0] * 10 + [None] * 5 + [10.0]
    stage_series(empty_warehouse, "Gappy", values)
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Gappy")
    assert len(mart) == 11

    last = mart.iloc[-1]
    assert last["obs_date"] == BASE_DATE + timedelta(days=15)
    assert last["rolling_7d_days"] == 2, "frame should hold only days 10 and 16"
    assert last["rolling_7d_pm2_5"] == pytest.approx(55.0, abs=0.01)
    # The gap is visible rather than papered over.
    assert last["days_since_prev_obs"] == 6
    assert last["prev_obs_date"] == BASE_DATE + timedelta(days=9)


def test_lag_produces_day_over_day_deltas(empty_warehouse: Engine):
    stage_series(empty_warehouse, "Deltas", [50.0, 75.0, 60.0])
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Deltas")

    assert pd.isna(mart.iloc[0]["prev_day_pm2_5"])
    assert pd.isna(mart.iloc[0]["dod_pm2_5_pct_change"])

    assert mart.iloc[1]["prev_day_pm2_5"] == pytest.approx(50.0)
    assert mart.iloc[1]["dod_pm2_5_abs_change"] == pytest.approx(25.0)
    assert mart.iloc[1]["dod_pm2_5_pct_change"] == pytest.approx(50.0)
    assert mart.iloc[1]["days_since_prev_obs"] == 1

    assert mart.iloc[2]["dod_pm2_5_abs_change"] == pytest.approx(-15.0)
    assert mart.iloc[2]["dod_pm2_5_pct_change"] == pytest.approx(-20.0)


def test_week_over_week_lag(empty_warehouse: Engine):
    values = [100.0] * 7 + [150.0]
    stage_series(empty_warehouse, "WoW", values)
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "WoW")
    last = mart.iloc[-1]
    assert last["prev_week_pm2_5"] == pytest.approx(100.0)
    assert last["prev_week_obs_date"] == BASE_DATE
    assert last["wow_pm2_5_pct_change"] == pytest.approx(50.0)


def test_cities_do_not_leak_across_the_partition(empty_warehouse: Engine):
    stage_series(empty_warehouse, "CityA", [10.0, 10.0, 10.0])
    stage_series(empty_warehouse, "CityB", [200.0, 200.0, 200.0])
    transform(empty_warehouse)

    a = mart_rows(empty_warehouse, "CityA")
    b = mart_rows(empty_warehouse, "CityB")
    assert a["rolling_7d_pm2_5"].max() == pytest.approx(10.0)
    assert b["rolling_7d_pm2_5"].min() == pytest.approx(200.0)
    assert pd.isna(a.iloc[0]["prev_day_pm2_5"]) and pd.isna(b.iloc[0]["prev_day_pm2_5"])


# ---------------------------------------------------------------------------
# mart: flags
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value, category, cpcb_breach, who_breach",
    [
        (10.0, "Good", False, False),
        (15.0, "Good", False, False),
        (15.01, "Good", False, True),
        (30.0, "Good", False, True),
        (30.01, "Satisfactory", False, True),
        (60.0, "Satisfactory", False, True),
        (60.01, "Moderate", True, True),
        (90.01, "Poor", True, True),
        (120.01, "Very Poor", True, True),
        (250.01, "Severe", True, True),
    ],
)
def test_cpcb_categories_and_threshold_flags(
    empty_warehouse: Engine, value, category, cpcb_breach, who_breach
):
    stage(empty_warehouse, _hourly_rows("Bands", BASE_DATE, value))
    transform(empty_warehouse)

    row = mart_rows(empty_warehouse, "Bands").iloc[0]
    assert row["cpcb_category"] == category
    assert bool(row["is_cpcb_breach"]) is cpcb_breach
    assert bool(row["is_who_breach"]) is who_breach


def test_anomaly_baseline_excludes_the_current_day(empty_warehouse: Engine):
    """A spike must be measured against the days before it, not against itself."""
    baseline = [50.0 + (i % 5) for i in range(40)]   # 50..54, mean ~52
    stage_series(empty_warehouse, "Spike", baseline + [200.0])
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Spike")
    spike = mart.iloc[-1]

    assert spike["avg_pm2_5"] == pytest.approx(200.0)
    assert spike["baseline_30d_days"] == 30
    # 200 is nowhere near the baseline mean -> it was excluded from its own window.
    assert spike["baseline_30d_mean"] == pytest.approx(52.0, abs=1.0)
    assert spike["z_score_30d"] > 2.0
    assert bool(spike["is_statistical_anomaly"]) is True

    # And the ordinary days around it are not flagged.
    assert not mart.iloc[:-1]["is_statistical_anomaly"].astype(bool).any()


def test_anomaly_needs_fourteen_baseline_days(empty_warehouse: Engine):
    """A young series must not flag on noise."""
    stage_series(empty_warehouse, "Young", [50.0, 51.0, 52.0, 53.0, 500.0])
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Young")
    last = mart.iloc[-1]
    assert last["avg_pm2_5"] == pytest.approx(500.0)
    assert last["baseline_30d_days"] == 4
    assert bool(last["is_statistical_anomaly"]) is False, (
        "flag fired with fewer than 14 baseline days"
    )


def test_flat_series_yields_no_anomaly_and_null_z(empty_warehouse: Engine):
    """Zero variance must not divide by zero."""
    stage_series(empty_warehouse, "Flat", [60.0] * 20)
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Flat")
    assert (mart["baseline_30d_std"].dropna() == 0).all()
    assert mart["z_score_30d"].isna().all()
    assert not mart["is_statistical_anomaly"].astype(bool).any()


def test_ytd_breach_counter_accumulates_and_resets_each_year(empty_warehouse: Engine):
    stage_series(
        empty_warehouse, "Counter", [100.0, 100.0, 20.0, 100.0], start=date(2026, 12, 29)
    )
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Counter")
    # 2026-12-29, 12-30 breach; 12-31 does not; 2027-01-01 breaches in a new year.
    assert mart["ytd_cpcb_breach_days"].tolist() == [1, 2, 2, 1]


def test_completeness_and_season_labels(empty_warehouse: Engine):
    stage(empty_warehouse, _hourly_rows("Meta", date(2026, 7, 15), 40.0, hours=12))
    stage(empty_warehouse, _hourly_rows("Meta", date(2026, 11, 15), 180.0, hours=24))
    transform(empty_warehouse)

    mart = mart_rows(empty_warehouse, "Meta")
    july, november = mart.iloc[0], mart.iloc[1]

    assert july["indian_season"] == "Southwest Monsoon"
    assert july["data_completeness_pct"] == pytest.approx(50.0)
    assert november["indian_season"] == "Post-Monsoon (Oct-Nov)"
    assert november["data_completeness_pct"] == pytest.approx(100.0)
    assert november["year_month"] == "2026-11"
    assert november["iso_day_of_week"] == 7  # 2026-11-15 is a Sunday
    assert bool(november["is_weekend"]) is True


def test_latest_snapshot_view_returns_one_row_per_city(empty_warehouse: Engine):
    stage_series(empty_warehouse, "SnapA", [10.0, 20.0, 30.0])
    stage_series(empty_warehouse, "SnapB", [40.0, 50.0])
    transform(empty_warehouse)

    snapshot = read_sql_frame(
        f"SELECT city, obs_date, avg_pm2_5 FROM {SCHEMA}.mart_latest_city_snapshot "
        f"ORDER BY city",
        empty_warehouse,
    )
    assert snapshot["city"].tolist() == ["SnapA", "SnapB"]
    assert snapshot.iloc[0]["avg_pm2_5"] == pytest.approx(30.0)
    assert snapshot.iloc[1]["avg_pm2_5"] == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# analytical queries
# ---------------------------------------------------------------------------
def test_breach_streaks_gaps_and_islands(empty_warehouse: Engine):
    # Breach on days 1-4, clean on day 5, breach on days 6-7.
    stage_series(
        empty_warehouse,
        "Streaky",
        [100.0, 100.0, 100.0, 100.0, 20.0, 100.0, 100.0],
    )
    transform(empty_warehouse)

    streaks = read_sql_frame(
        f"""
        WITH breach_days AS (
            SELECT city, obs_date,
                   obs_date - (ROW_NUMBER() OVER (PARTITION BY city ORDER BY obs_date))::INTEGER
                       AS streak_key
            FROM {SCHEMA}.mart_aqi_analytics
            WHERE is_cpcb_breach AND city = 'Streaky'
        )
        SELECT MIN(obs_date) AS streak_start,
               MAX(obs_date) AS streak_end,
               COUNT(*)      AS consecutive_breach_days
        FROM breach_days
        GROUP BY streak_key
        ORDER BY consecutive_breach_days DESC
        """,
        empty_warehouse,
    )

    assert streaks["consecutive_breach_days"].tolist() == [4, 2]
    assert streaks.iloc[0]["streak_start"] == BASE_DATE
    assert streaks.iloc[0]["streak_end"] == BASE_DATE + timedelta(days=3)


def test_every_analytical_query_runs(empty_warehouse: Engine):
    """Guard against a query in 03_analytical_queries.sql drifting out of sync."""
    from src.config import ANALYTICAL_SQL_PATH
    from src.report import split_analytical_queries

    stage_series(empty_warehouse, "QueryCheck", [50.0 + (i % 7) * 10 for i in range(45)])
    transform(empty_warehouse)
    start_run(empty_warehouse, mode="test", past_days=1, cities_requested=1)

    queries = split_analytical_queries(ANALYTICAL_SQL_PATH.read_text(encoding="utf-8"))
    assert queries, "no queries parsed out of the analytical SQL file"

    with empty_warehouse.connect() as conn:
        for key, item in queries.items():
            frame = pd.read_sql_query(item["sql"], conn)
            assert not frame.columns.empty, f"{key} returned no columns"


# ---------------------------------------------------------------------------
# run auditing
# ---------------------------------------------------------------------------
def test_pipeline_run_audit_records_a_successful_run(empty_warehouse: Engine):
    run_id = start_run(empty_warehouse, mode="incremental", past_days=4, cities_requested=6)

    running = read_sql_frame(
        f"SELECT * FROM {SCHEMA}.pipeline_runs WHERE run_id = {run_id}", empty_warehouse
    ).iloc[0]
    assert running["status"] == "running"
    assert running["finished_at"] is None or pd.isna(running["finished_at"])

    finish_run(
        empty_warehouse,
        run_id,
        status="success",
        cities_succeeded=6,
        rows_staged=864,
        clean_rows_total=540,
        message="all cities extracted",
    )

    done = read_sql_frame(
        f"SELECT * FROM {SCHEMA}.pipeline_runs WHERE run_id = {run_id}", empty_warehouse
    ).iloc[0]
    assert done["status"] == "success"
    assert done["cities_succeeded"] == 6
    assert done["rows_staged"] == 864
    assert done["duration_seconds"] >= 0
    assert done["finished_at"] is not None


def test_pipeline_run_audit_rejects_an_unknown_status(empty_warehouse: Engine):
    from sqlalchemy.exc import IntegrityError

    run_id = start_run(empty_warehouse, mode="incremental", past_days=4, cities_requested=6)
    # Short enough to reach the CHECK constraint rather than tripping the
    # varchar(20) length limit first.
    with pytest.raises(IntegrityError):
        finish_run(empty_warehouse, run_id, status="bogus")


# ---------------------------------------------------------------------------
# constraints
# ---------------------------------------------------------------------------
def test_clean_layer_rejects_negative_concentrations(empty_warehouse: Engine):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        with empty_warehouse.begin() as conn:
            conn.exec_driver_sql(
                f"""
                INSERT INTO {SCHEMA}.clean_daily_aqi
                    (city, obs_date, avg_pm2_5, max_pm2_5, min_pm2_5, avg_pm10,
                     avg_no2, avg_so2, avg_co, avg_us_aqi, valid_hours_count)
                VALUES ('Bad', '2026-01-01', -5, 10, 0, 20, 10, 5, 100, 50, 24)
                """
            )


def test_clean_layer_rejects_impossible_hour_counts(empty_warehouse: Engine):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        with empty_warehouse.begin() as conn:
            conn.exec_driver_sql(
                f"""
                INSERT INTO {SCHEMA}.clean_daily_aqi
                    (city, obs_date, avg_pm2_5, max_pm2_5, min_pm2_5, avg_pm10,
                     avg_no2, avg_so2, avg_co, avg_us_aqi, valid_hours_count)
                VALUES ('Bad', '2026-01-01', 50, 60, 40, 100, 10, 5, 100, 150, 25)
                """
            )


def test_clean_layer_rejects_mean_outside_min_max(empty_warehouse: Engine):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        with empty_warehouse.begin() as conn:
            conn.exec_driver_sql(
                f"""
                INSERT INTO {SCHEMA}.clean_daily_aqi
                    (city, obs_date, avg_pm2_5, max_pm2_5, min_pm2_5, avg_pm10,
                     avg_no2, avg_so2, avg_co, avg_us_aqi, valid_hours_count)
                VALUES ('Bad', '2026-01-01', 500, 60, 40, 100, 10, 5, 100, 150, 24)
                """
            )
