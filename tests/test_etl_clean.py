"""Unit tests for extraction shaping, cleaning and configuration.

No database and no network: these run anywhere and are the gate CI puts in
front of the production ETL step.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.config import STAGING_COLUMNS, Settings
from src.db import normalize_database_url
from src.etl import _hourly_to_frame, _prepare_for_load, clean_dataframe, decide_window
from src.report import split_analytical_queries
from src.config import ANALYTICAL_SQL_PATH

REF_NOW = pd.Timestamp("2026-10-31 12:00:00")


def _raw(**overrides) -> pd.DataFrame:
    base = {
        "city": ["Delhi"] * 4,
        "latitude": [28.6139] * 4,
        "longitude": [77.2090] * 4,
        "observation_time": [
            "2026-10-31 09:00:00",
            "2026-10-31 10:00:00",
            "2026-10-31 10:00:00",  # duplicate hour, revised value
            "2026-10-31 15:00:00",  # future forecast hour
        ],
        "pm2_5": [145.20, 150.00, 152.75, 180.00],
        "pm10": [230.00, 235.00, 236.00, 260.00],
        "no2": [42.10, 44.00, 44.50, 50.00],
        "so2": [12.00, 12.00, 12.50, 14.00],
        "co": [850.00, 860.00, 861.00, 900.00],
        "us_aqi": [198, 200, 201, 210],
    }
    base.update(overrides)
    return pd.DataFrame(base, columns=STAGING_COLUMNS)


# ---------------------------------------------------------------------------
# _hourly_to_frame
# ---------------------------------------------------------------------------
def test_hourly_to_frame_pads_truncated_series():
    """A variable the API returns short must become NULL, not a length error."""
    frame = _hourly_to_frame(
        "Delhi",
        28.6139,
        77.2090,
        {
            "time": ["2026-10-31T00:00", "2026-10-31T01:00", "2026-10-31T02:00"],
            "pm2_5": [10.0, 11.0, 12.0],
            "pm10": [20.0],            # truncated
            # nitrogen_dioxide, sulphur_dioxide, carbon_monoxide, us_aqi absent
        },
    )
    assert list(frame.columns) == STAGING_COLUMNS
    assert len(frame) == 3
    assert frame.loc[0, "pm10"] == 20.0
    assert frame["pm10"].iloc[1:].isna().all(), "short series should pad with nulls"
    assert frame["no2"].isna().all()
    assert frame["us_aqi"].isna().all()


def test_hourly_to_frame_ignores_overlong_series():
    frame = _hourly_to_frame(
        "Mumbai",
        19.0760,
        72.8777,
        {"time": ["2026-10-31T00:00"], "pm2_5": [10.0, 11.0, 12.0]},
    )
    assert len(frame) == 1
    assert frame.loc[0, "pm2_5"] == 10.0


def test_hourly_to_frame_handles_empty_payload():
    frame = _hourly_to_frame("Delhi", 28.6, 77.2, {"time": []})
    assert frame.empty
    assert list(frame.columns) == STAGING_COLUMNS


# ---------------------------------------------------------------------------
# clean_dataframe
# ---------------------------------------------------------------------------
def test_clean_drops_future_hours_and_deduplicates():
    cleaned = clean_dataframe(_raw(), reference_now=REF_NOW)

    # 15:00 is in the future; the 10:00 duplicate collapses to its last value.
    assert len(cleaned) == 2
    assert cleaned["observation_time"].max() == pd.Timestamp("2026-10-31 10:00:00")

    ten = cleaned.loc[cleaned["observation_time"] == pd.Timestamp("2026-10-31 10:00:00")]
    assert ten.iloc[0]["pm2_5"] == pytest.approx(152.75)


def test_clean_blanks_impossible_values_rather_than_clamping():
    """A negative reading must not be clamped to 0 — that fakes clean air."""
    raw = _raw(
        pm2_5=[-12.5, 150.00, 152.75, 180.00],
        no2=[-3.0, 44.00, 44.50, 50.00],
        co=[99999.0, 860.00, 861.00, 900.00],  # above the plausible ceiling
    )
    cleaned = clean_dataframe(raw, reference_now=REF_NOW)

    nine = cleaned.loc[cleaned["observation_time"] == pd.Timestamp("2026-10-31 09:00:00")].iloc[0]
    assert pd.isna(nine["pm2_5"]), "negative PM2.5 should be NaN, not 0"
    assert pd.isna(nine["no2"])
    assert pd.isna(nine["co"]), "implausibly high CO should be NaN"
    # The row survives because PM10 is still valid.
    assert nine["pm10"] == pytest.approx(230.0)


def test_clean_drops_rows_with_no_particulate_at_all():
    raw = _raw(
        pm2_5=[None, 150.00, 152.75, 180.00],
        pm10=[None, 235.00, 236.00, 260.00],
    )
    cleaned = clean_dataframe(raw, reference_now=REF_NOW)
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["observation_time"] == pd.Timestamp("2026-10-31 10:00:00")


def test_clean_drops_unparseable_timestamps():
    raw = _raw(
        observation_time=[
            "not-a-timestamp",
            "2026-10-31 10:00:00",
            "2026-10-31 11:00:00",
            "2026-10-31 15:00:00",
        ]
    )
    cleaned = clean_dataframe(raw, reference_now=REF_NOW)
    assert len(cleaned) == 2
    assert cleaned["observation_time"].notna().all()


def test_clean_rounds_us_aqi_and_keeps_nulls():
    raw = _raw(us_aqi=[198.4, 200.6, 201.0, 210.0])
    cleaned = clean_dataframe(raw, reference_now=REF_NOW)
    assert cleaned.iloc[0]["us_aqi"] == 198.0
    assert cleaned.iloc[1]["us_aqi"] == 201.0


def test_clean_of_empty_frame_returns_expected_columns():
    cleaned = clean_dataframe(pd.DataFrame(), reference_now=REF_NOW)
    assert cleaned.empty
    assert list(cleaned.columns) == STAGING_COLUMNS


def test_clean_output_is_sorted_and_reindexed():
    raw = _raw(
        observation_time=[
            "2026-10-31 11:00:00",
            "2026-10-31 09:00:00",
            "2026-10-31 10:00:00",
            "2026-10-31 15:00:00",
        ]
    )
    cleaned = clean_dataframe(raw, reference_now=REF_NOW)
    assert cleaned["observation_time"].is_monotonic_increasing
    assert list(cleaned.index) == list(range(len(cleaned)))
    assert list(cleaned.columns) == STAGING_COLUMNS


# ---------------------------------------------------------------------------
# _prepare_for_load
# ---------------------------------------------------------------------------
def test_prepare_for_load_makes_us_aqi_int_or_none():
    cleaned = clean_dataframe(_raw(us_aqi=[198.0, None, None, 210.0]), reference_now=REF_NOW)
    prepared = _prepare_for_load(cleaned)
    values = prepared["us_aqi"].tolist()
    assert values[0] == 198 and isinstance(values[0], int)
    assert values[1] is None


# ---------------------------------------------------------------------------
# Settings / window selection
# ---------------------------------------------------------------------------
def test_settings_defaults(monkeypatch):
    for name in (
        "BACKFILL_DAYS",
        "INCREMENTAL_DAYS",
        "MIN_HOURS_PER_DAY",
        "STAGING_RETENTION_DAYS",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = Settings.from_env()
    assert settings.backfill_days == 90
    assert settings.incremental_days == 4
    assert settings.min_hours_per_day == 6


def test_settings_caps_backfill_at_api_limit(monkeypatch):
    monkeypatch.setenv("BACKFILL_DAYS", "365")
    assert Settings.from_env().backfill_days == 92


def test_settings_rejects_nonsense(monkeypatch):
    monkeypatch.setenv("MIN_HOURS_PER_DAY", "0")
    with pytest.raises(ValueError, match="MIN_HOURS_PER_DAY"):
        Settings.from_env()

    monkeypatch.setenv("MIN_HOURS_PER_DAY", "6")
    monkeypatch.setenv("INCREMENTAL_DAYS", "abc")
    with pytest.raises(ValueError, match="INCREMENTAL_DAYS"):
        Settings.from_env()


def test_decide_window_backfills_only_when_empty():
    settings = Settings()
    assert decide_window(0, settings) == (settings.backfill_days, "backfill")
    assert decide_window(1, settings) == (settings.incremental_days, "incremental")


# ---------------------------------------------------------------------------
# Connection string normalization
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "given",
    [
        "postgres://u:p@host/db",
        "postgresql://u:p@host/db",
        "postgresql+psycopg2://u:p@host/db",
        "  postgres://u:p@host/db  ",
    ],
)
def test_normalize_database_url(given):
    assert normalize_database_url(given) == "postgresql+psycopg2://u:p@host/db"


def test_normalize_preserves_query_string():
    normalized = normalize_database_url("postgres://u:p@host/db?sslmode=require")
    assert normalized.endswith("/db?sslmode=require")
    assert normalized.startswith("postgresql+psycopg2://")


# ---------------------------------------------------------------------------
# Report query splitting
# ---------------------------------------------------------------------------
def test_analytical_queries_all_parse():
    queries = split_analytical_queries(ANALYTICAL_SQL_PATH.read_text(encoding="utf-8"))
    assert len(queries) >= 8
    assert "Q1" in queries and "Q5" in queries
    for key, item in queries.items():
        assert item["title"], f"{key} has no title"
        assert item["sql"].upper().lstrip().startswith(("SELECT", "WITH")), key
        assert item["sql"].rstrip().endswith(";"), f"{key} is not terminated"
