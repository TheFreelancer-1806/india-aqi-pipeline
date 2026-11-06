"""Contract tests against the live Open-Meteo Air Quality API.

Deselected by default (pytest.ini sets ``-m "not network"``) so the suite stays
offline-safe. Run them when you want to know whether the upstream contract still
holds:

    pytest -m network -v

These are the tests that catch a renamed field or a changed unit before the
scheduled pipeline silently loads garbage.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.config import CITIES, POLLUTANT_COLUMNS, STAGING_COLUMNS
from src.etl import build_http_session, clean_dataframe, fetch_city_aqi

pytestmark = pytest.mark.network


@pytest.fixture(scope="module")
def session():
    http = build_http_session()
    yield http
    http.close()


def test_api_returns_the_expected_shape(session):
    raw = fetch_city_aqi("Delhi", 28.6139, 77.2090, past_days=2, session=session)

    assert list(raw.columns) == STAGING_COLUMNS
    assert len(raw) >= 24, "two past days plus today should exceed 24 hourly rows"
    assert raw["city"].eq("Delhi").all()
    assert raw["pm2_5"].notna().any(), "PM2.5 came back entirely empty"
    assert raw["pm10"].notna().any()


def test_api_values_are_physically_plausible(session):
    cleaned = clean_dataframe(
        fetch_city_aqi("Delhi", 28.6139, 77.2090, past_days=2, session=session)
    )

    assert not cleaned.empty
    for column in POLLUTANT_COLUMNS:
        values = cleaned[column].dropna()
        if values.empty:
            continue
        assert (values >= 0).all(), f"{column} returned a negative value"
        assert (values <= 2500).all(), f"{column} returned an implausible value"


def test_cleaning_never_keeps_a_future_hour(session):
    cleaned = clean_dataframe(
        fetch_city_aqi("Mumbai", 19.0760, 72.8777, past_days=1, session=session)
    )
    now_ist = pd.Timestamp.now(tz="Asia/Kolkata").tz_localize(None)
    assert cleaned["observation_time"].max() <= now_ist


def test_every_configured_city_resolves(session):
    """A typo in a coordinate should fail here, not at 01:00 UTC."""
    for city, meta in CITIES.items():
        raw = fetch_city_aqi(city, meta["lat"], meta["lon"], past_days=1, session=session)
        assert not raw.empty, f"{city} returned no rows"
        assert raw["pm2_5"].notna().any(), f"{city} returned no PM2.5"
