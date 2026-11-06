"""Run the analytical query set and render docs/FINDINGS.md from live data.

The findings document is generated, not hand-written, so every number in it is
traceable to a query in sql/03_analytical_queries.sql against the warehouse as
it stood at render time.

    python -m src.report                      # write docs/FINDINGS.md
    python -m src.report --stdout             # print instead of writing
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.engine import Engine

from src.config import (
    ANALYTICAL_SQL_PATH,
    CITY_ZONES,
    CPCB_PM25_SAFE_LIMIT,
    DOCS_DIR,
    WHO_PM25_SAFE_LIMIT,
)
from src.db import create_db_engine

logger = logging.getLogger("aqi.report")

#: Matches the "-- Qn — title" banner that opens each query in the SQL file.
_QUERY_HEADER = re.compile(r"^--\s*(Q\d+)\s*[—-]\s*(.+?)\s*$", re.MULTILINE)


def split_analytical_queries(sql_text: str) -> Dict[str, Dict[str, str]]:
    """Split 03_analytical_queries.sql into its individual labelled queries."""
    matches = list(_QUERY_HEADER.finditer(sql_text))
    queries: Dict[str, Dict[str, str]] = {}
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(sql_text)
        body = sql_text[match.end() : end]
        # Drop the remaining comment lines of the banner, keep executable SQL.
        statement = "\n".join(
            line for line in body.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement:
            queries[match.group(1)] = {
                "title": match.group(2).strip(),
                "sql": statement,
            }
    return queries


def run_queries(engine: Engine) -> Dict[str, pd.DataFrame]:
    """Execute every query in the analytical SQL file."""
    queries = split_analytical_queries(ANALYTICAL_SQL_PATH.read_text(encoding="utf-8"))
    results: Dict[str, pd.DataFrame] = {}
    with engine.connect() as conn:
        for key, item in queries.items():
            try:
                results[key] = pd.read_sql_query(item["sql"], conn)
                logger.info("%s (%s): %d rows", key, item["title"], len(results[key]))
            except Exception as exc:  # noqa: BLE001
                logger.error("%s failed: %s", key, exc)
                results[key] = pd.DataFrame()
    return results


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------
def _md_table(df: pd.DataFrame, max_rows: Optional[int] = None) -> str:
    if df.empty:
        return "_No rows returned._"
    shown = df.head(max_rows) if max_rows else df
    return shown.to_markdown(index=False, floatfmt=".2f")


def _fmt(value: object, nd: int = 1, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "n/a"
    if pd.isna(value):
        return "n/a"
    try:
        return f"{float(value):,.{nd}f}{suffix}"
    except (TypeError, ValueError):
        return f"{value}{suffix}"


def _coverage_line(health: pd.DataFrame) -> str:
    if health.empty:
        return "_No data loaded._"
    return (
        f"{int(health['days_loaded'].sum())} city-days across "
        f"{health.shape[0]} cities, "
        f"{health['first_day'].min()} to {health['last_day'].max()}, "
        f"{int(health['missing_days'].sum())} missing day(s), "
        f"mean completeness {_fmt(health['avg_valid_hours'].mean(), 1)}/24 hours"
    )


# ---------------------------------------------------------------------------
# Findings narrative, derived from the query results
# ---------------------------------------------------------------------------
def _finding_gradient(breach: pd.DataFrame) -> List[str]:
    """Threshold breaches: spread between the cleanest and dirtiest airshed."""
    if breach.empty:
        return []
    worst = breach.iloc[0]
    best = breach.iloc[-1]
    lines = [
        f"### 1. The breach rate, not the mean, separates the airsheds",
        "",
        f"Over the loaded window **{worst['city']}** "
        f"({CITY_ZONES.get(worst['city'], 'n/a')}) sits in CPCB breach on "
        f"**{_fmt(worst['cpcb_breach_pct'], 1, '%')}** of days "
        f"({int(worst['cpcb_breach_days'])}/{int(worst['days_observed'])}), "
        f"against **{_fmt(best['cpcb_breach_pct'], 1, '%')}** for "
        f"**{best['city']}** ({CITY_ZONES.get(best['city'], 'n/a')}). "
        f"Mean daily PM2.5 runs {_fmt(worst['mean_pm2_5'])} vs "
        f"{_fmt(best['mean_pm2_5'])} ug/m3.",
        "",
        f"The WHO 24-hour guideline ({WHO_PM25_SAFE_LIMIT:g} ug/m3) is a different "
        f"instrument entirely: it is breached on "
        f"{_fmt(breach['who_breach_pct'].min(), 1, '%')} to "
        f"{_fmt(breach['who_breach_pct'].max(), 1, '%')} of days across these "
        f"cities, which is why this pipeline flags both standards separately "
        f"rather than reporting a single \"safe / unsafe\" bit.",
        "",
        _md_table(breach),
        "",
    ]
    return lines


def _finding_seasonality(seasonal: pd.DataFrame, trend: pd.DataFrame) -> List[str]:
    """Seasonality: season-to-season movement per city."""
    if seasonal.empty:
        return []
    pivot = seasonal.pivot_table(
        index="city", columns="indian_season", values="mean_pm2_5", aggfunc="mean"
    )
    lines = [
        "### 2. Seasonality is a step change, not a drift",
        "",
        "Mean daily PM2.5 (ug/m3) by Indian meteorological season. The window "
        "loaded here may not cover every season — an empty cell means the "
        "pipeline has not yet observed that season.",
        "",
        _md_table(pivot.round(1).reset_index()),
        "",
    ]
    if pivot.shape[1] >= 2:
        spread = (pivot.max(axis=1) - pivot.min(axis=1)).sort_values(ascending=False)
        top = spread.index[0]
        lines += [
            f"The seasonal spread is widest in **{top}** "
            f"({_fmt(spread.iloc[0])} ug/m3 between its cleanest and dirtiest "
            f"season) and narrowest in **{spread.index[-1]}** "
            f"({_fmt(spread.iloc[-1])} ug/m3). A wide spread means an airshed "
            f"whose air quality is set by meteorology; a narrow one means a city "
            f"whose emissions dominate year round.",
            "",
        ]
    if not trend.empty:
        monthly = trend.pivot_table(
            index="year_month", columns="city", values="mean_rolling_7d_pm2_5"
        )
        lines += [
            "Month-by-month mean of the SQL 7-day rolling average:",
            "",
            _md_table(monthly.round(1).reset_index()),
            "",
        ]
    return lines


def _finding_persistence(persistence: pd.DataFrame) -> List[str]:
    """Trend/structure: does a shock persist or wash out?"""
    if persistence.empty or persistence["lag1_autocorrelation"].isna().all():
        return []
    ranked = persistence.dropna(subset=["lag1_autocorrelation"]).sort_values(
        "lag1_autocorrelation", ascending=False
    )
    sticky, volatile = ranked.iloc[0], ranked.iloc[-1]
    high = float(sticky["lag1_autocorrelation"])
    low = float(volatile["lag1_autocorrelation"])

    lines = [
        "### 3. Pollution is highly autocorrelated day to day",
        "",
        f"Lag-1 autocorrelation of daily PM2.5 — correlating each day against the "
        f"mart's own `LAG()` column — runs from **{_fmt(low, 2)}** "
        f"(**{volatile['city']}**, {CITY_ZONES.get(volatile['city'], 'n/a')}) to "
        f"**{_fmt(high, 2)}** (**{sticky['city']}**, "
        f"{CITY_ZONES.get(sticky['city'], 'n/a')}). Today's air is mostly "
        f"yesterday's air: a single day's reading carries much less independent "
        f"information than it appears to.",
        "",
    ]

    if high - low < 0.25:
        lines += [
            f"The spread across these cities is narrow ({_fmt(low, 2)} to "
            f"{_fmt(high, 2)}), so on this window the data does not separate "
            f"\"accumulating\" from \"ventilated\" airsheds — every city in the "
            f"set shows strong day-to-day carryover. A longer window covering "
            f"the winter inversion would be needed to test that distinction.",
            "",
        ]
    else:
        lines += [
            f"That spread is wide enough to be structural: high autocorrelation "
            f"means pollution accumulating under a stable boundary layer, low "
            f"autocorrelation an airshed flushed on roughly a daily cycle.",
            "",
        ]

    lines += [
        f"Day-over-day churn is the same story from the other side: mean absolute "
        f"DoD change spans {_fmt(ranked['mean_abs_dod_pct'].min(), 1, '%')} to "
        f"{_fmt(ranked['mean_abs_dod_pct'].max(), 1, '%')}. This is the practical "
        f"case for putting a 7-day rolling average next to the daily value — the "
        f"daily number moves on weather, the rolling one on conditions.",
        "",
        _md_table(ranked),
        "",
    ]
    return lines


def _finding_anomaly_vs_threshold(
    breach: pd.DataFrame, spikes: pd.DataFrame, streaks: pd.DataFrame
) -> List[str]:
    """Why a static threshold alone is not an alerting strategy."""
    lines = [
        "### 4. A static threshold cannot alert; a rolling Z-score can",
        "",
    ]
    if not breach.empty:
        saturated = breach[breach["cpcb_breach_pct"] >= 80]
        if not saturated.empty:
            names = ", ".join(f"**{c}**" for c in saturated["city"])
            lines += [
                f"{names} breach the CPCB {CPCB_PM25_SAFE_LIMIT:g} ug/m3 standard on "
                f"at least 80% of observed days. An alert wired to that threshold "
                f"fires almost every morning and is ignored by the second week.",
                "",
            ]
        else:
            lines += [
                f"No city in this window is saturated against the CPCB "
                f"{CPCB_PM25_SAFE_LIMIT:g} ug/m3 threshold (highest breach rate "
                f"{_fmt(breach['cpcb_breach_pct'].max(), 1, '%')}), so the static "
                f"flag still carries information here. It stops doing so as soon as "
                f"the winter inversion season loads.",
                "",
            ]
    if not streaks.empty:
        top = streaks.iloc[0]
        lines += [
            f"The longest unbroken breach streak is **{int(top['consecutive_breach_days'])} "
            f"days** in **{top['city']}** ({top['streak_start']} to {top['streak_end']}), "
            f"found with a gaps-and-islands query over the mart.",
            "",
        ]
    if not spikes.empty:
        worst = spikes.iloc[0]
        lines += [
            f"The 30-day rolling Z-score behaves differently. Ranked by sigma "
            f"(Q4 returns the top 15), the most extreme episode in this window is "
            f"**{worst['city']}** on **{worst['obs_date']}** at "
            f"{_fmt(worst['avg_pm2_5'])} ug/m3 — "
            f"**{_fmt(worst['z_score_30d'], 2)} sigma** above its own trailing "
            f"baseline of {_fmt(worst['baseline_30d_mean'])} ug/m3. Because the "
            f"baseline excludes the current day, a spike cannot hide inside the "
            f"mean it would otherwise inflate.",
            "",
            _md_table(spikes, max_rows=10),
            "",
        ]
    else:
        lines += [
            "No day in the loaded window exceeded 2 sigma above its trailing "
            "30-day baseline. The flag needs at least 14 baseline days before it "
            "will fire, so a freshly backfilled warehouse reports none by design.",
            "",
        ]
    return lines


def _finding_weekly(weekly: pd.DataFrame) -> List[str]:
    if weekly.empty or weekly["weekend_minus_weekday"].isna().all():
        return []
    ranked = weekly.dropna(subset=["weekend_minus_weekday"]).sort_values(
        "weekend_minus_weekday"
    )
    cleaner_weekends = int((ranked["weekend_minus_weekday"] < 0).sum())
    no2_gap = ranked["weekend_mean_no2"] - ranked["weekday_mean_no2"]

    return [
        "### 5. Weekends are measurably cleaner than weekdays",
        "",
        f"The PM2.5 weekend effect runs from "
        f"{_fmt(ranked['weekend_minus_weekday'].min(), 1)} to "
        f"{_fmt(ranked['weekend_minus_weekday'].max(), 1)} ug/m3, and is negative "
        f"(cleaner weekends) in **{cleaner_weekends} of {len(ranked)}** cities.",
        "",
        f"NO2 moves the same way — {_fmt(no2_gap.min(), 1)} to "
        f"{_fmt(no2_gap.max(), 1)} ug/m3 between weekend and weekday. NO2 is "
        f"short-lived and traffic-dominated, so a weekly cycle in it is a "
        f"fingerprint of human activity rather than of weather: meteorology has "
        f"no reason to know what day of the week it is.",
        "",
        _md_table(ranked),
        "",
    ]


def render_findings(results: Dict[str, pd.DataFrame]) -> str:
    """Compose docs/FINDINGS.md from the query results."""
    seasonal = results.get("Q1", pd.DataFrame())
    trend = results.get("Q2", pd.DataFrame())
    breach = results.get("Q3", pd.DataFrame())
    spikes = results.get("Q4", pd.DataFrame())
    streaks = results.get("Q5", pd.DataFrame())
    persistence = results.get("Q6", pd.DataFrame())
    weekly = results.get("Q7", pd.DataFrame())
    health = results.get("Q8", pd.DataFrame())
    runs = results.get("Q9", pd.DataFrame())

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    parts: List[str] = [
        "# Findings",
        "",
        "> Generated by `python -m src.report` from the warehouse itself. Every "
        "number below comes from a query in "
        "[`sql/03_analytical_queries.sql`](../sql/03_analytical_queries.sql) run "
        "against `aqi_pipeline.mart_aqi_analytics`. Re-run the command after a "
        "pipeline run to refresh it.",
        "",
        f"- **Generated:** {generated}",
        f"- **Coverage:** {_coverage_line(health)}",
        f"- **Standards:** CPCB NAAQS 24-h PM2.5 = {CPCB_PM25_SAFE_LIMIT:g} ug/m3, "
        f"WHO 2021 24-h guideline = {WHO_PM25_SAFE_LIMIT:g} ug/m3",
        "",
        "A short window shows weather; a long one shows climate. Read the "
        "coverage line above before reading a seasonal claim below.",
        "",
        "---",
        "",
    ]

    parts += _finding_gradient(breach)
    parts += ["---", ""]
    parts += _finding_seasonality(seasonal, trend)
    parts += ["---", ""]
    parts += _finding_persistence(persistence)
    parts += ["---", ""]
    parts += _finding_anomaly_vs_threshold(breach, spikes, streaks)
    parts += ["---", ""]
    parts += _finding_weekly(weekly)
    parts += ["---", ""]

    parts += [
        "## Data quality and freshness",
        "",
        "Coverage and completeness per city (Q8). `partial_days` counts days "
        "built from fewer than 24 hourly observations — normally just the "
        "current day, which is still filling.",
        "",
        _md_table(health),
        "",
    ]
    if not runs.empty:
        parts += [
            "Recent pipeline invocations (Q9), from the `pipeline_runs` audit table:",
            "",
            _md_table(runs, max_rows=10),
            "",
        ]

    parts += [
        "---",
        "",
        "## How to read the anomaly flag",
        "",
        "`is_statistical_anomaly` is true when a day's mean PM2.5 is more than "
        "2 sigma above the mean of the **preceding** 30 days, and only once at "
        "least 14 of those days exist. Excluding the current day matters: a day "
        "bad enough to matter would otherwise pull up the very baseline it is "
        "compared against, and the flag would under-report exactly the episodes "
        "it exists to catch.",
        "",
        "`is_cpcb_breach` and `is_who_breach` are plain threshold tests and stay "
        "in the mart alongside it. The two answer different questions: the "
        "threshold says *is this air legal to breathe*, the Z-score says *is "
        "today unusual for this city*.",
        "",
    ]
    return "\n".join(parts) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Render docs/FINDINGS.md from the warehouse.")
    parser.add_argument("--stdout", action="store_true", help="Print instead of writing the file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DOCS_DIR / "FINDINGS.md",
        help="Output path (default docs/FINDINGS.md).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    engine = create_db_engine()
    markdown = render_findings(run_queries(engine))

    if args.stdout:
        print(markdown)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown, encoding="utf-8")
        logger.info("Wrote %s (%d chars).", args.output, len(markdown))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
