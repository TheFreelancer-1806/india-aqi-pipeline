"""Self-refreshing Streamlit dashboard over aqi_pipeline.mart_aqi_analytics.

The dashboard computes nothing. Every rolling average, LAG delta, category and
anomaly flag is read from the SQL mart, so the numbers on screen and the numbers
an analyst gets from psql cannot drift apart.

    streamlit run app.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

from src.config import (
    CITY_ZONES,
    CPCB_PM25_SAFE_LIMIT,
    WHO_PM25_SAFE_LIMIT,
)
from src.db import create_db_engine, read_sql_frame

load_dotenv()

CACHE_TTL_SECONDS = 1800  # 30 min: the pipeline runs daily, so this is ample

CATEGORY_COLOURS = {
    "Good": "#2A9D8F",
    "Satisfactory": "#8AB17D",
    "Moderate": "#E9C46A",
    "Poor": "#F4A261",
    "Very Poor": "#E76F51",
    "Severe": "#9B2226",
}

st.set_page_config(
    page_title="India Metro AQI Monitor",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------
@st.cache_resource
def get_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        try:
            url = st.secrets["DATABASE_URL"]
        except Exception:  # noqa: BLE001 - no secrets file configured
            url = None
    if not url:
        st.error(
            "`DATABASE_URL` is not configured. Set it in `.env` for a local run, "
            "or under **App settings → Secrets** on Streamlit Community Cloud."
        )
        st.stop()
    return create_db_engine(url)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying the analytical mart…")
def load_mart() -> pd.DataFrame:
    df = read_sql_frame(
        """
        SELECT * FROM aqi_pipeline.mart_aqi_analytics
        ORDER BY city, obs_date
        """,
        get_engine(),
    )
    if not df.empty:
        df["obs_date"] = pd.to_datetime(df["obs_date"])
        df["zone"] = df["city"].map(CITY_ZONES).fillna("Unknown")
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_last_run() -> pd.DataFrame:
    return read_sql_frame(
        """
        SELECT run_id, started_at, finished_at, status, mode, past_days,
               cities_succeeded, cities_requested, rows_staged,
               clean_rows_total, duration_seconds, message
        FROM aqi_pipeline.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 20
        """,
        get_engine(),
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_breach_streaks() -> pd.DataFrame:
    """Longest unbroken CPCB breach runs, via a gaps-and-islands query."""
    return read_sql_frame(
        """
        WITH breach_days AS (
            SELECT city, obs_date,
                   obs_date - (ROW_NUMBER() OVER (PARTITION BY city ORDER BY obs_date))::INTEGER
                       AS streak_key
            FROM aqi_pipeline.mart_aqi_analytics
            WHERE is_cpcb_breach
        )
        SELECT city,
               MIN(obs_date) AS streak_start,
               MAX(obs_date) AS streak_end,
               COUNT(*)      AS consecutive_breach_days
        FROM breach_days
        GROUP BY city, streak_key
        ORDER BY consecutive_breach_days DESC, streak_start DESC
        LIMIT 12
        """,
        get_engine(),
    )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar(df: pd.DataFrame):
    with st.sidebar:
        st.header("Filters")

        cities = sorted(df["city"].unique())
        selected_cities = st.multiselect("Cities", options=cities, default=cities)

        min_date = df["obs_date"].min().date()
        max_date = df["obs_date"].max().date()
        date_range = st.date_input(
            "Date window",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        st.divider()
        st.caption(
            f"Data is cached for {CACHE_TTL_SECONDS // 60} minutes and re-queried "
            "automatically after that. The pipeline itself runs daily at 01:00 UTC "
            "on GitHub Actions."
        )
        if st.button("Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption(
            f"**Standards** · CPCB NAAQS 24-h PM2.5 {CPCB_PM25_SAFE_LIMIT:g} µg/m³ "
            f"· WHO 2021 24-h {WHO_PM25_SAFE_LIMIT:g} µg/m³"
        )

    if not selected_cities:
        selected_cities = cities

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    else:
        start, end = pd.Timestamp(min_date), pd.Timestamp(max_date)

    return selected_cities, start, end


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
def render_header(df: pd.DataFrame, runs: pd.DataFrame) -> None:
    st.title("India Metro Air Quality — Automated Pipeline")
    st.caption(
        "Open-Meteo Air Quality API → PostgreSQL medallion schema "
        "(staging → clean → mart) → this dashboard. "
        "Rolling averages, day-over-day deltas and anomaly flags are SQL window "
        "functions, not dashboard code."
    )

    latest_obs = df["obs_date"].max()
    cols = st.columns(4)
    cols[0].metric("Latest observed day", latest_obs.strftime("%Y-%m-%d"))
    cols[1].metric("City-days loaded", f"{len(df):,}")

    if runs.empty:
        cols[2].metric("Last pipeline run", "no runs logged")
        cols[3].metric("Pipeline status", "unknown")
        return

    successful = runs[runs["status"].isin(["success", "partial"])]
    if successful.empty:
        cols[2].metric("Last successful run", "none")
    else:
        last = successful.iloc[0]
        started = pd.to_datetime(last["started_at"], utc=True)
        age_hours = (datetime.now(timezone.utc) - started.to_pydatetime()).total_seconds() / 3600
        cols[2].metric(
            "Last successful run",
            started.strftime("%Y-%m-%d %H:%M UTC"),
            delta=f"{age_hours:.1f} h ago",
            delta_color="off",
        )

    newest = runs.iloc[0]
    cols[3].metric(
        "Latest run status",
        str(newest["status"]),
        delta=f"{newest['mode']} · {newest['rows_staged'] or 0:,.0f} rows staged",
        delta_color="off",
    )

    if newest["status"] == "failed":
        st.error(f"The most recent pipeline run failed: {newest['message']}")
    elif newest["status"] == "partial":
        st.warning(f"The most recent run completed with city failures: {newest['message']}")


# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
def render_kpis(df: pd.DataFrame) -> None:
    st.subheader("Latest day per city — value, day-over-day LAG delta, flag")

    latest = (
        df.sort_values("obs_date")
        .groupby("city", as_index=False)
        .tail(1)
        .sort_values("avg_pm2_5", ascending=False)
    )
    if latest.empty:
        st.info("No rows in the selected window.")
        return

    for row_start in range(0, len(latest), 3):
        chunk = latest.iloc[row_start : row_start + 3]
        cols = st.columns(len(chunk))
        for col, (_, row) in zip(cols, chunk.iterrows()):
            with col:
                dod = row["dod_pm2_5_pct_change"]
                st.metric(
                    label=f"{row['city']} · {row['cpcb_category']}",
                    value=f"{row['avg_pm2_5']:.1f} µg/m³",
                    delta=("n/a" if pd.isna(dod) else f"{dod:+.1f}% DoD"),
                    delta_color="inverse",
                    help=(
                        f"{row['obs_date'].date()} · {row['zone']} · "
                        f"{int(row['valid_hours_count'])}/24 hours observed · "
                        f"7-day rolling {row['rolling_7d_pm2_5']:.1f} µg/m³"
                    ),
                )
                if bool(row["is_statistical_anomaly"]):
                    st.error(f"Acute episode · Z = {row['z_score_30d']:+.2f}σ")
                elif bool(row["is_cpcb_breach"]):
                    st.warning(f"CPCB breach · 7-day {row['rolling_7d_pm2_5']:.0f}")
                elif bool(row["is_who_breach"]):
                    st.info(f"Within CPCB, above WHO · 7-day {row['rolling_7d_pm2_5']:.0f}")
                else:
                    st.success(f"Within both standards · 7-day {row['rolling_7d_pm2_5']:.0f}")


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
def tab_trends(df: pd.DataFrame) -> None:
    st.subheader("7-day rolling PM2.5 against both standards")
    st.caption(
        "The line is `rolling_7d_pm2_5` from the mart — a calendar-aware "
        "`AVG() OVER (RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)`. "
        "Crosses mark days flagged `is_statistical_anomaly` (>2σ above the "
        "preceding 30-day baseline)."
    )

    fig = px.line(
        df.sort_values("obs_date"),
        x="obs_date",
        y="rolling_7d_pm2_5",
        color="city",
        labels={
            "rolling_7d_pm2_5": "7-day rolling PM2.5 (µg/m³)",
            "obs_date": "Date",
            "city": "City",
        },
        template="plotly_white",
    )
    fig.add_hline(
        y=CPCB_PM25_SAFE_LIMIT,
        line_dash="dash",
        line_color="#D62828",
        annotation_text=f"CPCB {CPCB_PM25_SAFE_LIMIT:g}",
        annotation_position="top left",
    )
    fig.add_hline(
        y=WHO_PM25_SAFE_LIMIT,
        line_dash="dot",
        line_color="#2A9D8F",
        annotation_text=f"WHO {WHO_PM25_SAFE_LIMIT:g}",
        annotation_position="bottom left",
    )
    anomalies = df[df["is_statistical_anomaly"].fillna(False).astype(bool)]
    if not anomalies.empty:
        fig.add_trace(
            go.Scatter(
                x=anomalies["obs_date"],
                y=anomalies["avg_pm2_5"],
                mode="markers",
                marker=dict(color="#9B2226", size=11, symbol="x"),
                name="Acute episode (Z > 2σ)",
                customdata=anomalies[["city", "z_score_30d"]].to_numpy(),
                hovertemplate=(
                    "%{customdata[0]}<br>%{x|%Y-%m-%d}<br>"
                    "Daily PM2.5 %{y:.1f} µg/m³<br>Z = %{customdata[1]:.2f}σ<extra></extra>"
                ),
            )
        )
    fig.update_layout(height=460, hovermode="x unified", legend_title_text="")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily value against its own rolling baseline")
    city = st.selectbox("City", options=sorted(df["city"].unique()))
    one = df[df["city"] == city].sort_values("obs_date")

    detail = go.Figure()
    detail.add_trace(
        go.Bar(
            x=one["obs_date"],
            y=one["avg_pm2_5"],
            name="Daily mean",
            marker_color=one["cpcb_category"].map(CATEGORY_COLOURS).fillna("#9AA5B1"),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.1f} µg/m³<extra></extra>",
        )
    )
    detail.add_trace(
        go.Scatter(
            x=one["obs_date"],
            y=one["rolling_7d_pm2_5"],
            name="7-day rolling",
            line=dict(color="#1D3557", width=2.5),
        )
    )
    detail.add_trace(
        go.Scatter(
            x=one["obs_date"],
            y=one["baseline_30d_mean"],
            name="30-day baseline (excl. today)",
            line=dict(color="#6C757D", width=1.5, dash="dot"),
        )
    )
    detail.add_hline(y=CPCB_PM25_SAFE_LIMIT, line_dash="dash", line_color="#D62828")
    detail.update_layout(
        template="plotly_white",
        height=420,
        barmode="overlay",
        yaxis_title="PM2.5 (µg/m³)",
        xaxis_title="Date",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(detail, use_container_width=True)


def tab_seasonality(df: pd.DataFrame) -> None:
    st.subheader("Distribution by Indian meteorological season")
    st.caption(
        "Each box is the spread of daily means for that city within that season. "
        "A tall box means air quality set by weather; a short one means emissions "
        "that do not let up."
    )
    fig_box = px.box(
        df,
        x="indian_season",
        y="avg_pm2_5",
        color="city",
        points=False,
        category_orders={
            "indian_season": [
                "Winter (Inversion)",
                "Pre-Monsoon (Summer)",
                "Southwest Monsoon",
                "Post-Monsoon (Oct-Nov)",
            ]
        },
        labels={"indian_season": "Season", "avg_pm2_5": "Daily mean PM2.5 (µg/m³)"},
        template="plotly_white",
    )
    fig_box.add_hline(y=CPCB_PM25_SAFE_LIMIT, line_dash="dash", line_color="#D62828")
    fig_box.update_layout(height=440, legend_title_text="")
    st.plotly_chart(fig_box, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.subheader("Monthly rolling baseline")
        monthly = (
            df.groupby(["city", "year_month"], as_index=False)
            .agg(mean_rolling_7d=("rolling_7d_pm2_5", "mean"))
            .pivot(index="city", columns="year_month", values="mean_rolling_7d")
        )
        if monthly.empty:
            st.info("Not enough data for a monthly view.")
        else:
            fig_heat = px.imshow(
                monthly.round(1),
                color_continuous_scale="YlOrRd",
                aspect="auto",
                labels=dict(x="Month", y="City", color="µg/m³"),
                text_auto=True,
                template="plotly_white",
            )
            fig_heat.update_layout(height=380)
            st.plotly_chart(fig_heat, use_container_width=True)

    with right:
        st.subheader("Aerosol signature: PM2.5/PM10 against NO2")
        st.caption(
            "A high PM2.5/PM10 ratio with high NO2 points at combustion and "
            "secondary aerosol; a low ratio at coarse road and crustal dust."
        )
        fig_scatter = px.scatter(
            df.dropna(subset=["pm25_to_pm10_ratio"]),
            x="avg_no2",
            y="pm25_to_pm10_ratio",
            color="city",
            size="avg_pm2_5",
            size_max=18,
            opacity=0.65,
            hover_data=["obs_date", "cpcb_category", "indian_season"],
            labels={
                "avg_no2": "Daily mean NO2 (µg/m³)",
                "pm25_to_pm10_ratio": "PM2.5 / PM10",
            },
            template="plotly_white",
        )
        fig_scatter.update_layout(height=380, legend_title_text="")
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.subheader("Weekday cycle")
    weekday_names = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}
    weekly = (
        df.assign(day=df["iso_day_of_week"].map(weekday_names))
        .groupby(["city", "day"], as_index=False)
        .agg(mean_pm2_5=("avg_pm2_5", "mean"), mean_no2=("avg_no2", "mean"))
    )
    fig_week = px.line(
        weekly,
        x="day",
        y="mean_pm2_5",
        color="city",
        markers=True,
        category_orders={"day": list(weekday_names.values())},
        labels={"day": "ISO day of week", "mean_pm2_5": "Mean PM2.5 (µg/m³)"},
        template="plotly_white",
    )
    fig_week.update_layout(height=360, legend_title_text="")
    st.plotly_chart(fig_week, use_container_width=True)


def tab_breaches(df: pd.DataFrame, streaks: pd.DataFrame) -> None:
    summary = (
        df.groupby("city", as_index=False)
        .agg(
            days=("obs_date", "count"),
            mean_pm2_5=("avg_pm2_5", "mean"),
            cpcb_days=("is_cpcb_breach", "sum"),
            who_days=("is_who_breach", "sum"),
            anomalies=("is_statistical_anomaly", "sum"),
        )
        .assign(
            cpcb_pct=lambda d: (d["cpcb_days"] / d["days"] * 100).round(1),
            who_pct=lambda d: (d["who_days"] / d["days"] * 100).round(1),
        )
    )

    left, right = st.columns([3, 2])

    with left:
        st.subheader("Share of days in breach of each standard")
        melted = summary.melt(
            id_vars=["city"],
            value_vars=["cpcb_pct", "who_pct"],
            var_name="standard",
            value_name="pct",
        ).replace(
            {
                "standard": {
                    "cpcb_pct": f"CPCB (>{CPCB_PM25_SAFE_LIMIT:g})",
                    "who_pct": f"WHO (>{WHO_PM25_SAFE_LIMIT:g})",
                }
            }
        )
        fig = px.bar(
            melted.sort_values("pct"),
            x="pct",
            y="city",
            color="standard",
            orientation="h",
            barmode="group",
            text="pct",
            labels={"pct": "% of observed days", "city": "", "standard": ""},
            color_discrete_map={
                f"CPCB (>{CPCB_PM25_SAFE_LIMIT:g})": "#D62828",
                f"WHO (>{WHO_PM25_SAFE_LIMIT:g})": "#F4A261",
            },
            template="plotly_white",
        )
        fig.update_traces(texttemplate="%{text:.0f}%", textposition="outside", cliponaxis=False)
        fig.update_layout(height=420, xaxis_range=[0, 112])
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Days in each CPCB band")
        band_order = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
        bands = (
            df.groupby(["city", "cpcb_category"], as_index=False)
            .agg(days=("obs_date", "count"))
        )
        fig_band = px.bar(
            bands,
            x="days",
            y="city",
            color="cpcb_category",
            orientation="h",
            category_orders={"cpcb_category": band_order},
            color_discrete_map=CATEGORY_COLOURS,
            labels={"days": "Days", "city": "", "cpcb_category": ""},
            template="plotly_white",
        )
        fig_band.update_layout(height=420, legend=dict(orientation="h", y=-0.18))
        st.plotly_chart(fig_band, use_container_width=True)

    st.subheader("Longest unbroken CPCB breach streaks")
    st.caption(
        "Computed in SQL with a gaps-and-islands pattern: "
        "`obs_date - ROW_NUMBER() OVER (PARTITION BY city ORDER BY obs_date)` "
        "is constant across an unbroken run of breach days."
    )
    if streaks.empty:
        st.success("No day in this warehouse breaches the CPCB 24-hour standard.")
    else:
        st.dataframe(streaks, use_container_width=True, hide_index=True)

    st.subheader("Acute episodes isolated by the rolling Z-score")
    spikes = (
        df[df["is_statistical_anomaly"].fillna(False).astype(bool)]
        .sort_values("z_score_30d", ascending=False)
        .loc[
            :,
            [
                "city",
                "obs_date",
                "indian_season",
                "avg_pm2_5",
                "rolling_7d_pm2_5",
                "baseline_30d_mean",
                "z_score_30d",
                "dod_pm2_5_pct_change",
                "cpcb_category",
            ],
        ]
    )
    if spikes.empty:
        st.info(
            "No day exceeds 2σ above its preceding 30-day baseline in this window. "
            "The flag needs 14 baseline days before it will fire, so a freshly "
            "backfilled warehouse reports none by design."
        )
    else:
        st.dataframe(
            spikes.head(25),
            use_container_width=True,
            hide_index=True,
            column_config={
                "obs_date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            },
        )


def tab_explorer(df: pd.DataFrame, runs: pd.DataFrame) -> None:
    st.subheader("`aqi_pipeline.mart_aqi_analytics`")
    st.caption(
        "The analytical mart exactly as the view returns it, filtered to the "
        "sidebar selection. Every column here is produced in SQL."
    )
    st.dataframe(
        df.sort_values(["obs_date", "city"], ascending=[False, True]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "obs_date": st.column_config.DateColumn("obs_date", format="YYYY-MM-DD"),
        },
    )
    st.download_button(
        "Download this selection as CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="aqi_mart_selection.csv",
        mime="text/csv",
    )

    st.subheader("`aqi_pipeline.pipeline_runs`")
    st.caption("Scheduling audit trail. A gap here means a scheduled run did not happen.")
    if runs.empty:
        st.info("No pipeline runs logged yet.")
    else:
        st.dataframe(runs, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        mart = load_mart()
        runs = load_last_run()
    except Exception as exc:  # noqa: BLE001 - surface connection problems in the UI
        st.error(f"Could not read the warehouse: {type(exc).__name__}: {exc}")
        st.info(
            "Check `DATABASE_URL`, then run `python -m src.etl` to create the "
            "schema and load data."
        )
        st.stop()
        return

    if mart.empty:
        st.warning(
            "`aqi_pipeline.mart_aqi_analytics` is empty. Run `python -m src.etl` "
            "to backfill, then refresh."
        )
        if not runs.empty:
            st.dataframe(runs, use_container_width=True, hide_index=True)
        st.stop()
        return

    render_header(mart, runs)
    cities, start, end = render_sidebar(mart)

    filtered = mart[
        mart["city"].isin(cities) & mart["obs_date"].between(start, end)
    ].copy()

    st.divider()
    render_kpis(filtered if not filtered.empty else mart)
    st.divider()

    trends, seasonality, breaches, explorer = st.tabs(
        ["Trends", "Seasonality", "Breaches & anomalies", "Data explorer"]
    )
    with trends:
        tab_trends(filtered)
    with seasonality:
        tab_seasonality(filtered)
    with breaches:
        tab_breaches(filtered, load_breach_streaks())
    with explorer:
        tab_explorer(filtered, runs)


main()
