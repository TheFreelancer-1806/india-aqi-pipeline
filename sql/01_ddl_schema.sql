-- =============================================================================
-- sql/01_ddl_schema.sql
-- India Metro AQI pipeline — three-tier (medallion) schema.
--
--   stg_air_quality   (bronze) append-only raw hourly landing zone
--   clean_daily_aqi   (silver) deduplicated daily grain, constrained
--   mart_aqi_analytics(gold)   window functions, LAG deltas, anomaly flags
--
-- Idempotent: safe to run on every pipeline invocation.
-- Run standalone with:  psql "$DATABASE_URL" -f sql/01_ddl_schema.sql
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS aqi_pipeline;


-- -----------------------------------------------------------------------------
-- 1. STAGING — raw hourly observations exactly as the API returned them.
--    Append-only. Pruned on a rolling window by 02_transform_load.sql.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aqi_pipeline.stg_air_quality (
    ingestion_id     BIGSERIAL PRIMARY KEY,
    city             VARCHAR(50)  NOT NULL,
    latitude         NUMERIC(7,4) NOT NULL,
    longitude        NUMERIC(7,4) NOT NULL,
    observation_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- IST wall clock
    pm2_5            NUMERIC(8,2),
    pm10             NUMERIC(8,2),
    no2              NUMERIC(8,2),
    so2              NUMERIC(8,2),
    co               NUMERIC(10,2),
    us_aqi           INTEGER,
    ingested_at      TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_city_obs_time
    ON aqi_pipeline.stg_air_quality (city, observation_time);

CREATE INDEX IF NOT EXISTS idx_stg_ingested_at
    ON aqi_pipeline.stg_air_quality (ingested_at);


-- -----------------------------------------------------------------------------
-- 2. CLEAN — one row per city per calendar day (IST), upserted.
--    valid_hours_count records how much of the day the average is built from,
--    which 02_transform_load.sql uses to refuse partial-day regressions.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aqi_pipeline.clean_daily_aqi (
    city              VARCHAR(50)  NOT NULL,
    obs_date          DATE         NOT NULL,
    avg_pm2_5         NUMERIC(8,2) NOT NULL CHECK (avg_pm2_5 >= 0),
    max_pm2_5         NUMERIC(8,2) NOT NULL CHECK (max_pm2_5 >= 0),
    min_pm2_5         NUMERIC(8,2) NOT NULL CHECK (min_pm2_5 >= 0),
    avg_pm10          NUMERIC(8,2) NOT NULL CHECK (avg_pm10  >= 0),
    avg_no2           NUMERIC(8,2) NOT NULL CHECK (avg_no2   >= 0),
    avg_so2           NUMERIC(8,2) NOT NULL CHECK (avg_so2   >= 0),
    avg_co            NUMERIC(10,2) NOT NULL CHECK (avg_co   >= 0),
    avg_us_aqi        INTEGER      NOT NULL CHECK (avg_us_aqi >= 0),
    valid_hours_count INTEGER      NOT NULL CHECK (valid_hours_count BETWEEN 1 AND 24),
    updated_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, obs_date),
    CONSTRAINT chk_pm25_min_le_max CHECK (min_pm2_5 <= max_pm2_5),
    CONSTRAINT chk_pm25_avg_in_range CHECK (avg_pm2_5 BETWEEN min_pm2_5 AND max_pm2_5)
);

CREATE INDEX IF NOT EXISTS idx_clean_obs_date
    ON aqi_pipeline.clean_daily_aqi (obs_date);


-- -----------------------------------------------------------------------------
-- 3. RUN LOG — one row per pipeline invocation, so the dashboard can show
--    pipeline freshness and a scheduled run that silently stops is visible.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aqi_pipeline.pipeline_runs (
    run_id            BIGSERIAL PRIMARY KEY,
    started_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at       TIMESTAMP WITH TIME ZONE,
    status            VARCHAR(20) NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'partial', 'failed')),
    mode              VARCHAR(20),          -- 'backfill' | 'incremental'
    past_days         INTEGER,
    cities_requested  INTEGER,
    cities_succeeded  INTEGER,
    rows_staged       INTEGER,
    clean_rows_total  INTEGER,
    duration_seconds  NUMERIC(10,2),
    message           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON aqi_pipeline.pipeline_runs (started_at DESC);


-- -----------------------------------------------------------------------------
-- 4. MART — the analytical layer. All derived metrics live here as SQL window
--    functions over clean_daily_aqi, so the dashboard stays a thin renderer.
--
--    Window frames use RANGE ... INTERVAL rather than ROWS, so a missing day
--    (API outage) shortens the window instead of silently reaching further back
--    in calendar time. rolling_7d_days exposes how many days actually landed in
--    the frame.
--
--    The anomaly baseline deliberately EXCLUDES the current row
--    (RANGE ... '30 days' PRECEDING AND '1 day' PRECEDING): a spike must be
--    measured against the days before it, not against a mean it inflated itself.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW aqi_pipeline.mart_aqi_analytics AS
WITH windowed_base AS (
    SELECT
        city,
        obs_date,
        avg_pm2_5,
        max_pm2_5,
        min_pm2_5,
        avg_pm10,
        avg_no2,
        avg_so2,
        avg_co,
        avg_us_aqi,
        valid_hours_count,

        -- Day-over-day and week-over-week via LAG(). prev_obs_date makes a gap
        -- in the series visible instead of passing it off as "yesterday".
        LAG(avg_pm2_5, 1) OVER w            AS prev_day_pm2_5,
        LAG(obs_date, 1)  OVER w            AS prev_obs_date,
        LAG(avg_pm2_5, 7) OVER w            AS prev_week_pm2_5,
        LAG(obs_date, 7)  OVER w            AS prev_week_obs_date,

        -- 7-day trailing rolling averages (calendar-aware).
        AVG(avg_pm2_5) OVER w7              AS rolling_7d_pm2_5_raw,
        AVG(avg_pm10)  OVER w7              AS rolling_7d_pm10_raw,
        AVG(avg_us_aqi) OVER w7             AS rolling_7d_us_aqi_raw,
        COUNT(*) OVER w7                    AS rolling_7d_days,

        -- 30-day baseline for the Z-score, current day excluded.
        AVG(avg_pm2_5)        OVER w30b     AS baseline_30d_mean,
        STDDEV_SAMP(avg_pm2_5) OVER w30b    AS baseline_30d_std,
        COUNT(*)              OVER w30b     AS baseline_30d_days,

        -- Running count of CPCB breaches within the calendar year.
        SUM(CASE WHEN avg_pm2_5 > 60 THEN 1 ELSE 0 END) OVER (
            PARTITION BY city, EXTRACT(YEAR FROM obs_date)
            ORDER BY obs_date
            ROWS UNBOUNDED PRECEDING
        )                                   AS ytd_cpcb_breach_days
    FROM aqi_pipeline.clean_daily_aqi
    WINDOW
        w AS (PARTITION BY city ORDER BY obs_date),
        w7 AS (
            PARTITION BY city ORDER BY obs_date
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ),
        w30b AS (
            PARTITION BY city ORDER BY obs_date
            RANGE BETWEEN INTERVAL '30 days' PRECEDING
                      AND INTERVAL '1 day'  PRECEDING
        )
)
SELECT
    city,
    obs_date,
    TO_CHAR(obs_date, 'YYYY-MM')                 AS year_month,
    EXTRACT(ISODOW FROM obs_date)::INTEGER       AS iso_day_of_week,
    (EXTRACT(ISODOW FROM obs_date) >= 6)         AS is_weekend,

    -- Indian Meteorological Department season bands.
    CASE
        WHEN EXTRACT(MONTH FROM obs_date) IN (12, 1, 2)   THEN 'Winter (Inversion)'
        WHEN EXTRACT(MONTH FROM obs_date) IN (3, 4, 5)    THEN 'Pre-Monsoon (Summer)'
        WHEN EXTRACT(MONTH FROM obs_date) IN (6, 7, 8, 9) THEN 'Southwest Monsoon'
        ELSE 'Post-Monsoon (Oct-Nov)'
    END                                          AS indian_season,

    avg_pm2_5,
    max_pm2_5,
    min_pm2_5,
    avg_pm10,
    avg_no2,
    avg_so2,
    avg_co,
    avg_us_aqi,
    valid_hours_count,
    ROUND(valid_hours_count * 100.0 / 24, 1)     AS data_completeness_pct,

    -- A high PM2.5/PM10 ratio points at fine combustion and secondary aerosol;
    -- a low ratio at coarse road and crustal dust.
    ROUND(avg_pm2_5 / NULLIF(avg_pm10, 0), 3)    AS pm25_to_pm10_ratio,

    -- Day-over-day (LAG) deltas.
    prev_day_pm2_5,
    prev_obs_date,
    (obs_date - prev_obs_date)                   AS days_since_prev_obs,
    ROUND(avg_pm2_5 - prev_day_pm2_5, 2)         AS dod_pm2_5_abs_change,
    ROUND(((avg_pm2_5 - prev_day_pm2_5) / NULLIF(prev_day_pm2_5, 0)) * 100, 2)
                                                 AS dod_pm2_5_pct_change,

    -- Week-over-week (LAG 7) deltas.
    prev_week_pm2_5,
    prev_week_obs_date,
    ROUND(((avg_pm2_5 - prev_week_pm2_5) / NULLIF(prev_week_pm2_5, 0)) * 100, 2)
                                                 AS wow_pm2_5_pct_change,

    -- Rolling averages.
    ROUND(rolling_7d_pm2_5_raw, 2)               AS rolling_7d_pm2_5,
    ROUND(rolling_7d_pm10_raw, 2)                AS rolling_7d_pm10,
    ROUND(rolling_7d_us_aqi_raw, 1)              AS rolling_7d_us_aqi,
    rolling_7d_days,
    (rolling_7d_days >= 7)                       AS rolling_7d_is_full_window,

    -- Anomaly baseline + Z-score.
    ROUND(baseline_30d_mean, 2)                  AS baseline_30d_mean,
    ROUND(baseline_30d_std, 2)                   AS baseline_30d_std,
    baseline_30d_days,
    ROUND((avg_pm2_5 - baseline_30d_mean) / NULLIF(baseline_30d_std, 0), 2)
                                                 AS z_score_30d,

    ytd_cpcb_breach_days,

    -- CPCB 24-hour PM2.5 sub-index breakpoints (National AQI, ug/m3).
    CASE
        WHEN avg_pm2_5 > 250 THEN 'Severe'
        WHEN avg_pm2_5 > 120 THEN 'Very Poor'
        WHEN avg_pm2_5 > 90  THEN 'Poor'
        WHEN avg_pm2_5 > 60  THEN 'Moderate'
        WHEN avg_pm2_5 > 30  THEN 'Satisfactory'
        ELSE 'Good'
    END                                          AS cpcb_category,

    -- Threshold breaches: CPCB NAAQS 24-h standard, and the stricter WHO 2021
    -- 24-h guideline.
    (avg_pm2_5 > 60.0)                           AS is_cpcb_breach,
    (avg_pm2_5 > 15.0)                           AS is_who_breach,

    -- Statistical anomaly: >2 sigma above the trailing 30-day baseline, and only
    -- once that baseline has at least 14 days behind it, so the first fortnight
    -- of a new city does not flag on noise.
    (
        baseline_30d_days >= 14
        AND baseline_30d_std > 0
        AND (avg_pm2_5 - baseline_30d_mean) / baseline_30d_std > 2.0
    )                                            AS is_statistical_anomaly
FROM windowed_base;


-- -----------------------------------------------------------------------------
-- 5. CONVENIENCE VIEW — latest observed day per city, for KPI cards.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW aqi_pipeline.mart_latest_city_snapshot AS
SELECT DISTINCT ON (city) *
FROM aqi_pipeline.mart_aqi_analytics
ORDER BY city, obs_date DESC;
