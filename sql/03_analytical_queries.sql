-- =============================================================================
-- sql/03_analytical_queries.sql
-- Standalone analytical queries over aqi_pipeline.mart_aqi_analytics.
--
-- These are the queries behind docs/FINDINGS.md. Run them individually in psql
-- or a SQL client; src/report.py executes the same set and renders the results.
-- =============================================================================


-- Q1 — Seasonal profile per city.
--      Seasonality: how the daily mean, the aerosol signature and the breach
--      rate move between the monsoon washout and the winter inversion.
SELECT
    city,
    indian_season,
    COUNT(*)                                                        AS days_observed,
    ROUND(AVG(avg_pm2_5), 1)                                        AS mean_pm2_5,
    ROUND(MAX(avg_pm2_5), 1)                                        AS peak_daily_pm2_5,
    ROUND(AVG(pm25_to_pm10_ratio), 3)                               AS avg_combustion_ratio,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_cpcb_breach) / COUNT(*), 1) AS cpcb_breach_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_who_breach)  / COUNT(*), 1) AS who_breach_pct
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY city, indian_season
ORDER BY city, mean_pm2_5 DESC;


-- Q2 — Trend: month-over-month rolling baseline per city.
--      Uses the mart's own 7-day rolling average so each month is represented by
--      a smoothed level rather than a noisy daily mean.
SELECT
    year_month,
    city,
    COUNT(*)                            AS days,
    ROUND(AVG(rolling_7d_pm2_5), 1)     AS mean_rolling_7d_pm2_5,
    ROUND(AVG(avg_pm2_5), 1)            AS mean_daily_pm2_5,
    ROUND(AVG(avg_us_aqi), 0)           AS mean_us_aqi
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY year_month, city
ORDER BY city, year_month;


-- Q3 — Threshold breaches: how often each city exceeds each standard, and how
--      much of the year it has already spent in breach.
SELECT
    city,
    COUNT(*)                                                            AS days_observed,
    ROUND(AVG(avg_pm2_5), 1)                                            AS mean_pm2_5,
    COUNT(*) FILTER (WHERE is_cpcb_breach)                              AS cpcb_breach_days,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_cpcb_breach) / COUNT(*), 1) AS cpcb_breach_pct,
    COUNT(*) FILTER (WHERE is_who_breach)                               AS who_breach_days,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_who_breach)  / COUNT(*), 1) AS who_breach_pct,
    COUNT(*) FILTER (WHERE cpcb_category IN ('Very Poor', 'Severe'))    AS very_poor_or_worse_days,
    MAX(ytd_cpcb_breach_days)                                           AS ytd_cpcb_breach_days
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY city
ORDER BY cpcb_breach_pct DESC, mean_pm2_5 DESC;


-- Q4 — Acute spikes isolated by the rolling Z-score rather than the static
--      threshold. These are the days a city broke away from its own baseline.
SELECT
    city,
    obs_date,
    indian_season,
    avg_pm2_5,
    rolling_7d_pm2_5,
    baseline_30d_mean,
    dod_pm2_5_pct_change,
    z_score_30d,
    cpcb_category
FROM aqi_pipeline.mart_aqi_analytics
WHERE is_statistical_anomaly
ORDER BY z_score_30d DESC
LIMIT 15;


-- Q5 — Longest consecutive CPCB breach streaks (gaps-and-islands).
--      Subtracting a per-city row number from the date collapses each unbroken
--      run of breach days onto a single constant, which then groups.
WITH breach_days AS (
    SELECT
        city,
        obs_date,
        obs_date - (ROW_NUMBER() OVER (PARTITION BY city ORDER BY obs_date))::INTEGER
            AS streak_key
    FROM aqi_pipeline.mart_aqi_analytics
    WHERE is_cpcb_breach
)
SELECT
    city,
    MIN(obs_date)            AS streak_start,
    MAX(obs_date)            AS streak_end,
    COUNT(*)                 AS consecutive_breach_days
FROM breach_days
GROUP BY city, streak_key
ORDER BY consecutive_breach_days DESC, streak_start
LIMIT 15;


-- Q6 — Persistence: does a pollution shock stick, or wash out the next day?
--      Lag-1 autocorrelation separates cities that accumulate over days from
--      cities that are ventilated daily. mean_abs_dod_pct measures churn.
SELECT
    city,
    COUNT(*) FILTER (WHERE prev_day_pm2_5 IS NOT NULL)      AS paired_days,
    ROUND(CORR(avg_pm2_5, prev_day_pm2_5)::NUMERIC, 3)      AS lag1_autocorrelation,
    ROUND(AVG(ABS(dod_pm2_5_pct_change)), 1)                AS mean_abs_dod_pct,
    ROUND(STDDEV_SAMP(avg_pm2_5), 1)                        AS daily_pm2_5_stddev,
    ROUND(AVG(avg_pm2_5), 1)                                AS mean_pm2_5,
    ROUND(STDDEV_SAMP(avg_pm2_5) / NULLIF(AVG(avg_pm2_5), 0), 3)
                                                            AS coefficient_of_variation
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY city
ORDER BY lag1_autocorrelation DESC NULLS LAST;


-- Q7 — Weekly cycle: a weekday/weekend gap is a fingerprint of traffic and
--      industry rather than meteorology.
SELECT
    city,
    ROUND(AVG(avg_pm2_5) FILTER (WHERE NOT is_weekend), 1)  AS weekday_mean_pm2_5,
    ROUND(AVG(avg_pm2_5) FILTER (WHERE is_weekend), 1)      AS weekend_mean_pm2_5,
    ROUND(
        AVG(avg_pm2_5) FILTER (WHERE is_weekend)
      - AVG(avg_pm2_5) FILTER (WHERE NOT is_weekend), 1
    )                                                       AS weekend_minus_weekday,
    ROUND(AVG(avg_no2) FILTER (WHERE NOT is_weekend), 1)    AS weekday_mean_no2,
    ROUND(AVG(avg_no2) FILTER (WHERE is_weekend), 1)        AS weekend_mean_no2
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY city
ORDER BY weekend_minus_weekday;


-- Q8 — Pipeline health: coverage, completeness and freshness per city.
SELECT
    city,
    MIN(obs_date)                                            AS first_day,
    MAX(obs_date)                                            AS last_day,
    COUNT(*)                                                 AS days_loaded,
    (MAX(obs_date) - MIN(obs_date) + 1)                      AS days_in_span,
    (MAX(obs_date) - MIN(obs_date) + 1) - COUNT(*)           AS missing_days,
    ROUND(AVG(valid_hours_count), 1)                         AS avg_valid_hours,
    COUNT(*) FILTER (WHERE valid_hours_count < 24)           AS partial_days
FROM aqi_pipeline.mart_aqi_analytics
GROUP BY city
ORDER BY city;


-- Q9 — Last ten pipeline invocations (freshness / scheduling audit).
SELECT
    run_id,
    started_at,
    status,
    mode,
    past_days,
    cities_succeeded || '/' || cities_requested AS cities,
    rows_staged,
    clean_rows_total,
    duration_seconds,
    message
FROM aqi_pipeline.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
