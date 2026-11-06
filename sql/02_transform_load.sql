-- =============================================================================
-- sql/02_transform_load.sql
-- Promote raw hourly staging rows into the daily clean layer, then prune staging.
--
-- Idempotent by construction: re-running over the same staging window produces
-- the same clean rows (UPSERT on the natural key city + obs_date).
--
-- Two knobs are read from session settings so this file stays pure SQL and can
-- be run standalone in psql. src/etl.py sets them with SET LOCAL; the COALESCE
-- defaults apply when it is run by hand.
--
--   aqi.min_hours_per_day        default 6   -- completeness gate
--   aqi.staging_retention_days   default 14  -- raw replay window
--
-- Run standalone with:  psql "$DATABASE_URL" -f sql/02_transform_load.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- STAGING -> CLEAN
-- -----------------------------------------------------------------------------
INSERT INTO aqi_pipeline.clean_daily_aqi AS tgt (
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
    updated_at
)
WITH deduplicated_hourly AS (
    -- The same hour arrives on several runs (overlapping extraction windows) and
    -- the API revises recent values. Keep the most recently ingested version.
    SELECT DISTINCT ON (city, observation_time)
        city,
        observation_time,
        pm2_5,
        pm10,
        no2,
        so2,
        co,
        us_aqi
    FROM aqi_pipeline.stg_air_quality
    WHERE pm2_5 IS NOT NULL
    ORDER BY city, observation_time, ingested_at DESC, ingestion_id DESC
)
SELECT
    city,
    observation_time::DATE                              AS obs_date,
    ROUND(AVG(pm2_5), 2)                                AS avg_pm2_5,
    ROUND(MAX(pm2_5), 2)                                AS max_pm2_5,
    ROUND(MIN(pm2_5), 2)                                AS min_pm2_5,
    -- NULLs are skipped by AVG rather than coalesced to 0 first, so a missing
    -- co-pollutant shortens the sample instead of dragging the mean toward zero.
    COALESCE(ROUND(AVG(pm10), 2), 0)                    AS avg_pm10,
    COALESCE(ROUND(AVG(no2), 2), 0)                     AS avg_no2,
    COALESCE(ROUND(AVG(so2), 2), 0)                     AS avg_so2,
    COALESCE(ROUND(AVG(co), 2), 0)                      AS avg_co,
    COALESCE(ROUND(AVG(us_aqi))::INTEGER, 0)            AS avg_us_aqi,
    COUNT(*)::INTEGER                                   AS valid_hours_count,
    CURRENT_TIMESTAMP
FROM deduplicated_hourly
GROUP BY city, observation_time::DATE
HAVING COUNT(*) >= COALESCE(
    NULLIF(current_setting('aqi.min_hours_per_day', true), '')::INTEGER, 6
)
ON CONFLICT (city, obs_date) DO UPDATE SET
    avg_pm2_5         = EXCLUDED.avg_pm2_5,
    max_pm2_5         = EXCLUDED.max_pm2_5,
    min_pm2_5         = EXCLUDED.min_pm2_5,
    avg_pm10          = EXCLUDED.avg_pm10,
    avg_no2           = EXCLUDED.avg_no2,
    avg_so2           = EXCLUDED.avg_so2,
    avg_co            = EXCLUDED.avg_co,
    avg_us_aqi        = EXCLUDED.avg_us_aqi,
    valid_hours_count = EXCLUDED.valid_hours_count,
    updated_at        = CURRENT_TIMESTAMP
-- Guard against partial-day regression. Once staging has been pruned, a later
-- run can see only a slice of an old day; without this predicate that slice
-- would overwrite a complete 24-hour average with a worse one.
WHERE EXCLUDED.valid_hours_count >= tgt.valid_hours_count;


-- -----------------------------------------------------------------------------
-- STAGING RETENTION
-- Raw hourly rows are kept only long enough to replay a recent load. Pruning is
-- by ingestion time, not observation time, so a 90-day backfill stays replayable
-- for the full retention window.
-- -----------------------------------------------------------------------------
DELETE FROM aqi_pipeline.stg_air_quality
WHERE ingested_at < CURRENT_TIMESTAMP - (
    COALESCE(
        NULLIF(current_setting('aqi.staging_retention_days', true), '')::INTEGER, 14
    ) || ' days'
)::INTERVAL;
