-- UX Analytics - reproducible SQL script
-- Tested on DuckDB (works on SparkSQL/Trino too).
-- Run top to bottom, safe to re-run (DROP IF EXISTS everywhere).


-- 0. database setup
-- In DuckDB the database is the .duckdb file you open; this schema groups our
-- tables. On SparkSQL/Trino swap the two lines below for CREATE DATABASE / USE.
CREATE SCHEMA IF NOT EXISTS ux_analytics;
USE ux_analytics;


-- 1. raw tables (bronze)
-- Explicit schema so the column types are documented, then load the raw JSON.
-- read_json_auto is DuckDB; on Spark this would be: INSERT INTO ... SELECT * FROM json.`...`

DROP TABLE IF EXISTS raw_sessions;
CREATE TABLE raw_sessions (
    session_id   VARCHAR,
    user_id      VARCHAR,
    page         VARCHAR,
    device       VARCHAR,
    duration_sec BIGINT,
    clicked_cta  BOOLEAN,
    converted    BOOLEAN,
    ts           VARCHAR,      -- kept as string; parsed safely in Silver
    country      VARCHAR
);

INSERT INTO raw_sessions
SELECT session_id, user_id, page, device, duration_sec,
       clicked_cta, converted, ts, country
FROM read_json_auto('data/raw/sessions.json');

DROP TABLE IF EXISTS raw_pages;
CREATE TABLE raw_pages (
    page_id     INTEGER,
    page        VARCHAR,
    funnel_step INTEGER,
    step_name   VARCHAR
);

INSERT INTO raw_pages
SELECT page_id, page, funnel_step, step_name
FROM read_json_auto('data/raw/pages.json');


-- 2. clean tables (silver)
-- data quality rules:
--   DQ-1 drop null PKs
--   DQ-2 normalize device to lowercase
--   DQ-3 nullify unrealistic duration_sec
--   DQ-4 safe timestamp cast
--   DQ-5 fill missing country
--   DQ-6 bot/suspicious flag

DROP TABLE IF EXISTS clean_sessions;
CREATE TABLE clean_sessions AS
SELECT
    session_id,
    user_id,
    page,
    LOWER(TRIM(device))                                              AS device,
    CASE
        WHEN duration_sec < 0 OR duration_sec > 7200 THEN NULL
        ELSE duration_sec
    END                                                              AS duration_sec,
    clicked_cta,
    converted,
    TRY_CAST(ts AS TIMESTAMP)                                        AS event_ts,
    COALESCE(UPPER(country), 'UNKNOWN')                              AS country,
    (duration_sec < 5)                                               AS is_suspicious,
    CURRENT_TIMESTAMP                                                AS _loaded_at
FROM raw_sessions
WHERE session_id IS NOT NULL
  AND user_id    IS NOT NULL;

DROP TABLE IF EXISTS clean_pages;
CREATE TABLE clean_pages AS
SELECT
    page_id,
    page,
    funnel_step,
    TRIM(step_name) AS step_name
FROM raw_pages
WHERE page_id IS NOT NULL;


-- 3. gold tables

-- gold 1: funnel conversion metrics (JOIN + AGG)
DROP TABLE IF EXISTS gold_funnel_metrics;
CREATE TABLE gold_funnel_metrics AS
SELECT
    s.page,
    p.funnel_step,
    p.step_name,
    COUNT(s.session_id)                                             AS session_count,
    ROUND(AVG(s.duration_sec), 1)                                   AS avg_duration_sec,
    SUM(CAST(s.clicked_cta AS INTEGER))                             AS cta_clicks,
    SUM(CAST(s.converted   AS INTEGER))                             AS conversions,
    ROUND(
        SUM(CAST(s.converted AS INTEGER)) * 100.0
        / NULLIF(COUNT(s.session_id), 0), 1
    )                                                               AS conversion_rate_pct
FROM clean_sessions s
LEFT JOIN clean_pages p ON s.page = p.page
GROUP BY s.page, p.funnel_step, p.step_name
ORDER BY p.funnel_step;

-- gold 2: device breakdown (AGG)
DROP TABLE IF EXISTS gold_device_metrics;
CREATE TABLE gold_device_metrics AS
SELECT
    device,
    COUNT(session_id)                                               AS session_count,
    ROUND(AVG(duration_sec), 1)                                     AS avg_duration_sec,
    ROUND(
        SUM(CAST(clicked_cta AS INTEGER)) * 100.0
        / NULLIF(COUNT(session_id), 0), 1
    )                                                               AS cta_ctr_pct,
    ROUND(
        SUM(CAST(converted AS INTEGER)) * 100.0
        / NULLIF(COUNT(session_id), 0), 1
    )                                                               AS cvr_pct
FROM clean_sessions
GROUP BY device
ORDER BY session_count DESC;


-- 4. data quality checks (run these to see the % of bad rows)

SELECT 'null session_id rate'           AS dq_check,
       ROUND(100.0 * SUM(CASE WHEN session_id IS NULL THEN 1 ELSE 0 END)
             / COUNT(*), 2)             AS pct
FROM raw_sessions

UNION ALL

SELECT 'invalid duration_sec rate',
       ROUND(100.0 * SUM(CASE
           WHEN duration_sec < 0 OR duration_sec > 7200 THEN 1 ELSE 0 END)
           / COUNT(*), 2)
FROM raw_sessions

UNION ALL

SELECT 'unparseable timestamp rate',
       ROUND(100.0 * SUM(CASE
           WHEN TRY_CAST(ts AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END)
           / COUNT(*), 2)
FROM raw_sessions

UNION ALL

SELECT 'orphaned sessions (no page match)',
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clean_sessions), 2)
FROM clean_sessions s
LEFT JOIN clean_pages p ON s.page = p.page
WHERE p.page_id IS NULL;


-- 5. verification - row counts per table

SELECT 'raw_sessions'        AS table_name, COUNT(*) AS rows FROM raw_sessions
UNION ALL SELECT 'raw_pages',               COUNT(*) FROM raw_pages
UNION ALL SELECT 'clean_sessions',          COUNT(*) FROM clean_sessions
UNION ALL SELECT 'clean_pages',             COUNT(*) FROM clean_pages
UNION ALL SELECT 'gold_funnel_metrics',     COUNT(*) FROM gold_funnel_metrics
UNION ALL SELECT 'gold_device_metrics',     COUNT(*) FROM gold_device_metrics;
