# UX Analytics ELT

Reproducible SQL ELT for product conversion-funnel analytics, built on a
Bronze -> Silver -> Gold (medallion) model. Runs on DuckDB.

## 1. Problem Statement

Product teams track thousands of sessions a day but have no single view of how
users move through the conversion funnel. Questions we want to answer:

- At which funnel step (Awareness -> Interest -> Consideration -> Decision) do
  users drop off the most?
- Which device type (mobile / desktop / tablet) converts best?
- How many sessions are bot traffic or measurement errors?

Goal: take the raw tracker events, clean them with explicit data-quality rules,
and produce a small set of aggregated UX metrics ready for a dashboard.

## 2. Architecture

```
event tracker JSON  (sessions.json, pages.json)
        |
        v
BRONZE  raw_sessions / raw_pages      raw JSON loaded as-is
        |
        |  data quality rules
        v
SILVER  clean_sessions / clean_pages  validated, typed, normalised
        |
        |  LEFT JOIN + GROUP BY
        v
GOLD    gold_funnel_metrics  (JOIN + AGG)   conversion rate per funnel step
        gold_device_metrics  (AGG)          CTR / CVR per device
```

Sessions are linked to the funnel via `sessions.page = pages.page`.

## 3. Table Schemas

`raw_sessions` (Bronze)

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| session_id   | VARCHAR | Session primary key                  |
| user_id      | VARCHAR | User identifier                      |
| page         | VARCHAR | URL path visited                     |
| device       | VARCHAR | Device type (raw, not normalised)    |
| duration_sec | BIGINT  | Session duration in seconds          |
| clicked_cta  | BOOLEAN | Whether the CTA button was clicked   |
| converted    | BOOLEAN | Whether the session ended in a sale  |
| ts           | VARCHAR | Timestamp as raw string              |
| country      | VARCHAR | Country code (may be null)           |

`clean_sessions` (Silver) adds: device lowercased, duration_sec nullified when
out of range, `event_ts` parsed from `ts`, country defaulted to 'UNKNOWN',
`is_suspicious` flag (duration < 5s), and a `_loaded_at` audit timestamp.

`raw_pages` / `clean_pages`

| Column      | Type    | Description                  |
|-------------|---------|------------------------------|
| page_id     | INTEGER | Primary key                  |
| page        | VARCHAR | URL path                     |
| funnel_step | INTEGER | Step number in funnel (1-4)  |
| step_name   | VARCHAR | Human-readable step label    |

`gold_funnel_metrics` is the JOIN + aggregation deliverable: session_count,
avg_duration_sec, cta_clicks, conversions and conversion_rate_pct per funnel step.

## 4. Data Quality Risks

| ID   | Risk                           | Where it bites      | Mitigation                                  |
|------|--------------------------------|---------------------|---------------------------------------------|
| DQ-1 | Null primary keys              | Ingestion           | Drop rows with null session_id / user_id    |
| DQ-2 | Inconsistent device casing     | Transformation      | `lower(trim(device))` in Silver             |
| DQ-3 | Unrealistic duration_sec       | Ingestion / sensors | Nullify when < 0 or > 7200 s                |
| DQ-4 | Unparseable timestamps         | Transformation      | `try_cast` -> NULL instead of failing       |
| DQ-5 | Orphaned sessions (no page FK) | Sharing / joins     | LEFT JOIN + monitor unmatched rate          |
| DQ-6 | Re-run duplicates              | Scale / operational | Idempotent rebuild + `_loaded_at` audit     |

Section 4 of `sql/pipeline.sql` runs these as live checks (% of bad rows).

## 5. How to Run

```bash
git clone https://github.com/kachastepien/ELT-processes
cd ELT-processes

pip install duckdb
duckdb ux_analytics.duckdb -c ".read sql/pipeline.sql"
```

The script is idempotent - every section starts with `DROP ... IF EXISTS`, so it
can be re-run any number of times and produces the same result.

## 6. Repository Structure

```
ELT-processes/
|-- README.md
|-- data/
|   `-- raw/
|       |-- sessions.json     raw session events (Bronze)
|       `-- pages.json        page / funnel metadata (Bronze)
`-- sql/
    `-- pipeline.sql          full ELT: database, raw, clean, gold, DQ checks
```
