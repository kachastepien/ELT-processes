# UX Analytics ETL Pipeline

**Stack:** PySpark · Apache Airflow · dbt (bonus)  
**Course:** Scalable Processing + Orchestration  
**Repo:** https://github.com/kachastepien/ELT-processes

---

## 1. Problem Statement

Product teams track thousands of sessions daily but lack a unified view
of user behaviour across the conversion funnel. Open questions:

- At which funnel step (Awareness → Interest → Consideration → Decision)
  do users drop off most?
- Which device type (mobile / desktop / tablet) converts best?
- How many sessions are bot traffic or measurement errors?

**Pipeline goal:** Build a reproducible, observable Bronze→Silver→Gold ETL
that runs daily, applies data quality rules to raw tracker events, and
delivers aggregated UX metrics to a gold layer ready for BI / dashboards.

---

## 2. Architecture

```
[Source: event tracker JSON]
         │
         ▼
┌─────────────────┐
│  BRONZE (raw)   │  Raw JSON, no transformations
│  sessions.json  │  session_id, user_id, page, device,
│  pages.json     │  duration_sec, clicked_cta, converted, ts, country
└────────┬────────┘
         │   6 DQ rules applied
         ▼
┌─────────────────┐
│  SILVER (clean) │  Validated, typed records
│  clean_sessions │  + is_suspicious flag
│  clean_pages    │  + event_ts parsed
└────────┬────────┘  + device normalized
         │
         ▼  (LEFT JOIN + GROUP BY)
┌──────────────────────────────────────┐
│  GOLD                                │
│  gold_funnel_metrics  (JOIN + AGG)   │  CVR per funnel step
│  gold_device_metrics  (AGG)          │  CTR / CVR per device
└──────────────────────────────────────┘
```

---

## 3. Table Schemas

### `raw_sessions` (Bronze)

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

### `clean_sessions` (Silver)

All columns above, plus:

| Column        | Type      | Description                               |
|---------------|-----------|-------------------------------------------|
| device        | VARCHAR   | Normalised to lowercase                   |
| duration_sec  | BIGINT    | NULL when < 0 or > 7200                   |
| event_ts      | TIMESTAMP | Safely parsed from `ts`                   |
| country       | VARCHAR   | NULL → 'UNKNOWN'                          |
| is_suspicious | BOOLEAN   | True when duration_sec < 5 s (bot signal) |
| _loaded_at    | TIMESTAMP | Audit: when the record entered Silver     |

### `raw_pages` / `clean_pages`

| Column     | Type    | Description                   |
|------------|---------|-------------------------------|
| page_id    | INTEGER | Primary key                   |
| page       | VARCHAR | URL path                      |
| funnel_step| INTEGER | Step number in funnel (1–4)   |
| step_name  | VARCHAR | Human-readable step label     |

### `gold_funnel_metrics` (Gold — JOIN + AGG)

| Column               | Type    | Description                          |
|----------------------|---------|--------------------------------------|
| page                 | VARCHAR | URL path                             |
| funnel_step          | INTEGER | Funnel step number                   |
| step_name            | VARCHAR | Step label (from JOIN with pages)    |
| session_count        | BIGINT  | Total sessions at this step          |
| avg_duration_sec     | DOUBLE  | Mean time spent on page              |
| cta_clicks           | BIGINT  | Total CTA button clicks              |
| conversions          | BIGINT  | Total conversions                    |
| conversion_rate_pct  | DOUBLE  | conversions / session_count × 100    |

---

## 4. Data Quality Risks

| ID   | Risk                          | Severity    | Mitigation                                      |
|------|-------------------------------|-------------|-------------------------------------------------|
| DQ-1 | Null primary keys             | Critical    | `dropna(subset=["session_id","user_id"])`       |
| DQ-2 | Inconsistent device casing    | Moderate    | `lower(trim(device))` in Silver                 |
| DQ-3 | Unrealistic duration_sec      | Critical    | Nullify when < 0 or > 7200 s                    |
| DQ-4 | Unparseable timestamps        | Moderate    | `try_to_timestamp()` → NULL on bad input        |
| DQ-5 | Orphaned sessions (no page FK)| Moderate    | LEFT JOIN + monitor unmatched rate              |
| DQ-6 | Re-run duplicates             | Operational | `write.mode("overwrite")` + `_loaded_at` audit  |

---

## 5. Tech Stack

| Component    | Technology     | Why                                          |
|--------------|----------------|----------------------------------------------|
| Processing   | PySpark 4.1    | Scalable, lazy evaluation, rich ETL API      |
| Orchestration| Airflow 3.2    | DAGs, retries, SLA, XCom, schedule           |
| Transforms   | dbt (bonus)    | SQL-first, tests, lineage, documentation     |
| Output format| Parquet/Snappy | Columnar, compressed, fast on aggregations   |
| Input format | JSON (newline) | Native format for event trackers             |

---

## 6. How to Run

```bash
# 1. Clone
git clone https://github.com/kachastepien/ELT-processes
cd ELT-processes

# 2. Install
pip install pyspark apache-airflow

# 3. Run PySpark pipeline
python3 spark/etl_pipeline.py

# 4. Run Airflow (optional)
export AIRFLOW_HOME=$(pwd)/airflow
airflow db migrate
airflow dags trigger ux_analytics_etl

# 5. Run SQL (DuckDB)
pip install duckdb
duckdb -c ".read sql/pipeline.sql"

# 6. dbt (bonus)
pip install dbt-duckdb
cd dbt && dbt run && dbt test
```

---

## 7. Repository Structure

```
ux-analytics-etl/
├── README.md
├── data/
│   └── raw/
│       ├── sessions.json          # Bronze: raw sessions
│       └── pages.json             # Bronze: page / funnel metadata
├── spark/
│   └── etl_pipeline.py           # PySpark Bronze→Silver→Gold job
├── airflow/
│   └── dags/
│       └── ux_analytics_dag.py   # Airflow DAG
├── sql/
│   └── pipeline.sql              # Reproducible SQL script
└── dbt/
    └── models/
        ├── schema.yml             # Sources, descriptions, tests
        ├── staging/
        │   ├── stg_sessions.sql   # Silver: sessions view
        │   └── stg_pages.sql      # Silver: pages view
        └── marts/
            ├── mart_funnel_metrics.sql   # Gold: funnel CVR
            └── mart_device_metrics.sql   # Gold: device CTR/CVR
```

---

*Course project: Scalable Processing (PySpark) + Orchestration (Apache Airflow)*
