"""
Airflow DAG: ux_analytics_etl
==============================
Orchestrates the full Bronze → Silver → Gold pipeline
for UX analytics data: user sessions, CTA clicks, conversion funnel.

Key Airflow concepts demonstrated:
  • DAG with daily schedule (03:00 UTC)
  • Task dependency tree
  • Retries with exponential backoff
  • BranchPythonOperator – conditional routing
  • XCom – passing DQ results between tasks
  • SLA alert when pipeline exceeds time limit
  • max_active_runs=1 – prevents concurrent overlapping runs
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_SCRIPT = os.path.join(BASE_DIR, "spark", "etl_pipeline.py")

DEFAULT_ARGS = {
    "owner":                     "data_team",
    "depends_on_past":           False,
    "email":                     ["data-alerts@company.com"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5 min → 10 min → 20 min
    "max_retry_delay":           timedelta(hours=1),
    "execution_timeout":         timedelta(hours=2),
    "sla":                       timedelta(hours=3),
}


def validate_source_files(**context):
    """
    Check that required raw data files exist before launching the Spark job.
    Pushes a JSON summary via XCom so downstream tasks can read the result.
    """
    raw_dir  = os.path.join(BASE_DIR, "data", "raw")
    required = ["sessions.json", "pages.json"]
    missing  = [f for f in required
                if not os.path.exists(os.path.join(raw_dir, f))]

    result = {
        "checked_at":  datetime.utcnow().isoformat(),
        "missing":     missing,
        "all_present": len(missing) == 0,
    }
    context["ti"].xcom_push(key="validation_result", value=json.dumps(result))
    logging.info("Validation: %s", result)

    if missing:
        raise FileNotFoundError(f"Missing source files: {missing}")


def branch_on_validation(**context):
    """BranchPythonOperator: route to ETL or alert task."""
    raw    = context["ti"].xcom_pull(task_ids="validate_sources",
                                     key="validation_result")
    result = json.loads(raw)
    return "run_bronze_silver" if result["all_present"] else "alert_missing_files"


def run_data_quality_gate(**context):
    """
    Post-Silver DQ checks. Reads the Parquet output from the Spark job
    and asserts quality thresholds:
      – null duration_sec rate < 30 %
      – suspicious session rate < 20 %
    Pushes results via XCom; raises ValueError if thresholds are exceeded.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.appName("DQ-Gate").master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    path = os.path.join(BASE_DIR, "data", "clean", "sessions")
    if not os.path.exists(path):
        logging.warning("Clean sessions not found – skipping DQ gate")
        spark.stop()
        return

    df    = spark.read.parquet(path)
    total = df.count()
    null_dur    = df.filter(F.col("duration_sec").isNull()).count()
    suspicious  = df.filter(F.col("is_suspicious") == True).count()

    summary = {
        "total_rows":         total,
        "null_duration_pct":  round(null_dur   / total * 100, 2) if total else 0,
        "suspicious_pct":     round(suspicious / total * 100, 2) if total else 0,
        "dq_passed": (null_dur / total < 0.30 and suspicious / total < 0.20)
                     if total else False,
    }
    context["ti"].xcom_push(key="dq_summary", value=json.dumps(summary))
    logging.info("DQ summary: %s", summary)

    if not summary["dq_passed"]:
        raise ValueError(f"DQ gate failed: {summary}")
    spark.stop()


def notify_success(**context):
    logging.info("Pipeline completed for logical_date=%s", context["logical_date"])


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ux_analytics_etl",
    description="Bronze → Silver → Gold ETL for UX conversion funnel analytics",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule="0 3 * * *",   # daily at 03:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["etl", "pyspark", "ux", "analytics"],
    doc_md="""
    ## UX Analytics ETL

    **Analytical goal:** Identify drop-off points in the product conversion funnel
    and compare device performance (mobile vs desktop vs tablet).

    | Layer  | Storage           | Description                  |
    |--------|-------------------|------------------------------|
    | Bronze | data/raw/*.json   | Raw files, no changes        |
    | Silver | data/clean/       | Validated, typed, clean      |
    | Gold   | data/gold/        | Aggregated UX metrics        |

    **Schedule:** daily 03:00 UTC · retries 3× with exponential backoff (5→10→20 min)
    """,
) as dag:

    validate_sources = PythonOperator(
        task_id="validate_sources",
        python_callable=validate_source_files,
    )

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=branch_on_validation,
    )

    alert_missing_files = BashOperator(
        task_id="alert_missing_files",
        bash_command='echo "ALERT: source files missing – pipeline aborted"',
    )

    run_bronze_silver = BashOperator(
        task_id="run_bronze_silver",
        bash_command=f"python3 {SPARK_SCRIPT}",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    dq_gate = PythonOperator(
        task_id="dq_gate",
        python_callable=run_data_quality_gate,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command='echo "Gold layer built within main Spark job – gate passed"',
        trigger_rule="all_success",
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    #
    #  validate_sources
    #        │
    #      branch
    #    ┌──┴──────────────┐
    #  alert          run_bronze_silver
    #    │                 │
    #   end             dq_gate
    #                     │
    #                  build_gold
    #                     │
    #                   notify
    #                     │
    #                    end
    #
    validate_sources >> branch
    branch           >> [run_bronze_silver, alert_missing_files]
    alert_missing_files >> end
    run_bronze_silver >> dq_gate >> build_gold >> notify >> end
