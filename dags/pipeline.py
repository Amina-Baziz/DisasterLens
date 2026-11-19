"""
DisasterLens hourly pipeline (Airflow DAG).

Every hour this DAG:
  1. downloads the latest USGS earthquakes and the latest NASA EONET events
     and saves both answers, unchanged, in the data lake (MinIO)
  2. checks that both files really arrived
  3. copies the files of this run to a work folder, where Spark reads them
  4. runs the Spark job (src/transform.py), which cleans both sources into staging
  5. merges staging into the warehouse and refreshes the daily summary
  6. writes the run log, then runs the data quality rules

The tasks only pass small notes to each other (a file path, a record count),
never the data itself.
"""

import json
import os
import subprocess
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from minio.error import S3Error

# The project folder (it holds src/) sits next to dags/.
# Adding it to Python's path makes "from src import ..." work inside Airflow.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src import eonet, load, storage, usgs  # noqa: E402

# Where the files of one run are copied for Spark (inside the Airflow container).
WORKDIR_ROOT = os.getenv("PIPELINE_WORKDIR", "/tmp/disasterlens")

# How many days of EONET events the hourly run asks for.
EONET_DAYS = 1

# The PostgreSQL driver Spark needs to write to the database (installed in the Airflow image).
JDBC_JAR = os.getenv("POSTGRES_JDBC_JAR", "/opt/postgresql.jar")

DAG_ID = "disasterlens_pipeline"

# Tasks whose result decides if the run counts as a success (the log and the quality check are not).
PIPELINE_TASKS = [
    "extract_usgs_to_lake", "extract_eonet_to_lake", "validate_raw", "stage_raw_to_workdir",
    "spark_transform", "merge_to_warehouse", "refresh_marts",
]


# =====================================================================
# Helpers
# =====================================================================

def make_sure_bucket_exists():
    """Create the data lake bucket on a fresh machine. Two tasks may try at once, which is fine."""
    try:
        storage.ensure_bucket()
    except S3Error as error:
        if error.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def save_to_lake(source, run_id, payload, status_code, url, record_count):
    """Save one API answer in the lake and return a small note about it."""
    path = storage.put_raw(
        source, run_id, payload,
        meta={
            "endpoint": url,
            "http_status": status_code,
            "fetched_at_utc": usgs.utc_now_iso(),
            "record_count": record_count,
        },
    )
    return {"source": source, "path": path, "record_count": record_count}


# =====================================================================
# Task functions
# =====================================================================

def extract_usgs_to_lake(run_id):
    """Download the earthquakes of the last 24 hours and save them in the lake."""
    make_sure_bucket_exists()
    payload, status_code, url = usgs.fetch_feed()
    return save_to_lake("usgs", run_id, payload, status_code, url, len(payload["features"]))


def extract_eonet_to_lake(run_id):
    """Download the latest EONET events and save them in the lake."""
    make_sure_bucket_exists()
    payload, status_code, url = eonet.fetch_events(days=EONET_DAYS)
    return save_to_lake("eonet", run_id, payload, status_code, url, len(payload["events"]))


def validate_raw(run_id, ti):
    """Check that both sources saved their files for this run. Fails the task if one is missing."""
    notes = ti.xcom_pull(task_ids=["extract_usgs_to_lake", "extract_eonet_to_lake"])
    files_in_lake = storage.list_run(run_id)

    problems = []
    for note in notes:
        if not note:
            problems.append("a download task returned no note")
            continue
        response_path = note["path"]
        meta_path = response_path.replace("response.json", "_meta.json")
        for expected in (response_path, meta_path):
            if expected not in files_in_lake:
                problems.append(f"{note['source']}: {expected} is not in the lake")
    if problems:
        raise ValueError("Raw data check failed: " + "; ".join(problems))

    return {
        "files": len(files_in_lake),
        "usgs_records": notes[0]["record_count"],
        "eonet_records": notes[1]["record_count"],
    }


def stage_raw_to_workdir(run_id):
    """Copy the files of this run from the lake to a local folder, keeping the folder layout."""
    folder = os.path.join(WORKDIR_ROOT, storage.safe_run_id(run_id))
    paths = storage.download_run(run_id, folder)
    return {"workdir": folder, "files": len(paths)}


def parse_spark_summary(output):
    """Find the 'SPARK_SUMMARY {...}' line the Spark job prints at the end and return it as a dict."""
    for line in output.splitlines():
        if line.startswith("SPARK_SUMMARY "):
            return json.loads(line[len("SPARK_SUMMARY "):])
    raise RuntimeError("The Spark job finished but printed no SPARK_SUMMARY line")


def run_outcome(task_states):
    """
    Decide how the run is logged, from the final state of each pipeline task.
    task_states is {task_id: state}. Returns (status, error_message).
    """
    broken = {task: state for task, state in task_states.items() if state in ("failed", "upstream_failed")}
    if not broken:
        return "success", None
    first = next(task for task in PIPELINE_TASKS if task in broken)
    return "failed", f"{first} {broken[first]}"


def to_naive_utc(moment):
    """Airflow times carry a timezone. The warehouse stores plain UTC times."""
    return moment.astimezone(timezone.utc).replace(tzinfo=None) if moment else None


def transform_with_spark(run_id, ti):
    """Run the Spark job on this run's files. It writes the clean events into staging.stg_event."""
    stage = ti.xcom_pull(task_ids="stage_raw_to_workdir")
    env = {
        **os.environ,
        "PYTHONPATH": PROJECT_ROOT,
        "PYSPARK_SUBMIT_ARGS": f"--jars {JDBC_JAR} --driver-class-path {JDBC_JAR} pyspark-shell",
    }
    result = subprocess.run(
        [sys.executable, "-m", "src.transform",
         "--input-dir", stage["workdir"],
         "--run-id", storage.safe_run_id(run_id),
         "--output", "jdbc"],
        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("Spark job failed:\n" + result.stderr[-3000:])
    stats = parse_spark_summary(result.stdout)
    print("Spark summary:", json.dumps(stats))
    return stats


def merge_to_warehouse():
    """Move the staged events into the warehouse in one transaction (all or nothing)."""
    with closing(load.connect()) as conn:
        with conn:  # commits at the end, rolls back if anything fails
            counts = load.merge_staging_to_fact(conn)
    print("Merge counts:", json.dumps(counts))
    return counts


def refresh_marts():
    """Refresh the daily summary the dashboard reads."""
    with closing(load.connect()) as conn:
        load.refresh_marts(conn)


def record_run(run_id, dag_run, ti):
    """Write this run into ops.pipeline_runs. Runs even when an earlier task failed."""
    states = {t.task_id: t.state for t in dag_run.get_task_instances() if t.task_id in PIPELINE_TASKS}
    status, error_message = run_outcome(states)

    checked = ti.xcom_pull(task_ids="validate_raw") or {}
    spark = ti.xcom_pull(task_ids="spark_transform") or {}
    merged = ti.xcom_pull(task_ids="merge_to_warehouse") or {}
    fetched = (checked.get("usgs_records") or 0) + (checked.get("eonet_records") or 0) if checked else None

    now = datetime.now(timezone.utc)
    with closing(load.connect()) as conn:
        with conn:
            load.record_run(
                conn, run_id, DAG_ID, status,
                to_naive_utc(dag_run.start_date or now), to_naive_utc(now),
                rows_fetched=fetched,
                rows_inserted=merged.get("rows_inserted"),
                rows_updated=merged.get("rows_updated"),
                rows_rejected=spark.get("rows_rejected"),
                error_message=error_message,
            )
    print(f"Run logged as {status}")


def run_dq_checks(run_id):
    """Run the data quality rules and save the results. Fails the task if a rule of type error fails."""
    with closing(load.connect()) as conn:
        with conn:
            summary = load.run_dq_checks(conn, run_id)
    print("Data quality:", json.dumps(summary))
    if summary["failed_errors"]:
        raise AirflowFailException("Data quality errors: " + ", ".join(summary["failed_errors"]))
    return summary



# =====================================================================
# The DAG
# =====================================================================

default_args = {
    "owner": "disasterlens",
    "retries": 2,                          # try again twice if a task fails
    "retry_delay": timedelta(minutes=2),   # wait 2 minutes between tries
}

with DAG(
    dag_id=DAG_ID,
    description="Hourly: USGS and EONET to the data lake and the warehouse",
    start_date=datetime(2026, 11, 1),
    schedule="@hourly",
    catchup=False,          # do not run for the hours that were missed
    max_active_runs=1,      # never two runs at the same time
    default_args=default_args,
    tags=["disasterlens"],
) as dag:

    extract_usgs = PythonOperator(
        task_id="extract_usgs_to_lake", python_callable=extract_usgs_to_lake,
    )
    extract_eonet_task = PythonOperator(
        task_id="extract_eonet_to_lake", python_callable=extract_eonet_to_lake,
    )
    validate = PythonOperator(
        task_id="validate_raw", python_callable=validate_raw,
    )
    stage = PythonOperator(
        task_id="stage_raw_to_workdir", python_callable=stage_raw_to_workdir,
    )

    spark = PythonOperator(
        task_id="spark_transform", python_callable=transform_with_spark,
        execution_timeout=timedelta(minutes=15),
    )
    merge = PythonOperator(task_id="merge_to_warehouse", python_callable=merge_to_warehouse)
    refresh = PythonOperator(task_id="refresh_marts", python_callable=refresh_marts)
    log_run = PythonOperator(
        task_id="record_run", python_callable=record_run,
        trigger_rule=TriggerRule.ALL_DONE,   # runs even when an earlier task failed
    )
    quality = PythonOperator(task_id="run_dq_checks", python_callable=run_dq_checks)

    # Both downloads run side by side, then the check, the copy, Spark, the merge and the refresh.
    # The run is logged after that (always), and the quality rules run once it is logged.
    [extract_usgs, extract_eonet_task] >> validate >> stage >> spark >> merge >> refresh >> log_run
    [refresh, log_run] >> quality