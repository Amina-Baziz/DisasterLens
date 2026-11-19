"""
DisasterLens hourly pipeline (Airflow DAG).

Every hour this DAG:
  1. downloads the latest USGS earthquakes and the latest NASA EONET events
     and saves both answers, unchanged, in the data lake (MinIO)
  2. checks that both files really arrived
  3. copies the files of this run to a work folder, where Spark reads them

The tasks only pass small notes to each other (a file path and a record count),
never the data itself.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio.error import S3Error

# The project folder (it holds src/) sits next to dags/.
# Adding it to Python's path makes "from src import ..." work inside Airflow.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src import eonet, storage, usgs  # noqa: E402

# Where the files of one run are copied for Spark (inside the Airflow container).
WORKDIR_ROOT = os.getenv("PIPELINE_WORKDIR", "/tmp/disasterlens")

# How many days of EONET events the hourly run asks for.
EONET_DAYS = 1


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


# =====================================================================
# The DAG
# =====================================================================

default_args = {
    "owner": "disasterlens",
    "retries": 2,                          # try again twice if a task fails
    "retry_delay": timedelta(minutes=2),   # wait 2 minutes between tries
}

with DAG(
    dag_id="disasterlens_pipeline",
    description="Hourly: USGS and EONET to the data lake",
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

    # Both downloads run side by side, then the check, then the copy.
    [extract_usgs, extract_eonet_task] >> validate >> stage