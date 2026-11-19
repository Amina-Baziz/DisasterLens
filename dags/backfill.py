"""
DisasterLens backfill (Airflow DAG, started by hand).

Loads history so the dashboard has real trends:
  - USGS: the last N months, one request per month (a single request is limited
    to 20,000 earthquakes). Only earthquakes of magnitude 2.5 or more are asked for,
    because all sizes together would be more than the limit allows.
  - EONET: the last N days in one request.

After the downloads it runs the same steps as the hourly DAG (dags/pipeline.py):
check, copy to the work folder, Spark, merge, refresh, run log, quality rules.
Settings can be changed when triggering: months_back (default 12), eonet_days (default 365).
"""

import os
import sys
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models.dag import DagContext
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# This file reuses tasks from pipeline.py, which sits in the same folder.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importing pipeline also puts the project folder on Python's path, so "from src import" works below.
from pipeline import (  # noqa: E402
    WORKDIR_ROOT, default_args, make_sure_bucket_exists, merge_to_warehouse, record_run,
    refresh_marts, run_dq_checks, transform_with_spark,
)
from src import eonet, storage, usgs  # noqa: E402

# Importing pipeline created its DAG too. Airflow would think this file defines that DAG as well
# and complain about a duplicate, so forget it here. This file only defines the backfill DAG.
DagContext.autoregistered_dags.clear()


# =====================================================================
# Helpers
# =====================================================================

def month_ranges(months_back, today):
    """
    The date ranges to ask USGS for, oldest first, as ('YYYY-MM-DD', 'YYYY-MM-DD') pairs.
    One range per calendar month, ending with the current (unfinished) month, which stops tomorrow.
    """
    ranges = []
    for steps_back in range(months_back - 1, -1, -1):
        year, month = today.year, today.month - steps_back
        while month <= 0:
            month += 12
            year -= 1
        start = date(year, month, 1)
        end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        end = min(end, today + timedelta(days=1))
        ranges.append((start.isoformat(), end.isoformat()))
    return ranges


def list_backfill_files(run_id, client=None):
    """Every file this backfill run saved in the lake (its monthly runs all start with the same run id)."""
    client = client or storage.get_client()
    marker = f"/run_id={storage.safe_run_id(run_id)}__"
    objects = client.list_objects(storage.bucket_name(), prefix=f"{storage.RAW_PREFIX}/", recursive=True)
    return sorted(obj.object_name for obj in objects if marker in obj.object_name)


def save_part(source, part_run_id, payload, status_code, url, record_count):
    """Save one downloaded piece in the lake."""
    return storage.put_raw(
        source, part_run_id, payload,
        meta={
            "endpoint": url,
            "http_status": status_code,
            "fetched_at_utc": usgs.utc_now_iso(),
            "record_count": record_count,
        },
    )


# =====================================================================
# Task functions (the other tasks are reused from pipeline.py)
# =====================================================================

def extract_usgs_to_lake(run_id, params):
    """Download the earthquakes month by month. Each month is saved as its own run in the lake."""
    make_sure_bucket_exists()
    session = usgs.make_session()
    ranges = month_ranges(int(params["months_back"]), datetime.utcnow().date())
    total = 0
    for start, end in ranges:
        payload, status_code, url = usgs.fetch_fdsn(start, end, session=session)
        count = len(payload["features"])
        save_part("usgs", f"{run_id}__usgs_{start[:7]}", payload, status_code, url, count)
        print(f"USGS {start} to {end}: {count} earthquakes")
        total += count
    return {"source": "usgs", "files": len(ranges), "record_count": total}


def extract_eonet_to_lake(run_id, params):
    """Download the last N days of EONET events in one request."""
    make_sure_bucket_exists()
    payload, status_code, url = eonet.fetch_events(days=int(params["eonet_days"]))
    count = len(payload["events"])
    save_part("eonet", f"{run_id}__eonet", payload, status_code, url, count)
    return {"source": "eonet", "files": 1, "record_count": count}


def validate_raw(run_id, ti):
    """Check that every monthly USGS file and the EONET file are in the lake."""
    usgs_note, eonet_note = ti.xcom_pull(task_ids=["extract_usgs_to_lake", "extract_eonet_to_lake"])
    files = list_backfill_files(run_id)
    found = {
        "usgs": sum(1 for f in files if "/source=usgs/" in f and f.endswith("/response.json")),
        "eonet": sum(1 for f in files if "/source=eonet/" in f and f.endswith("/response.json")),
    }
    expected = {"usgs": usgs_note["files"], "eonet": eonet_note["files"]}

    problems = [f"{source}: expected {expected[source]} files, found {found[source]}"
                for source in expected if found[source] != expected[source]]
    if problems:
        raise ValueError("Raw data check failed: " + "; ".join(problems))

    return {
        "files": len(files),
        "usgs_records": usgs_note["record_count"],
        "eonet_records": eonet_note["record_count"],
    }


def stage_raw_to_workdir(run_id):
    """Copy every file of this backfill from the lake to one local folder, keeping the folder layout."""
    client = storage.get_client()
    folder = os.path.join(WORKDIR_ROOT, storage.safe_run_id(run_id))
    keys = list_backfill_files(run_id, client=client)
    for key in keys:
        local_path = os.path.join(folder, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        client.fget_object(storage.bucket_name(), key, local_path)
    return {"workdir": folder, "files": len(keys)}


# =====================================================================
# The DAG
# =====================================================================

with DAG(
    dag_id="disasterlens_backfill",
    description="Manual: 12 months of USGS and a year of EONET history",
    start_date=datetime(2026, 11, 1),
    schedule=None,          # never runs by itself, only when triggered
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "months_back": Param(12, type="integer", minimum=1, maximum=24, description="Months of USGS history"),
        "eonet_days": Param(365, type="integer", minimum=1, maximum=730, description="Days of EONET history"),
    },
    tags=["disasterlens"],
) as dag:

    extract_usgs = PythonOperator(
        task_id="extract_usgs_to_lake", python_callable=extract_usgs_to_lake,
        execution_timeout=timedelta(minutes=30),
    )
    extract_eonet_task = PythonOperator(
        task_id="extract_eonet_to_lake", python_callable=extract_eonet_to_lake,
        execution_timeout=timedelta(minutes=15),
    )
    validate = PythonOperator(task_id="validate_raw", python_callable=validate_raw)
    stage = PythonOperator(task_id="stage_raw_to_workdir", python_callable=stage_raw_to_workdir)
    spark = PythonOperator(
        task_id="spark_transform", python_callable=transform_with_spark,
        execution_timeout=timedelta(minutes=30),
    )
    merge = PythonOperator(task_id="merge_to_warehouse", python_callable=merge_to_warehouse)
    refresh = PythonOperator(task_id="refresh_marts", python_callable=refresh_marts)
    log_run = PythonOperator(
        task_id="record_run", python_callable=record_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    quality = PythonOperator(task_id="run_dq_checks", python_callable=run_dq_checks)

    [extract_usgs, extract_eonet_task] >> validate >> stage >> spark >> merge >> refresh >> log_run
    [refresh, log_run] >> quality