"""
MinIO data lake helpers.

This file saves raw API answers into the data lake (MinIO) and reads them back.

Layout of every saved run (agreed in the data contract, handbook C2):
    disasterlens-lake/
      raw/source=<usgs|eonet>/ingest_date=YYYY-MM-DD/run_id=<safe_run_id>/response.json
      raw/source=<usgs|eonet>/ingest_date=YYYY-MM-DD/run_id=<safe_run_id>/_meta.json

Rules:
  - response.json is the API answer exactly as received (never cleaned or changed).
  - _meta.json describes the download (source, URL, HTTP status, time, record count...).
  - The same run always writes to the same path, so an Airflow retry overwrites
    instead of creating duplicates. Different runs never overwrite each other.
"""

import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio

RAW_PREFIX = "raw"
VALID_SOURCES = {"usgs", "eonet"}


# =====================================================================
# Connection
# =====================================================================

def get_client():
    """
    Connect to MinIO using the settings from .env.
    Inside Docker the address is 'minio:9000' (container name + port).
    secure=False because MinIO runs locally without HTTPS.
    """
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def bucket_name():
    """Name of the data lake bucket, from .env (default: disasterlens-lake)."""
    return os.getenv("MINIO_BUCKET", "disasterlens-lake")


def ensure_bucket(client=None):
    """Create the bucket if it does not exist yet (useful on a fresh machine)."""
    client = client or get_client()
    if not client.bucket_exists(bucket_name()):
        client.make_bucket(bucket_name())


# =====================================================================
# Paths
# =====================================================================

def safe_run_id(run_id):
    """
    Make an Airflow run id safe to use in a file path.
    Every character that is not a letter, number, '_' or '-' becomes '_'.
    Example: 'scheduled__2026-11-14T10:00:00+00:00'
          -> 'scheduled__2026-11-14T10_00_00_00_00'
    """
    return re.sub(r"[^A-Za-z0-9_-]", "_", run_id)


def raw_folder(source, run_id, ingest_date):
    """Build the folder path of one run, following the agreed layout."""
    return (
        f"{RAW_PREFIX}/source={source}/ingest_date={ingest_date}"
        f"/run_id={safe_run_id(run_id)}"
    )


def today_utc():
    """Today's date in UTC as 'YYYY-MM-DD'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# =====================================================================
# Write and read
# =====================================================================

def _put_json(client, key, data):
    """Upload one Python object as a JSON file. Returns the file size in bytes."""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name(), key, io.BytesIO(body),
        length=len(body), content_type="application/json",
    )
    return len(body)


def put_raw(source, run_id, payload, meta, ingest_date=None, client=None):
    """
    Save one API answer and its description into the data lake.

    source      -> 'usgs' or 'eonet'
    run_id      -> the Airflow run id (or a name like 'backfill_usgs_2026-03')
    payload     -> the API answer (saved unchanged as response.json)
    meta        -> extra information: endpoint, http_status, fetched_at_utc, record_count
    ingest_date -> 'YYYY-MM-DD'; default is today (UTC)

    Returns the path of response.json inside the bucket.
    """
    if source not in VALID_SOURCES:
        raise ValueError(f"Unknown source '{source}', expected one of {VALID_SOURCES}")

    client = client or get_client()
    ingest_date = ingest_date or today_utc()
    folder = raw_folder(source, run_id, ingest_date)

    # 1. The raw answer, unchanged
    size = _put_json(client, f"{folder}/response.json", payload)

    # 2. The description of this download
    full_meta = {
        "source": source,
        "run_id": run_id,
        "safe_run_id": safe_run_id(run_id),
        "ingest_date": ingest_date,
        "size_bytes": size,
        **meta,
    }
    _put_json(client, f"{folder}/_meta.json", full_meta)

    return f"{folder}/response.json"


def list_run(run_id, client=None):
    """List every file saved by one run (both sources). Returns a sorted list of paths."""
    client = client or get_client()
    marker = f"/run_id={safe_run_id(run_id)}/"
    objects = client.list_objects(bucket_name(), prefix=f"{RAW_PREFIX}/", recursive=True)
    return sorted(obj.object_name for obj in objects if marker in obj.object_name)


def download_run(run_id, folder, client=None):
    """
    Download every file of one run into a local folder, keeping the same folder layout.
    Spark later reads the files from this local folder.
    Returns the list of local file paths.
    """
    client = client or get_client()
    local_paths = []
    for key in list_run(run_id, client=client):
        local_path = Path(folder) / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object(bucket_name(), key, str(local_path))
        local_paths.append(str(local_path))
    return local_paths