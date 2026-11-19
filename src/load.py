# Loads staged data into the warehouse and runs data quality checks.
"""
Loads staged data into the warehouse, refreshes the marts, runs the data quality
rules and writes the run log.

Every function takes an open PostgreSQL connection. The functions that change data
do not commit, so the caller decides (the Airflow tasks commit once at the end,
the tests roll back).

Order inside the pipeline (see dags/pipeline.py):
  merge_staging_to_fact  ->  refresh_marts  ->  record_run  ->  run_dq_checks
"""

import os
from pathlib import Path

import psycopg2

QUERIES_FILE = Path(__file__).resolve().parent.parent / "sql" / "queries.sql"

# Columns of dw.fact_event that Spark delivers a new version of.
VERSIONED_COLUMNS = [
    "hazard_key", "date_key", "source_category", "title", "event_time_utc",
    "latitude", "longitude", "magnitude", "depth_km", "event_status",
    "has_valid_point", "source_updated_utc", "raw_object_path", "run_id",
]

# Staging rows that exist in the fact table are "newer" when their source update time
# is later (or when the old row has no update time). The same version again is not newer.
IS_NEWER = (
    "(dw.fact_event.source_updated_utc IS NULL "
    "OR EXCLUDED.source_updated_utc > dw.fact_event.source_updated_utc)"
)

# staging rows joined with the source and hazard dimensions (the keys the fact table needs)
STAGED_WITH_KEYS = """
    FROM staging.stg_event s
    JOIN dw.dim_source      src ON src.source_name = s.source
    JOIN dw.dim_hazard_type h   ON h.hazard_type   = s.hazard_type
"""


# =====================================================================
# Connection
# =====================================================================

def connect():
    """Open a PostgreSQL connection with the settings from .env (inside Docker: host 'postgres')."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "disasterlens"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


# =====================================================================
# Merge staging into the warehouse
# =====================================================================

def merge_staging_to_fact(conn):
    """
    Move the events Spark put in staging.stg_event into dw.fact_event.

    Rules:
      - A new event is inserted.
      - An event we already have is updated only when staging holds a newer version
        (later source_updated_utc). An older or identical version changes nothing.
      - first_seen_utc is set once and never overwritten.
      - last_seen_utc moves forward for every event staging contains.
      - Running the same merge twice gives the same result (nothing is duplicated).

    Does not commit. Returns {"rows_staged", "rows_inserted", "rows_updated", "rows_unchanged"}.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM staging.stg_event")
        rows_staged = cur.fetchone()[0]

        cur.execute(f"SELECT count(*) {STAGED_WITH_KEYS}")
        rows_matched = cur.fetchone()[0]
        if rows_matched != rows_staged:
            raise ValueError(
                f"{rows_staged - rows_matched} staged rows have an unknown source or hazard type"
            )

        cur.execute(
            f"""
            SELECT count(*) FILTER (WHERE f.event_key IS NULL),
                   count(*) FILTER (WHERE f.event_key IS NOT NULL
                                    AND (f.source_updated_utc IS NULL
                                         OR s.source_updated_utc > f.source_updated_utc))
            {STAGED_WITH_KEYS}
            LEFT JOIN dw.fact_event f ON f.event_key = s.event_key
            """
        )
        rows_inserted, rows_updated = cur.fetchone()

        updates = ",\n                ".join(
            f"{column} = CASE WHEN {IS_NEWER} THEN EXCLUDED.{column} "
            f"ELSE dw.fact_event.{column} END"
            for column in VERSIONED_COLUMNS
        )
        cur.execute(
            f"""
            INSERT INTO dw.fact_event
                (event_key, source_key, hazard_key, date_key, source_event_id, source_category,
                 title, event_time_utc, latitude, longitude, magnitude, depth_km, event_status,
                 has_valid_point, source_updated_utc, first_seen_utc, last_seen_utc,
                 raw_object_path, run_id)
            SELECT s.event_key, src.source_key, h.hazard_key,
                   to_char(s.event_time_utc, 'YYYYMMDD')::int,
                   s.source_event_id, s.source_category, s.title, s.event_time_utc,
                   s.latitude, s.longitude, s.magnitude, s.depth_km, s.event_status,
                   s.has_valid_point, s.source_updated_utc,
                   now() AT TIME ZONE 'utc', now() AT TIME ZONE 'utc',
                   s.raw_object_path, s.run_id
            {STAGED_WITH_KEYS}
            ON CONFLICT (event_key) DO UPDATE SET
                {updates},
                last_seen_utc = EXCLUDED.last_seen_utc
            """
        )
    return {
        "rows_staged": rows_staged,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_unchanged": rows_staged - rows_inserted - rows_updated,
    }


def refresh_marts(conn):
    """
    Refresh the daily summary so the dashboard sees the new events.
    CONCURRENTLY keeps the dashboard readable while it refreshes. It cannot run inside
    a transaction, so this needs a connection that has not started one.
    """
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_daily_hazard_summary")


# =====================================================================
# Run log
# =====================================================================

def record_run(conn, run_id, dag_id, status, started_at, finished_at,
               rows_fetched=None, rows_inserted=None, rows_updated=None,
               rows_rejected=None, error_message=None):
    """Write (or overwrite) the row of this run in ops.pipeline_runs. Does not commit."""
    duration = (finished_at - started_at).total_seconds() if started_at and finished_at else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.pipeline_runs
                (run_id, dag_id, status, started_at_utc, finished_at_utc, duration_seconds,
                 rows_fetched, rows_inserted, rows_updated, rows_rejected, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                dag_id = EXCLUDED.dag_id, status = EXCLUDED.status,
                started_at_utc = EXCLUDED.started_at_utc, finished_at_utc = EXCLUDED.finished_at_utc,
                duration_seconds = EXCLUDED.duration_seconds, rows_fetched = EXCLUDED.rows_fetched,
                rows_inserted = EXCLUDED.rows_inserted, rows_updated = EXCLUDED.rows_updated,
                rows_rejected = EXCLUDED.rows_rejected, error_message = EXCLUDED.error_message
            """,
            (run_id, dag_id, status, started_at, finished_at, duration,
             rows_fetched, rows_inserted, rows_updated, rows_rejected, error_message),
        )


# =====================================================================
# Data quality rules
# =====================================================================

def dq_sql():
    """The data quality statement: everything after the 'Part 2' heading in sql/queries.sql."""
    text = QUERIES_FILE.read_text()
    return text[text.index("SELECT", text.index("Part 2: data quality rules")):]


def run_dq_checks(conn, run_id):
    """
    Run every data quality rule and save one row per rule in ops.dq_results. Does not commit.

    Returns {"rules", "passed", "failed_errors", "failed_warnings"} where the two failed
    entries list the names of the rules that did not pass.
    """
    with conn.cursor() as cur:
        cur.execute(dq_sql())
        results = cur.fetchall()
        for rule_name, severity, _failed_count, passed, detail in results:
            cur.execute(
                """
                INSERT INTO ops.dq_results (run_id, rule_name, severity, passed, detail)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (run_id, rule_name, severity, passed, detail),
            )
    return {
        "rules": len(results),
        "passed": sum(1 for r in results if r[3]),
        "failed_errors": [r[0] for r in results if not r[3] and r[1] == "error"],
        "failed_warnings": [r[0] for r in results if not r[3] and r[1] == "warning"],
    }