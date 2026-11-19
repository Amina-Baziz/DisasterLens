"""
Tests for the warehouse load (src/load.py).

Each test plants rows in staging inside a database transaction, runs the load and
checks the result. Everything is rolled back at the end, so nothing is saved.

They need the PostgreSQL container, so they run inside the Airflow container.
On a PC without psycopg2 or without a database they are skipped.
"""
import os
from datetime import datetime, timedelta

import pytest

psycopg2 = pytest.importorskip("psycopg2")

from src import load  # noqa: E402

NOW = datetime(2026, 11, 20, 12, 0, 0)


@pytest.fixture
def conn():
    """A database connection whose changes are always rolled back. Staging starts empty."""
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "disasterlens"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
            connect_timeout=5,
        )
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL is not reachable")
    with connection.cursor() as cur:
        cur.execute("DELETE FROM staging.stg_event")
    yield connection
    connection.rollback()
    connection.close()


def stage(conn, key, title="old title", updated=NOW, hazard="earthquake", source="usgs"):
    """Put one event in staging.stg_event."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staging.stg_event
                (event_key, source, source_event_id, hazard_type, source_category, title,
                 event_time_utc, latitude, longitude, magnitude, has_valid_point,
                 source_updated_utc, run_id)
            VALUES (%s, %s, %s, %s, 'earthquake', %s, %s, 10, 20, 3.5, true, %s, 'ld_test')
            """,
            (key, source, key.split(":")[1], hazard, title, NOW, updated),
        )


def fact_row(conn, key):
    with conn.cursor() as cur:
        cur.execute("SELECT title, first_seen_utc, last_seen_utc FROM dw.fact_event WHERE event_key = %s", (key,))
        return cur.fetchone()


def test_new_events_are_inserted(conn):
    stage(conn, "usgs:ld_1")
    stage(conn, "usgs:ld_2")
    result = load.merge_staging_to_fact(conn)
    assert result == {"rows_staged": 2, "rows_inserted": 2, "rows_updated": 0, "rows_unchanged": 0}
    assert fact_row(conn, "usgs:ld_1")[0] == "old title"


def test_running_the_merge_twice_changes_nothing(conn):
    stage(conn, "usgs:ld_1")
    load.merge_staging_to_fact(conn)
    again = load.merge_staging_to_fact(conn)
    assert again == {"rows_staged": 1, "rows_inserted": 0, "rows_updated": 0, "rows_unchanged": 1}
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dw.fact_event WHERE event_key = 'usgs:ld_1'")
        assert cur.fetchone()[0] == 1


def test_newer_version_updates_the_row_and_keeps_first_seen(conn):
    stage(conn, "usgs:ld_1")
    load.merge_staging_to_fact(conn)
    first_seen = fact_row(conn, "usgs:ld_1")[1]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.stg_event")
    stage(conn, "usgs:ld_1", title="new title", updated=NOW + timedelta(hours=1))
    result = load.merge_staging_to_fact(conn)

    title, first_seen_after, _ = fact_row(conn, "usgs:ld_1")
    assert result["rows_updated"] == 1
    assert title == "new title"
    assert first_seen_after == first_seen


def test_older_version_is_ignored(conn):
    stage(conn, "usgs:ld_1", title="current")
    load.merge_staging_to_fact(conn)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.stg_event")
    stage(conn, "usgs:ld_1", title="stale", updated=NOW - timedelta(hours=1))
    result = load.merge_staging_to_fact(conn)

    assert result["rows_updated"] == 0
    assert fact_row(conn, "usgs:ld_1")[0] == "current"


def test_unknown_hazard_type_is_refused(conn):
    stage(conn, "usgs:ld_1", hazard="not_a_hazard")
    with pytest.raises(ValueError, match="unknown source or hazard"):
        load.merge_staging_to_fact(conn)


def test_refresh_marts_runs(conn):
    conn.rollback()  # the refresh needs a connection that has not started a transaction
    load.refresh_marts(conn)


def test_dq_results_are_saved_for_the_run(conn):
    summary = load.run_dq_checks(conn, "ld_test_run")
    assert summary["rules"] == 9
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM ops.dq_results WHERE run_id = 'ld_test_run'")
        assert cur.fetchone()[0] == 9


def test_record_run_overwrites_the_same_run(conn):
    start = NOW
    load.record_run(conn, "ld_test_run", "disasterlens_pipeline", "failed", start, start + timedelta(seconds=5),
                    error_message="boom")
    load.record_run(conn, "ld_test_run", "disasterlens_pipeline", "success", start, start + timedelta(seconds=9),
                    rows_fetched=10, rows_inserted=4, rows_updated=1, rows_rejected=2)
    with conn.cursor() as cur:
        cur.execute("SELECT status, duration_seconds, rows_inserted, error_message FROM ops.pipeline_runs "
                    "WHERE run_id = 'ld_test_run'")
        assert cur.fetchall() == [("success", 9.0, 4, None)]