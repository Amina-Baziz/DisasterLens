"""
Tests for the data quality rules in sql/queries.sql.

Each test plants a problem inside a database transaction, runs the rules, checks
that the right rule fails, and then rolls everything back. Nothing is saved.

They need the PostgreSQL container, so they run inside the Airflow container.
On a PC without psycopg2 or without a database they are skipped.
"""
import os
from pathlib import Path

import pytest

psycopg2 = pytest.importorskip("psycopg2")

QUERIES = Path(__file__).parent.parent / "sql" / "queries.sql"

EXPECTED_RULES = {
    "pk_unique", "required_fields", "coords_in_range", "summary_matches_fact",
    "no_future_events", "missing_point_share", "unmapped_category",
    "rejected_rows", "freshness",
}


def dq_sql():
    """The data quality statement: everything after the 'Part 2' heading."""
    text = QUERIES.read_text()
    start = text.index("SELECT", text.index("Part 2: data quality rules"))
    return text[start:]


@pytest.fixture
def conn():
    """A database connection whose changes are always rolled back."""
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
    yield connection
    connection.rollback()
    connection.close()


def run_rules(conn):
    """Run all rules and return {rule_name: (severity, failed_count)}."""
    with conn.cursor() as cur:
        cur.execute(dq_sql())
        return {name: (severity, failed) for name, severity, failed, _passed, _detail in cur.fetchall()}


def add_event(conn, key, hazard_key=1, time_sql="now() AT TIME ZONE 'utc'", lat="10", lon="20"):
    """Insert one test event (rolled back at the end of the test)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO dw.fact_event
                (event_key, source_key, hazard_key, date_key, source_event_id,
                 event_time_utc, latitude, longitude, has_valid_point)
            VALUES (%s, 1, %s, to_char(({time_sql}), 'YYYYMMDD')::int, %s,
                    ({time_sql}), {lat}, {lon}, true)
            """,
            (key, hazard_key, key),
        )


def test_every_rule_is_reported(conn):
    assert set(run_rules(conn)) == EXPECTED_RULES


def test_no_error_rule_fails_on_the_current_data(conn):
    results = run_rules(conn)
    failed_errors = [name for name, (severity, failed) in results.items()
                     if severity == "error" and failed > 0]
    assert failed_errors == []


def test_summary_mismatch_is_detected(conn):
    # A new fact row that is not in the daily summary yet must break summary_matches_fact
    add_event(conn, "usgs:dq_test_1")
    assert run_rules(conn)["summary_matches_fact"][1] >= 1


def test_future_event_is_flagged(conn):
    add_event(conn, "usgs:dq_test_2", time_sql="(now() AT TIME ZONE 'utc') + interval '3 days'")
    assert run_rules(conn)["no_future_events"][1] >= 1


def test_unmapped_category_is_flagged(conn):
    # hazard_key 13 is 'other'
    add_event(conn, "eonet:dq_test_3", hazard_key=13)
    assert run_rules(conn)["unmapped_category"][1] >= 1


def test_database_refuses_coordinates_out_of_range(conn):
    # The table has CHECK constraints, so a bad row can never get in
    with pytest.raises(psycopg2.errors.CheckViolation):
        add_event(conn, "usgs:dq_test_4", lat="95", lon="20")