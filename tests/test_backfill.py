"""
Tests for the backfill DAG (dags/backfill.py).

They check the month ranges, the shape of the DAG and the checks inside the tasks.
They do not call the APIs or MinIO. They need Airflow, so they run inside the
Airflow container and are skipped on a PC without Airflow.
"""
import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("airflow")

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
import backfill  # noqa: E402


class FakeTaskInstance:
    def __init__(self, notes):
        self.notes = notes

    def xcom_pull(self, task_ids):
        return self.notes


def test_twelve_months_end_with_the_current_month_stopping_tomorrow():
    ranges = backfill.month_ranges(12, date(2026, 11, 20))
    assert len(ranges) == 12
    assert ranges[0] == ("2025-12-01", "2026-01-01")
    assert ranges[-1] == ("2026-11-01", "2026-11-21")


def test_month_ranges_cross_the_new_year_correctly():
    assert backfill.month_ranges(3, date(2026, 1, 15)) == [
        ("2025-11-01", "2025-12-01"),
        ("2025-12-01", "2026-01-01"),
        ("2026-01-01", "2026-01-16"),
    ]


def test_dag_is_manual_and_has_the_same_tasks_as_the_hourly_dag():
    assert backfill.dag.dag_id == "disasterlens_backfill"
    assert backfill.dag.schedule_interval is None
    assert backfill.dag.catchup is False
    assert set(backfill.dag.task_ids) == {
        "extract_usgs_to_lake", "extract_eonet_to_lake", "validate_raw", "stage_raw_to_workdir",
        "spark_transform", "merge_to_warehouse", "refresh_marts", "record_run", "run_dq_checks",
    }


def test_defaults_are_twelve_months_and_a_year_of_eonet():
    assert backfill.dag.params["months_back"] == 12
    assert backfill.dag.params["eonet_days"] == 365


def test_each_month_is_saved_as_its_own_run(monkeypatch):
    saved = []
    monkeypatch.setattr(backfill, "make_sure_bucket_exists", lambda: None)
    monkeypatch.setattr(backfill.usgs, "make_session", lambda: None)
    monkeypatch.setattr(backfill.usgs, "fetch_fdsn",
                        lambda start, end, session=None: ({"features": [1, 2]}, 200, f"url?{start}"))
    monkeypatch.setattr(backfill, "save_part", lambda *args: saved.append(args[1]))

    note = backfill.extract_usgs_to_lake("manual__r1", {"months_back": 3, "eonet_days": 10})

    assert note == {"source": "usgs", "files": 3, "record_count": 6}
    assert len(set(saved)) == 3                       # three different run ids, so no overwriting
    assert all(name.startswith("manual__r1__usgs_") for name in saved)


def test_validate_passes_when_every_file_is_in_the_lake(monkeypatch):
    files = [f"raw/source=usgs/ingest_date=d/run_id=r__usgs_{m}/response.json" for m in ("a", "b")]
    files.append("raw/source=eonet/ingest_date=d/run_id=r__eonet/response.json")
    monkeypatch.setattr(backfill, "list_backfill_files", lambda run_id: files)
    notes = [{"files": 2, "record_count": 10}, {"files": 1, "record_count": 4}]
    result = backfill.validate_raw("r", FakeTaskInstance(notes))
    assert result == {"files": 3, "usgs_records": 10, "eonet_records": 4}


def test_validate_fails_when_a_month_is_missing(monkeypatch):
    files = ["raw/source=usgs/ingest_date=d/run_id=r__usgs_a/response.json",
             "raw/source=eonet/ingest_date=d/run_id=r__eonet/response.json"]
    monkeypatch.setattr(backfill, "list_backfill_files", lambda run_id: files)
    notes = [{"files": 2, "record_count": 10}, {"files": 1, "record_count": 4}]
    with pytest.raises(ValueError, match="usgs: expected 2 files, found 1"):
        backfill.validate_raw("r", FakeTaskInstance(notes))