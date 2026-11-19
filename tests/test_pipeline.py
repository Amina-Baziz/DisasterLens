"""
Tests for the hourly DAG (dags/pipeline.py).

They check the shape of the DAG and the small checks inside the tasks.
They do not call the APIs or MinIO. They need Airflow, so they run inside the
Airflow container and are skipped on a PC without Airflow.
"""
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))
import pipeline  # noqa: E402


class FakeTaskInstance:
    """Stands in for Airflow's task instance: returns the notes the download tasks would leave."""

    def __init__(self, notes):
        self.notes = notes

    def xcom_pull(self, task_ids):
        return self.notes


def notes_for(run_folder="raw/source={}/ingest_date=2026-11-20/run_id=r1"):
    return [
        {"source": "usgs", "path": run_folder.format("usgs") + "/response.json", "record_count": 10},
        {"source": "eonet", "path": run_folder.format("eonet") + "/response.json", "record_count": 4},
    ]


def test_dag_has_the_expected_tasks():
    assert pipeline.dag.dag_id == "disasterlens_pipeline"
    assert set(pipeline.dag.task_ids) == {
        "extract_usgs_to_lake", "extract_eonet_to_lake", "validate_raw", "stage_raw_to_workdir",
        "spark_transform", "merge_to_warehouse", "refresh_marts", "record_run", "run_dq_checks",
    }


def test_dag_runs_hourly_without_catchup_and_one_at_a_time():
    assert pipeline.dag.catchup is False
    assert pipeline.dag.max_active_runs == 1
    assert str(pipeline.dag.schedule_interval) == "@hourly"


def test_downloads_run_before_the_check_and_the_check_before_staging():
    validate = pipeline.dag.get_task("validate_raw")
    assert validate.upstream_task_ids == {"extract_usgs_to_lake", "extract_eonet_to_lake"}
    assert pipeline.dag.get_task("stage_raw_to_workdir").upstream_task_ids == {"validate_raw"}


def test_warehouse_steps_run_in_order():
    dag = pipeline.dag
    assert dag.get_task("spark_transform").upstream_task_ids == {"stage_raw_to_workdir"}
    assert dag.get_task("merge_to_warehouse").upstream_task_ids == {"spark_transform"}
    assert dag.get_task("refresh_marts").upstream_task_ids == {"merge_to_warehouse"}


def test_run_is_logged_even_after_a_failure_and_quality_runs_last():
    dag = pipeline.dag
    assert dag.get_task("record_run").trigger_rule == "all_done"
    assert dag.get_task("run_dq_checks").upstream_task_ids == {"refresh_marts", "record_run"}


def test_spark_summary_line_is_found_in_the_output():
    output = 'WARN something\nSPARK_SUMMARY {"rows_read": 5, "rows_rejected": 1}\n'
    assert pipeline.parse_spark_summary(output) == {"rows_read": 5, "rows_rejected": 1}


def test_missing_spark_summary_is_an_error():
    with pytest.raises(RuntimeError, match="SPARK_SUMMARY"):
        pipeline.parse_spark_summary("nothing useful here")


def test_run_outcome_is_success_when_nothing_failed():
    states = {"validate_raw": "success", "spark_transform": "success"}
    assert pipeline.run_outcome(states) == ("success", None)


def test_run_outcome_names_the_first_broken_task():
    states = {"spark_transform": "failed", "merge_to_warehouse": "upstream_failed", "validate_raw": "success"}
    assert pipeline.run_outcome(states) == ("failed", "spark_transform failed")


def test_validate_passes_when_all_files_are_in_the_lake(monkeypatch):
    notes = notes_for()
    files = [n["path"] for n in notes] + [n["path"].replace("response", "_meta") for n in notes]
    monkeypatch.setattr(pipeline.storage, "list_run", lambda run_id: files)
    result = pipeline.validate_raw("r1", FakeTaskInstance(notes))
    assert result == {"files": 4, "usgs_records": 10, "eonet_records": 4}


def test_validate_fails_when_a_source_is_missing(monkeypatch):
    notes = notes_for()
    only_usgs = [notes[0]["path"], notes[0]["path"].replace("response", "_meta")]
    monkeypatch.setattr(pipeline.storage, "list_run", lambda run_id: only_usgs)
    with pytest.raises(ValueError, match="eonet"):
        pipeline.validate_raw("r1", FakeTaskInstance(notes))


def test_staging_uses_one_folder_per_run(monkeypatch):
    seen = {}

    def fake_download(run_id, folder):
        seen["folder"] = folder
        return ["a", "b", "c"]

    monkeypatch.setattr(pipeline.storage, "download_run", fake_download)
    result = pipeline.stage_raw_to_workdir("scheduled__2026-11-20T10:00:00+00:00")
    assert result["files"] == 3
    assert result["workdir"].endswith("scheduled__2026-11-20T10_00_00_00_00")
    assert seen["folder"] == result["workdir"]