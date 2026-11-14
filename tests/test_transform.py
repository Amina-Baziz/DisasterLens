"""
Tests for the Spark job (src/transform.py).

They run Spark locally on the sample files in tests/fixtures/ and on small
files written on the fly, so they need no internet and no database.
"""
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.transform import build_staging

# Folder with the sample files
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """One small local Spark session shared by all tests."""
    session = (SparkSession.builder
               .master("local[1]")
               .appName("disasterlens_tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def put_raw(tmp_path, source, content, run="run1"):
    """
    Save a raw answer in the same folder layout as the data lake.
    content is a sample file name (text) or a Python dict.
    """
    folder = tmp_path / "raw" / f"source={source}" / "ingest_date=2026-11-14" / f"run_id={run}"
    folder.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        text = (FIXTURES / content).read_text()
    else:
        text = json.dumps(content)
    (folder / "response.json").write_text(text)


def run(spark, tmp_path):
    """Run the job and return (rows by event_key, stats)."""
    staged, stats = build_staging(spark, str(tmp_path / "raw"), "test_run")
    return {r["event_key"]: r.asDict() for r in staged.collect()}, stats


def usgs_answer(*features):
    return {"type": "FeatureCollection", "features": list(features)}


def usgs_feature(event_id, updated=1762000600000, mag=4.0, kind="earthquake", coords=(10.0, 20.0, 5.0)):
    return {
        "id": event_id,
        "properties": {"time": 1762000000000, "updated": updated, "place": "Somewhere",
                       "mag": mag, "type": kind},
        "geometry": {"type": "Point", "coordinates": list(coords)},
    }


def eonet_answer(*events):
    return {"title": "EONET Events", "events": list(events)}


def eonet_event(event_id, category="wildfires"):
    return {
        "id": event_id, "title": "Event", "closed": None,
        "categories": [{"id": category, "title": category}],
        "geometry": [{"date": "2026-11-10T00:00:00Z", "type": "Point", "coordinates": [1.0, 2.0]}],
    }


def test_usgs_sample_counts(spark, tmp_path):
    # 3 rows read, the quarry blast is rejected, 2 earthquakes are staged
    put_raw(tmp_path, "usgs", "usgs_sample.json")
    rows, stats = run(spark, tmp_path)
    assert stats["rows_read"] == 3
    assert stats["rejected_by_reason"] == {"not_an_earthquake": 1}
    assert stats["rows_staged"] == 2
    assert len(rows) == 2


def test_quarry_blast_is_removed(spark, tmp_path):
    put_raw(tmp_path, "usgs", "usgs_sample.json")
    rows, _ = run(spark, tmp_path)
    assert "usgs:uu60123456" not in rows


def test_null_magnitude_stays_empty(spark, tmp_path):
    put_raw(tmp_path, "usgs", "usgs_sample.json")
    rows, _ = run(spark, tmp_path)
    assert rows["usgs:nc75012345"]["magnitude"] is None


def test_usgs_row_has_the_agreed_values(spark, tmp_path):
    put_raw(tmp_path, "usgs", "usgs_sample.json")
    row = run(spark, tmp_path)[0]["usgs:us7000abcd"]
    assert row["hazard_type"] == "earthquake"
    assert (row["longitude"], row["latitude"], row["depth_km"]) == (139.69, 34.6, 35.2)
    assert row["has_valid_point"] is True
    assert row["event_status"] is None
    assert row["run_id"] == "test_run"
    assert "source=usgs" in row["raw_object_path"]


def test_eonet_sample_counts(spark, tmp_path):
    put_raw(tmp_path, "eonet", "eonet_sample.json")
    rows, stats = run(spark, tmp_path)
    assert stats["rows_read"] == 3
    assert stats["rows_staged"] == 3
    assert rows["eonet:EONET_1001"]["hazard_type"] == "severe_storm"
    assert rows["eonet:EONET_1002"]["hazard_type"] == "wildfire"
    assert rows["eonet:EONET_1003"]["hazard_type"] == "volcano"


def test_eonet_keeps_the_most_recent_point(spark, tmp_path):
    put_raw(tmp_path, "eonet", "eonet_sample.json")
    storm = run(spark, tmp_path)[0]["eonet:EONET_1001"]
    assert (storm["longitude"], storm["latitude"]) == (-61.0, 16.0)
    assert storm["geometry_count"] == 2
    assert storm["has_valid_point"] is True


def test_eonet_polygon_only_has_no_point(spark, tmp_path):
    put_raw(tmp_path, "eonet", "eonet_sample.json")
    fire, stats = None, None
    rows, stats = run(spark, tmp_path)
    fire = rows["eonet:EONET_1002"]
    assert fire["latitude"] is None and fire["longitude"] is None
    assert fire["has_valid_point"] is False
    assert stats["rows_without_point"] == 1


def test_eonet_open_and_closed_status(spark, tmp_path):
    put_raw(tmp_path, "eonet", "eonet_sample.json")
    rows, _ = run(spark, tmp_path)
    assert rows["eonet:EONET_1001"]["event_status"] == "open"
    assert rows["eonet:EONET_1003"]["event_status"] == "closed"


def test_empty_answers_are_a_normal_run(spark, tmp_path):
    put_raw(tmp_path, "usgs", "usgs_empty.json")
    put_raw(tmp_path, "eonet", "eonet_empty.json")
    rows, stats = run(spark, tmp_path)
    assert rows == {}
    assert stats["rows_read"] == 0 and stats["rows_staged"] == 0


def test_duplicate_id_keeps_the_newest_version(spark, tmp_path):
    # The same earthquake in two runs; the later update has the corrected magnitude
    put_raw(tmp_path, "usgs", usgs_answer(usgs_feature("dup1", updated=1000, mag=4.0)), run="run1")
    put_raw(tmp_path, "usgs", usgs_answer(usgs_feature("dup1", updated=2000, mag=4.6)), run="run2")
    rows, stats = run(spark, tmp_path)
    assert len(rows) == 1
    assert rows["usgs:dup1"]["magnitude"] == 4.6
    assert stats["duplicates_removed"] == 1


def test_bad_coordinates_are_emptied_but_the_event_is_kept(spark, tmp_path):
    put_raw(tmp_path, "usgs", usgs_answer(usgs_feature("bad1", coords=(500.0, 95.0, 5.0))))
    row = run(spark, tmp_path)[0]["usgs:bad1"]
    assert row["latitude"] is None and row["longitude"] is None
    assert row["has_valid_point"] is False


def test_missing_id_is_rejected(spark, tmp_path):
    put_raw(tmp_path, "usgs", usgs_answer(usgs_feature(None), usgs_feature("ok1")))
    rows, stats = run(spark, tmp_path)
    assert list(rows) == ["usgs:ok1"]
    assert stats["rejected_by_reason"] == {"missing_id": 1}


def test_unknown_eonet_category_becomes_other(spark, tmp_path):
    put_raw(tmp_path, "eonet", eonet_answer(eonet_event("E1", category="somethingNew")))
    rows, stats = run(spark, tmp_path)
    assert rows["eonet:E1"]["hazard_type"] == "other"
    assert stats["unmapped_category"] == 1


def test_manmade_events_are_removed(spark, tmp_path):
    put_raw(tmp_path, "eonet", eonet_answer(eonet_event("E2", category="manmade"), eonet_event("E3")))
    rows, stats = run(spark, tmp_path)
    assert list(rows) == ["eonet:E3"]
    assert stats["rejected_by_reason"] == {"manmade_event": 1}


def test_both_sources_in_one_run(spark, tmp_path):
    put_raw(tmp_path, "usgs", "usgs_sample.json")
    put_raw(tmp_path, "eonet", "eonet_sample.json")
    rows, stats = run(spark, tmp_path)
    assert stats["rows_staged"] == 5
    assert {r["source"] for r in rows.values()} == {"usgs", "eonet"}