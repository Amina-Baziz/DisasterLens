"""
Tests for the USGS client (src/usgs.py).

They use the saved sample files in tests/fixtures/,
so they run without internet and always give the same result.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.usgs import check_feature_collection, extract_events, ms_to_utc

# Folder with the sample files
FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    """Read a sample JSON file from tests/fixtures/."""
    return json.loads((FIXTURES / name).read_text())


def test_ids_are_parsed():
    # Every earthquake keeps its USGS id, in the same order
    events = extract_events(load("usgs_sample.json"))
    assert [e["id"] for e in events] == ["us7000abcd", "nc75012345", "uu60123456"]


def test_milliseconds_are_converted_to_utc():
    # 0 ms = 1 January 1970, and the result is in UTC
    assert ms_to_utc(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)
    first = extract_events(load("usgs_sample.json"))[0]
    assert first["time_utc"].tzinfo == timezone.utc
    assert first["time_utc"].year == 2025


def test_coordinates_order_is_lon_lat_depth():
    # USGS sends [longitude, latitude, depth]; we must not swap them
    first = extract_events(load("usgs_sample.json"))[0]
    assert first["longitude"] == 139.69
    assert first["latitude"] == 34.6
    assert first["depth_km"] == 35.2


def test_null_magnitude_is_kept_as_none():
    # A missing magnitude stays empty; it does not crash the code
    second = extract_events(load("usgs_sample.json"))[1]
    assert second["magnitude"] is None


def test_quarry_blast_type_is_kept_for_spark_to_filter():
    # Non-earthquakes are kept here; Spark removes and counts them later
    types = [e["type"] for e in extract_events(load("usgs_sample.json"))]
    assert "quarry blast" in types


def test_empty_response_is_valid():
    # 0 earthquakes is a normal answer, not an error
    payload = load("usgs_empty.json")
    assert check_feature_collection(payload) == 0
    assert extract_events(payload) == []


def test_wrong_response_is_rejected():
    # A broken answer must raise an error, so Airflow marks the task as failed
    with pytest.raises(ValueError):
        check_feature_collection({"error": "something went wrong"})