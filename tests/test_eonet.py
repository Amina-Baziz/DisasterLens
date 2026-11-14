"""
Tests for the EONET client (src/eonet.py).

They use the saved sample files in tests/fixtures/,
so they run without internet and always give the same result.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.eonet import check_events_payload, extract_events, parse_time

# Folder with the sample files
FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    """Read a sample JSON file from tests/fixtures/."""
    return json.loads((FIXTURES / name).read_text())


def test_ids_are_parsed():
    # Every event keeps its EONET id, in the same order
    events = extract_events(load("eonet_sample.json"))
    assert [e["id"] for e in events] == ["EONET_1001", "EONET_1002", "EONET_1003"]


def test_most_recent_point_is_kept():
    # The storm has 2 points; we keep the newest one: [longitude, latitude]
    storm = extract_events(load("eonet_sample.json"))[0]
    assert storm["longitude"] == -61.0
    assert storm["latitude"] == 16.0
    assert storm["has_valid_point"] is True


def test_event_time_is_the_latest_geometry_date_in_utc():
    storm = extract_events(load("eonet_sample.json"))[0]
    assert storm["event_time_utc"] == datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
    assert parse_time("2026-11-11T12:00:00Z").tzinfo == timezone.utc


def test_polygon_only_event_has_no_location():
    # An event with only an area shape gets an empty location; we never invent a center
    fire = extract_events(load("eonet_sample.json"))[1]
    assert fire["longitude"] is None
    assert fire["latitude"] is None
    assert fire["has_valid_point"] is False


def test_open_and_closed_status():
    # 'closed' holds a date in EONET; no date means the event is still open
    events = extract_events(load("eonet_sample.json"))
    assert [e["status"] for e in events] == ["open", "open", "closed"]


def test_category_is_read():
    events = extract_events(load("eonet_sample.json"))
    assert [e["category"] for e in events] == ["severeStorms", "wildfires", "volcanoes"]


def test_empty_response_is_valid():
    # 0 events is a normal answer, not an error
    payload = load("eonet_empty.json")
    assert check_events_payload(payload) == 0
    assert extract_events(payload) == []


def test_wrong_response_is_rejected():
    # A broken answer must raise an error, so Airflow marks the task as failed
    with pytest.raises(ValueError):
        check_events_payload({"error": "something went wrong"})