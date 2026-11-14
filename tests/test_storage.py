"""
Tests for the data lake helpers (src/storage.py).

These tests only check how paths are built, so they do not need MinIO running.
The real upload is checked by hand with src/try_storage.py.
"""
import pytest

from src.storage import put_raw, raw_folder, safe_run_id


def test_safe_run_id_replaces_special_characters():
    # ':' and '+' are not safe in paths, so they become '_'
    assert safe_run_id("scheduled__2026-11-14T10:00:00+00:00") == "scheduled__2026-11-14T10_00_00_00_00"


def test_safe_run_id_keeps_simple_names():
    assert safe_run_id("backfill_usgs_2026-03") == "backfill_usgs_2026-03"


def test_raw_folder_follows_the_agreed_layout():
    folder = raw_folder("usgs", "manual__2026-11-14T10:00:00+00:00", "2026-11-14")
    assert folder == "raw/source=usgs/ingest_date=2026-11-14/run_id=manual__2026-11-14T10_00_00_00_00"


def test_same_run_gives_same_folder():
    # Same run -> same path, so a retry overwrites instead of duplicating
    a = raw_folder("eonet", "run_1", "2026-11-14")
    b = raw_folder("eonet", "run_1", "2026-11-14")
    assert a == b


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError):
        put_raw("twitter", "run_1", {}, {}, client=object())