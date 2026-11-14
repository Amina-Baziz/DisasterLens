"""
NASA EONET client.

This file downloads natural event data (wildfires, storms, volcanoes, floods, ...)
from the NASA EONET API. It reuses the shared HTTP session from src/usgs.py,
so both sources have the same timeout, retries and User-Agent.

  - fetch_events(days) -> events from the last N days, used by the hourly DAG
                          and (with a bigger N) by the backfill
"""

import os
from datetime import datetime, timezone

from src.usgs import TIMEOUT_SECONDS, make_session


# =====================================================================
# Settings
# =====================================================================

# Read from .env (EONET_API_URL); the default is used if .env does not set it.
EVENTS_URL = os.getenv("EONET_API_URL", "https://eonet.gsfc.nasa.gov/api/v3/events")


# =====================================================================
# Download
# =====================================================================

def check_events_payload(payload):
    """
    Check that the API answer has the expected shape.

    A good EONET answer looks like:
        {"title": "EONET Events", "events": [ ...events... ]}

    - If the shape is wrong -> raise an error (the Airflow task will fail and turn red).
    - If the list is empty  -> that is fine: 0 events is a valid answer.

    Returns the number of events.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        raise ValueError("EONET response has no 'events' list")
    return len(payload["events"])


def fetch_events(days=1, status="all", session=None):
    """
    Get EONET events from the last N days.

    days   -> how many days back to look (including today)
    status -> 'open', 'closed' or 'all' (we use 'all' so we also see events that closed)

    Returns three things, like the USGS functions:
      payload     -> the full JSON answer (saved untouched in the data lake)
      status_code -> HTTP status, e.g. 200 (saved in _meta.json)
      final_url   -> the exact URL that was called (saved in _meta.json)
    """
    session = session or make_session()
    params = {"status": status, "days": days}
    response = session.get(EVENTS_URL, params=params, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    check_events_payload(payload)
    return payload, response.status_code, response.url


# =====================================================================
# Reading the answer (for quick checks and tests only)
# The real cleaning for the warehouse is done later by Spark (src/transform.py).
# =====================================================================

def parse_time(text):
    """Convert an EONET date like '2026-11-13T00:00:00Z' to a UTC datetime."""
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def _is_valid_point(geometry):
    """
    A valid point is a 'Point' with [longitude, latitude] inside the real ranges.
    Polygons (area shapes) are not points, so they are not valid here.
    """
    if geometry.get("type") != "Point":
        return False
    coords = geometry.get("coordinates")
    if not isinstance(coords, list) or len(coords) < 2:
        return False
    lon, lat = coords[0], coords[1]
    if not isinstance(lon, (int, float)) or not isinstance(lat, (int, float)):
        return False
    return -180 <= lon <= 180 and -90 <= lat <= 90


def extract_events(payload):
    """
    Turn the raw JSON into a simple list of events (one small dict each).

    Data contract for the location:
      - An event can have many geometries (a storm gets a new point every few hours).
      - We keep the most recent valid point as the location.
      - If the event only has polygons, the location stays empty (None).
        We never invent a center point. The full geometry stays in the raw file.

    event_time is the date of the most recent geometry of the event.
    Note: EONET coordinates are in the order [longitude, latitude].
    """
    events = []
    for event in payload.get("events", []):
        geometries = event.get("geometry") or []
        categories = event.get("categories") or []

        latest_geometry = max(geometries, key=lambda g: g.get("date") or "", default=None)
        points = [g for g in geometries if _is_valid_point(g)]
        latest_point = max(points, key=lambda g: g.get("date") or "", default=None)

        events.append({
            "id": event.get("id"),                                    # unique EONET id
            "title": event.get("title"),
            "category": categories[0].get("id") if categories else None,
            "status": "closed" if event.get("closed") else "open",   # 'closed' holds a date
            "event_time_utc": parse_time(latest_geometry.get("date")) if latest_geometry else None,
            "longitude": latest_point["coordinates"][0] if latest_point else None,
            "latitude": latest_point["coordinates"][1] if latest_point else None,
            "has_valid_point": latest_point is not None,
        })
    return events