"""
USGS earthquake API client.

This file downloads earthquake data from the USGS API (United States Geological Survey).
It also contains small shared helpers (HTTP session, time conversion) that the
EONET client (src/eonet.py) reuses, so both sources behave the same way.

Two ways to fetch data:
  - fetch_feed()          -> last 24 hours, used by the hourly Airflow DAG
  - fetch_fdsn(start,end) -> a date range, used by the backfill DAG (month by month)
"""

import os
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =====================================================================
# Settings
# =====================================================================

# Hourly feed: all earthquakes from the last 24 hours.
# Read from .env (USGS_API_URL); the default is used if .env does not set it.
FEED_URL = os.getenv(
    "USGS_API_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
)

# Query endpoint: lets us ask for a date range (used for the backfill).
FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Give up on a request if USGS does not answer within 30 seconds.
TIMEOUT_SECONDS = 30

# Polite name that tells the API who is calling.
USER_AGENT = "DisasterLens/1.0 (university data engineering project)"


# =====================================================================
# Shared helpers (also used by the EONET client)
# =====================================================================

def make_session():
    """
    Create an HTTP session that automatically retries when the API has a temporary problem.

    - Retries up to 3 times.
    - Waits longer each time (about 2s, 4s, 8s) so we do not overload the API.
    - Only retries on temporary errors:
        429 = too many requests, 500/502/503/504 = server problems.
    """
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def utc_now_iso():
    """Return the current time in UTC as text, e.g. '2026-11-14T10:00:00+00:00'."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ms_to_utc(ms):
    """
    Convert a USGS time to a normal UTC date.

    USGS gives times as milliseconds since 1 January 1970,
    for example 1794611157912 -> 2026-11-13 ... UTC.
    """
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


# =====================================================================
# USGS specific functions
# =====================================================================

def check_feature_collection(payload):
    """
    Check that the API answer has the expected shape.

    A good USGS answer looks like:
        {"type": "FeatureCollection", "features": [ ...earthquakes... ]}

    - If the shape is wrong -> raise an error (the Airflow task will fail and turn red).
    - If the list is empty  -> that is fine: 0 earthquakes is a valid answer.

    Returns the number of earthquakes.
    """
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
        raise ValueError("USGS response is not a GeoJSON FeatureCollection")
    if not isinstance(payload.get("features"), list):
        raise ValueError("USGS response has no 'features' list")
    return len(payload["features"])


def _get_json(url, params=None, session=None):
    """
    Call the API and return the answer.

    Steps:
      1. Send the request (with timeout and automatic retries).
      2. If the answer is still an error after retries (4xx/5xx) -> raise an error.
      3. Read the JSON and check its shape.

    Returns three things:
      payload     -> the full JSON answer (saved untouched in the data lake)
      status_code -> HTTP status, e.g. 200 (saved in _meta.json)
      final_url   -> the exact URL that was called (saved in _meta.json)
    """
    session = session or make_session()
    response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    check_feature_collection(payload)
    return payload, response.status_code, response.url


def fetch_feed(session=None):
    """Hourly run: get all earthquakes from the last 24 hours."""
    return _get_json(FEED_URL, session=session)


def fetch_fdsn(start, end, min_magnitude=2.5, session=None):
    """
    Backfill: get earthquakes between two dates.

    start, end    -> dates as text 'YYYY-MM-DD', e.g. '2026-10-01', '2026-11-01'
    min_magnitude -> only earthquakes of this magnitude or bigger (default 2.5)

    USGS returns at most 20,000 earthquakes per request,
    so the backfill calls this once per month.
    """
    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": min_magnitude,
        "orderby": "time-asc",  # oldest first
    }
    return _get_json(FDSN_URL, params=params, session=session)


def extract_events(payload):
    """
    Turn the raw JSON into a simple list of earthquakes (one small dict each).

    Used for quick checks and tests only.
    The real cleaning for the warehouse is done later by Spark (src/transform.py).

    Note: USGS coordinates are in the order [longitude, latitude, depth].
    """
    events = []
    for feature in payload.get("features", []):
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]

        events.append({
            "id": feature.get("id"),                         # unique USGS id
            "time_utc": ms_to_utc(props.get("time")),        # milliseconds -> UTC date
            "longitude": coords[0],                          # 1st value
            "latitude": coords[1],                           # 2nd value
            "depth_km": coords[2] if len(coords) > 2 else None,  # 3rd value
            "magnitude": props.get("mag"),                   # can be empty (None)
            "type": props.get("type"),                       # 'earthquake', 'quarry blast', ...
        })
    return events