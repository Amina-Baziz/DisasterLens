"""
DisasterLens dashboard (Streamlit).

Shows natural hazard events from two sources:
  - USGS: earthquakes measured by instruments
  - NASA EONET: wildfires, storms, floods, volcanoes and more, curated from news and satellite sources

It only reads the mart views and the daily summary in PostgreSQL. It never writes anything.

Run it on your own PC (the containers must be running):
    streamlit run dashboard/app.py
"""

from contextlib import closing
from pathlib import Path
import os

import altair as alt
import pandas as pd
import psycopg2
import pydeck as pdk
import streamlit as st


st.set_page_config(page_title="DisasterLens", layout="wide")


# =====================================================================
# Database connection
# =====================================================================

def load_env_file():
    """Read KEY=VALUE lines from the project's .env file (used on your own PC)."""
    path = Path(__file__).resolve().parent.parent / ".env"
    values = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def db_settings():
    """
    Where to find PostgreSQL.
    Inside Docker the database is called 'postgres'. On your own PC it is
    'localhost' on the port you chose in .env (POSTGRES_HOST_PORT).
    """
    env = {**load_env_file(), **os.environ}
    in_docker = Path("/.dockerenv").exists()
    if in_docker:
        host, port = env.get("POSTGRES_HOST", "postgres"), env.get("POSTGRES_PORT", "5432")
    else:
        host, port = "localhost", env.get("POSTGRES_HOST_PORT", "5432")
    return {
        "host": env.get("DASHBOARD_DB_HOST", host),
        "port": env.get("DASHBOARD_DB_PORT", port),
        "dbname": env.get("POSTGRES_DB", "disasterlens"),
        "user": env.get("DASHBOARD_DB_USER", env.get("POSTGRES_USER")),
        "password": env.get("DASHBOARD_DB_PASSWORD", env.get("POSTGRES_PASSWORD")),
    }


@st.cache_data(ttl=300)  # keep each answer for 5 minutes, so the page stays fast
def run_query(sql, params=()):
    """Run one SELECT and return the result as a table (DataFrame)."""
    with closing(psycopg2.connect(connect_timeout=5, **db_settings())) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [column[0] for column in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)


# =====================================================================
# Colors for the hazard groups (same colors on the map and in the legend)
# =====================================================================

GROUP_COLORS = {
    "geophysical":    "#ff4b4b",   # red
    "climatological": "#ffa421",   # orange
    "meteorological": "#1c83e1",   # blue
    "hydrological":   "#21c354",   # green
    "cryospheric":    "#803df5",   # violet
    "other":          "#808495",   # gray
}
LEGEND = (
    ":red[●] Geophysical &nbsp; :orange[●] Climatological &nbsp; :blue[●] Meteorological &nbsp; "
    ":green[●] Hydrological &nbsp; :violet[●] Cryospheric &nbsp; :gray[●] Other"
)


def hex_to_rgb(color):
    """'#ff4b4b' -> [255, 75, 75]"""
    return [int(color[i:i + 2], 16) for i in (1, 3, 5)]


def draw_map(events, radius_by_magnitude=False, height=480):
    """
    Draw events as colored dots on a world map. Hovering a dot shows its details.
    Colors follow the hazard group, same as the legend.
    """
    magnitude = events["magnitude"].astype(float)
    dots = pd.DataFrame({
        "latitude": events["latitude"].astype(float),
        "longitude": events["longitude"].astype(float),
        "color": events["hazard_group"].map(lambda g: hex_to_rgb(GROUP_COLORS.get(g, GROUP_COLORS["other"]))),
        "radius": (magnitude.fillna(0).clip(lower=0.5) ** 3 * 600) if radius_by_magnitude else 40000,
        "title": events["title"].fillna("(no title)"),
        "hazard": events["display_name"],
        "source": events["source_name"],
        "when": events["event_time_utc"].dt.strftime("%Y-%m-%d %H:%M UTC"),
        "detail": magnitude.map(lambda m: "" if pd.isna(m) else f"Magnitude {m:.1f}"),
    })
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=dots,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius="radius",
        radius_min_pixels=3,
        radius_max_pixels=25,
        opacity=0.75,
        pickable=True,
    )
    tooltip = {
        "html": "<b>{title}</b><br/>{hazard} ({source})<br/>{when}<br/>{detail}",
        "style": {"backgroundColor": "#262730", "color": "white", "fontSize": "13px"},
    }
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(latitude=20, longitude=0, zoom=1),
        map_style="light",
        tooltip=tooltip,
    )
    st.pydeck_chart(deck, height=height)


# =====================================================================
# Page header and connection check
# =====================================================================

st.title("DisasterLens")
st.caption("Natural hazards around the world, from USGS (earthquakes) and NASA EONET (other natural events).")

try:
    bounds = run_query(
        "SELECT min(full_date) AS first_day, max(full_date) AS last_day FROM mart.mv_daily_hazard_summary"
    )
except Exception as error:
    st.error("Could not read from the database. Check that the containers are running (docker compose ps).")
    st.caption(f"Details: {error}")
    st.stop()

if bounds["first_day"].isna().all():
    st.info("The warehouse has no events yet. Run the pipeline, then reload this page.")
    st.stop()

first_day, last_day = bounds.loc[0, "first_day"], bounds.loc[0, "last_day"]
all_hazards = run_query(
    "SELECT DISTINCT display_name FROM mart.mv_daily_hazard_summary ORDER BY display_name"
)["display_name"].tolist()


# =====================================================================
# Sidebar filters
# =====================================================================

st.sidebar.header("Filters")
default_start = max(first_day, last_day - pd.Timedelta(days=29).to_pytimedelta())
period = st.sidebar.date_input(
    "Date range", value=(default_start, last_day), min_value=first_day, max_value=last_day
)
if len(period) != 2:
    st.info("Pick an end date for the date range.")
    st.stop()
start, end = period

sources = st.sidebar.multiselect("Source", ["usgs", "eonet"], default=["usgs", "eonet"])
hazards = st.sidebar.multiselect("Hazard type", all_hazards, default=all_hazards)
min_magnitude = st.sidebar.slider(
    "Minimum magnitude (earthquakes only)", 0.0, 8.0, 0.0, 0.1,
    help="Hides earthquakes below this magnitude on the map and in the histogram. Other hazards are not affected.",
)
period_text = f"{start:%d %b %Y} to {end:%d %b %Y}"


# =====================================================================
# Data for the tiles
# =====================================================================

filters = (start, end, sources, hazards)

by_hazard = run_query(
    """
    SELECT display_name AS hazard, source_name AS source, sum(event_count)::int AS events
    FROM mart.mv_daily_hazard_summary
    WHERE full_date BETWEEN %s AND %s AND source_name = ANY(%s) AND display_name = ANY(%s)
    GROUP BY display_name, source_name
    ORDER BY events DESC
    """,
    filters,
)

if by_hazard.empty:
    st.info("No events match these filters. Try a wider date range, or select more sources or hazard types.")
    st.stop()

daily = run_query(
    """
    SELECT full_date, hazard_group, sum(event_count)::int AS events
    FROM mart.mv_daily_hazard_summary
    WHERE full_date BETWEEN %s AND %s AND source_name = ANY(%s) AND display_name = ANY(%s)
    GROUP BY full_date, hazard_group
    ORDER BY full_date
    """,
    filters,
)

located = run_query(
    """
    SELECT title, latitude, longitude, magnitude, hazard_group, display_name, source_name, event_time_utc, event_status
    FROM mart.v_event_map
    WHERE event_time_utc::date BETWEEN %s AND %s AND source_name = ANY(%s) AND display_name = ANY(%s)
    """,
    filters,
)

latest = run_query("SELECT * FROM mart.v_latest_events")
coverage = run_query("SELECT max(last_seen_utc) AS last_seen FROM mart.v_source_coverage")

total_events = int(by_hazard["events"].sum())
located_events = len(located)

# The minimum magnitude only hides earthquakes.
is_quake = located["display_name"] == "Earthquake"
visible = located[~is_quake | (located["magnitude"] >= min_magnitude)]


# =====================================================================
# Top numbers
# =====================================================================

c1, c2, c3 = st.columns(3)
c1.metric("Events in this view", f"{total_events:,}")
c2.metric("Events with a location", f"{located_events:,}")
last_seen = coverage.loc[0, "last_seen"]
c3.metric("Data last refreshed (UTC)", "no data" if pd.isna(last_seen) else f"{last_seen:%Y-%m-%d %H:%M}")

tab_overview, tab_quakes, tab_other, tab_events, tab_quality = st.tabs(
    ["Overview", "Earthquakes", "Other hazards", "Events", "Data quality"]
)


# =====================================================================
# Tab 1: overview (map, events by hazard, daily trend)
# =====================================================================

with tab_overview:
    st.subheader("Where events happened")
    st.caption(f"{located_events:,} of {total_events:,} events have a location. "
               "Events that only have an area shape (no single point) are counted but not drawn. "
               "Hover a dot for details.")
    st.markdown(LEGEND)
    if visible.empty:
        st.info("No events with a location match these filters.")
    else:
        draw_map(visible)

    left, right = st.columns(2)

    with left:
        st.subheader("Events by hazard type")
        bar = (
            alt.Chart(by_hazard)
            .mark_bar()
            .encode(
                x=alt.X("events:Q", title="Number of events"),
                y=alt.Y("hazard:N", sort="-x", title="Hazard type"),
                color=alt.Color("source:N", title="Source"),
                tooltip=["hazard", "source", "events"],
            )
            .properties(title=f"Events by hazard type and source, {period_text}", height=320)
        )
        st.altair_chart(bar, width="stretch")

    with right:
        st.subheader("Daily trend")
        line = (
            alt.Chart(daily)
            .mark_line(point=True)
            .encode(
                x=alt.X("full_date:T", title="Date"),
                y=alt.Y("events:Q", title="Events per day"),
                color=alt.Color(
                    "hazard_group:N", title="Hazard group",
                    scale=alt.Scale(domain=list(GROUP_COLORS), range=list(GROUP_COLORS.values())),
                ),
                tooltip=["full_date", "hazard_group", "events"],
            )
            .properties(title=f"Events per day by hazard group, {period_text}", height=320)
        )
        st.altair_chart(line, width="stretch")


# =====================================================================
# Tab 2: earthquakes (map sized by magnitude, histogram, strongest events)
# =====================================================================

with tab_quakes:
    quakes = visible[(visible["display_name"] == "Earthquake") & visible["magnitude"].notna()].copy()
    if quakes.empty:
        st.info("No earthquakes with a magnitude match these filters.")
    else:
        strongest = quakes["magnitude"].astype(float).max()
        q1, q2, q3 = st.columns(3)
        q1.metric("Earthquakes", f"{len(quakes):,}")
        q2.metric("Strongest magnitude", f"{strongest:.1f}")
        q3.metric("Average magnitude", f"{quakes['magnitude'].astype(float).mean():.2f}")

        st.subheader("Earthquake map")
        st.caption("Bigger circles are stronger earthquakes. Hover a dot for details.")
        quakes["magnitude"] = quakes["magnitude"].astype(float)
        draw_map(quakes, radius_by_magnitude=True)

        left, right = st.columns(2)
        with left:
            st.subheader("Earthquake magnitudes")
            buckets = (
                quakes.assign(bucket=quakes["magnitude"].apply(lambda m: int(m // 1)))
                .groupby("bucket").size().reset_index(name="earthquakes")
            )
            hist = (
                alt.Chart(buckets)
                .mark_bar()
                .encode(
                    x=alt.X("bucket:O", title="Magnitude (from, in steps of 1)"),
                    y=alt.Y("earthquakes:Q", title="Number of earthquakes"),
                    tooltip=["bucket", "earthquakes"],
                )
                .properties(title=f"Earthquakes by magnitude, {period_text}", height=320)
            )
            st.altair_chart(hist, width="stretch")

        with right:
            st.subheader("Strongest earthquakes")
            top = quakes.sort_values("magnitude", ascending=False).head(10)
            st.dataframe(
                top[["event_time_utc", "magnitude", "title"]],
                hide_index=True,
                height=320,
            )


# =====================================================================
# Tab 3: other hazards (wildfires, storms, floods and more, from EONET)
# =====================================================================

with tab_other:
    others = located[located["display_name"] != "Earthquake"].copy()
    if others.empty:
        st.info("No events of other hazard types match these filters.")
    else:
        kinds = sorted(others["display_name"].unique())
        chosen = st.multiselect("Hazard types to show", kinds, default=kinds)
        others = others[others["display_name"].isin(chosen)]

        o1, o2, o3 = st.columns(3)
        o1.metric("Events with a location", f"{len(others):,}")
        o2.metric("Still open", f"{int((others['event_status'] == 'open').sum()):,}")
        o3.metric("Closed", f"{int((others['event_status'] == 'closed').sum()):,}")

        st.subheader("Where they happened")
        st.markdown(LEGEND)
        if others.empty:
            st.info("Pick at least one hazard type.")
        else:
            draw_map(others)

        left, right = st.columns(2)
        with left:
            st.subheader("Open and closed events")
            status_counts = (
                others.assign(status=others["event_status"].fillna("unknown"))
                .groupby(["display_name", "status"]).size().reset_index(name="events")
            )
            status_chart = (
                alt.Chart(status_counts)
                .mark_bar()
                .encode(
                    x=alt.X("events:Q", title="Number of events"),
                    y=alt.Y("display_name:N", sort="-x", title="Hazard type"),
                    color=alt.Color("status:N", title="Status"),
                    tooltip=["display_name", "status", "events"],
                )
                .properties(title=f"Open and closed events by hazard type, {period_text}", height=320)
            )
            st.altair_chart(status_chart, width="stretch")

        with right:
            st.subheader("Daily trend by hazard type")
            per_day = (
                others.assign(day=others["event_time_utc"].dt.floor("D"))
                .groupby(["day", "display_name"]).size().reset_index(name="events")
            )
            trend = (
                alt.Chart(per_day)
                .mark_line(point=True)
                .encode(
                    x=alt.X("day:T", title="Date"),
                    y=alt.Y("events:Q", title="Events per day"),
                    color=alt.Color("display_name:N", title="Hazard type"),
                    tooltip=["day", "display_name", "events"],
                )
                .properties(title=f"Events per day by hazard type, {period_text}", height=320)
            )
            st.altair_chart(trend, width="stretch")


# =====================================================================
# Tab 4: latest events (with a search box)
# =====================================================================

with tab_events:
    st.subheader("Latest events")
    st.caption("The 200 most recent events overall, narrowed by the source and hazard filters.")
    search = st.text_input("Search in the title", placeholder="for example: Alaska, Japan, fire")
    shown = latest[latest["source_name"].isin(sources) & latest["hazard"].isin(hazards)]
    if search:
        shown = shown[shown["title"].fillna("").str.contains(search, case=False, regex=False)]
    if shown.empty:
        st.info("No events match these filters or this search.")
    else:
        st.dataframe(
            shown[["event_time_utc", "source_name", "hazard", "title", "magnitude", "event_status"]],
            hide_index=True,
            height=480,
        )


# =====================================================================
# Tab 5: data quality (rules, source coverage, pipeline runs)
# =====================================================================

def quality_sql():
    """The data quality statement from sql/queries.sql (everything after the 'Part 2' heading)."""
    path = Path(__file__).resolve().parent.parent / "sql" / "queries.sql"
    text = path.read_text()
    return text[text.index("SELECT", text.index("Part 2: data quality rules")):]


with tab_quality:
    st.subheader("Data quality rules")
    st.caption("Checked live on the warehouse. Errors mean the data is wrong. Warnings are worth a look.")
    try:
        rules = run_query(quality_sql())
        rules["result"] = rules.apply(
            lambda r: "pass" if r["passed"] else ("FAIL" if r["severity"] == "error" else "warning"), axis=1
        )
        n_pass = int(rules["passed"].sum())
        st.metric("Rules passed", f"{n_pass} of {len(rules)}")
        st.dataframe(
            rules[["rule_name", "severity", "result", "failed_count", "detail"]],
            hide_index=True,
        )
    except Exception as error:
        st.info("The data quality rules could not be loaded.")
        st.caption(f"Details: {error}")

    st.subheader("Coverage per source")
    source_table = run_query("SELECT * FROM mart.v_source_coverage ORDER BY source_name")
    st.dataframe(source_table, hide_index=True)

    st.subheader("Recent pipeline runs")
    runs = run_query(
        """
        SELECT run_id, status, started_at_utc, duration_seconds,
               rows_fetched, rows_inserted, rows_updated, rows_rejected
        FROM ops.pipeline_runs
        ORDER BY started_at_utc DESC NULLS LAST
        LIMIT 10
        """
    )
    if runs.empty:
        st.info("No pipeline runs have been logged yet.")
    else:
        st.dataframe(runs, hide_index=True)


# =====================================================================
# Notes
# =====================================================================

st.divider()
st.caption(
    "USGS events are instrument observations. EONET events are curated reports built from news and "
    "satellite sources. Counts show how many events were reported, not how severe they were, and some "
    "EONET categories are rare. The same real earthquake can appear in both sources; we do not merge them."
)