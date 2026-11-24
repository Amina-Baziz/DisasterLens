"""
DisasterLens dashboard (Streamlit).

Shows natural hazard events from two sources:
  - USGS: earthquakes measured by instruments
  - NASA EONET: wildfires, storms, floods, volcanoes and more, curated from news and satellite sources

It only reads the mart views and the ops tables in PostgreSQL. It never writes anything.
All events are loaded once (about 40,000 rows) and every filter works on that table in
memory, so the page reacts instantly when a filter changes.

Run it on your own PC (the containers must be running):
    streamlit run dashboard/app.py
"""

import inspect
import math
import os
import re
from contextlib import closing
from datetime import timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg2
import pydeck as pdk
import streamlit as st


st.set_page_config(page_title="DisasterLens", page_icon="🌍", layout="wide")


# =====================================================================
# Look and feel
# =====================================================================

# One color per hazard type, the same on every map and chart.
# Softened on purpose, so the dark page stays calm.
HAZARD_COLORS = {
    "Earthquake":          "#D9686B",
    "Wildfire":            "#D9A05A",
    "Flood":               "#5AAFA6",
    "Severe storm":        "#6496C0",
    "Sea and lake ice":    "#9C8CC4",
    "Volcano":             "#B8628A",
    "Landslide":           "#A08B74",
    "Drought":             "#CDB679",
    "Temperature extreme": "#CF9C74",
    "Dust and haze":       "#B3A39F",
    "Snow":                "#C9D8DA",
    "Water color":         "#6BA9B8",
    "Other":               "#8E939A",
}

CSS = """
<style>
.block-container {padding-top: 1.6rem; max-width: 1450px;}
.hero {background: linear-gradient(115deg, #121a3a 0%, #3d2a6e 45%, #a8410f 100%);
       border-radius: 20px; padding: 24px 32px 16px; color: #fff; margin-bottom: 20px;
       box-shadow: 0 10px 30px rgba(0,0,0,.35);}
.hero h1 {font-size: 2.4rem; margin: 0; padding: 0; color: #fff; font-weight: 800; letter-spacing: -.5px;}
.hero p {margin: 4px 0 14px; opacity: .92; font-size: 1.05rem;}
.chip {display: inline-block; background: rgba(255,255,255,.14); border: 1px solid rgba(255,255,255,.25);
       border-radius: 999px; padding: 4px 12px; margin: 0 6px 8px 0; font-size: .85rem;}
.live {display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 7px;}
.pulse {animation: pulse 2s infinite;}
@keyframes pulse {0% {box-shadow: 0 0 0 0 rgba(109,181,146,.6);}
                  70% {box-shadow: 0 0 0 9px rgba(109,181,146,0);}
                  100% {box-shadow: 0 0 0 0 rgba(109,181,146,0);}}
.kpi {background: #161b26; border: 1px solid rgba(255,255,255,.07); border-top: 3px solid var(--accent);
      border-radius: 14px; padding: 14px 18px; min-height: 124px;}
.kpi-label {font-size: .76rem; text-transform: uppercase; letter-spacing: .07em; opacity: .7;}
.kpi-value {font-size: 1.95rem; font-weight: 750; margin-top: 4px; line-height: 1.15;}
.kpi-sub {font-size: .84rem; margin-top: 5px; opacity: .9;}
.up {color: #CC8A73;} .down {color: #6DB592;} .flat {opacity: .65;}
.insight {background: linear-gradient(180deg, rgba(255,255,255,.045), rgba(255,255,255,.01));
          border: 1px solid rgba(255,255,255,.07); border-left: 4px solid var(--accent);
          border-radius: 12px; padding: 14px 18px; min-height: 112px;}
.insight-title {font-size: .74rem; text-transform: uppercase; letter-spacing: .07em; opacity: .65; margin-bottom: 6px;}
.insight-text {font-size: 1rem; line-height: 1.45;}
.legend {margin: 0 0 8px;}
.legend span {margin-right: 16px; font-size: .88rem; white-space: nowrap;}
.legend i {display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px;}
.flow {display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 4px 0 18px;}
.flow div {background: #161b26; border: 1px solid rgba(255,255,255,.1); border-top: 3px solid var(--accent);
           border-radius: 10px; padding: 8px 14px; font-size: .86rem; opacity: .95;}
.flow b {display: block; font-size: .98rem;}
.flow span {opacity: .45; font-size: 1.2rem;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# Calendar colors: dark for a quiet day, soft orange for the busiest.
CALENDAR_RANGE = ["#1a1f2c", "#4b3566", "#9a4f68", "#d08a62"]


def html(text):
    """Show a piece of HTML (no indentation, so Markdown does not turn it into a code block)."""
    st.markdown(re.sub(r"\n\s*", "", text), unsafe_allow_html=True)


def color_of(hazard):
    return HAZARD_COLORS.get(hazard, HAZARD_COLORS["Other"])


def color_scale(names):
    """Altair color scale that uses our fixed hazard colors."""
    names = list(names)
    return alt.Scale(domain=names, range=[color_of(n) for n in names])


def legend(names):
    html('<div class="legend">' + "".join(
        f'<span><i style="background:{color_of(n)}"></i>{n}</span>' for n in names) + "</div>")


def show_chart(chart, stretch=True):
    """Draw an Altair chart, at full width or at its own size (newer Streamlit uses width=, older use_container_width=)."""
    if "width" in inspect.signature(st.altair_chart).parameters:
        st.altair_chart(chart, width="stretch" if stretch else "content")
    else:
        st.altair_chart(chart, use_container_width=stretch)


def kpi(column, label, value, sub, accent):
    with column:
        html(f'<div class="kpi" style="--accent:{accent}"><div class="kpi-label">{label}</div>'
             f'<div class="kpi-value">{value}</div><div class="kpi-sub">{sub}</div></div>')


# =====================================================================
# Database
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
    if Path("/.dockerenv").exists():
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


# USGS writes US states as short codes ("10 km NE of Ridgecrest, CA").
REGION_NAMES = {"CA": "California", "NV": "Nevada", "MX": "Mexico", "B.C., MX": "Mexico", "OR": "Oregon",
                "WA": "Washington", "UT": "Utah", "ID": "Idaho", "MT": "Montana", "WY": "Wyoming",
                "TX": "Texas", "OK": "Oklahoma", "NM": "New Mexico", "AZ": "Arizona"}


def region_of(title):
    """'25 km SW of Kablalan, Philippines' -> 'Philippines', 'Fiji region' -> 'Fiji'."""
    if not isinstance(title, str) or not title:
        return "Unknown"
    if ", " in title:
        region = title.rsplit(", ", 1)[1]
    else:
        region = re.sub(r"^.*\bof\s+(the\s+)?", "", title)
    region = re.sub(r"\s+region$", "", region).strip()
    return REGION_NAMES.get(region, region[:1].upper() + region[1:]) or "Unknown"


# Rough boxes (south, north, west, east) for the area of an event, checked in this order.
# NASA EONET titles often name no place ("Tropical Storm Melissa"), so its events get an area from their coordinates.
WORLD_AREAS = [
    ("Antarctica", -90, -60, -180, 180),
    ("Arctic", 66, 90, -180, 180),
    ("Central America and Caribbean", 7, 23, -118, -59),
    ("North America", 15, 72, -170, -50),
    ("South America", -56, 13, -82, -34),
    ("Europe", 35, 71, -25, 45),
    ("Middle East", 12, 42, 35, 63),
    ("Africa", -35, 37, -18, 52),
    ("Russia and Central Asia", 40, 78, 45, 180),
    ("South Asia", 5, 37, 60, 92),
    ("East Asia", 20, 55, 92, 146),
    ("Southeast Asia", -11, 20, 92, 141),
    ("Australia and Oceania", -50, -11, 110, 180),
    ("Pacific Ocean", -60, 66, 120, 180),
    ("Pacific Ocean", -60, 66, -180, -70),
    ("Atlantic Ocean", -60, 66, -70, 20),
    ("Indian Ocean", -60, 30, 20, 120),
]


def area_of(lat, lon):
    """The rough world area a point lies in, for example 'Southeast Asia' or 'Atlantic Ocean'."""
    if pd.isna(lat) or pd.isna(lon):
        return "No exact location"
    for name, south, north, west, east in WORLD_AREAS:
        if south <= lat <= north and west <= lon <= east:
            return name
    return "Other area"


# Shorter names for countries whose official name is long.
SHORT_NAMES = {"Russian Federation": "Russia", "Congo, The Democratic Republic of the": "DR Congo",
               "Syrian Arab Republic": "Syria", "Lao People's Democratic Republic": "Laos",
               "Moldova, Republic of": "Moldova", "Micronesia, Federated States of": "Micronesia",
               "Saint Helena, Ascension and Tristan da Cunha": "Saint Helena"}

try:   # offline lookup of the nearest town (packages reverse_geocoder and pycountry)
    import pycountry
    import reverse_geocoder
    HAVE_GEOCODER = True
except ImportError:
    HAVE_GEOCODER = False


def country_name(code):
    country = pycountry.countries.get(alpha_2=code)
    if country is None:
        return code
    name = getattr(country, "common_name", None) or country.name
    return SHORT_NAMES.get(name, name)


def km_between(lat1, lon1, lat2, lon2):
    """Distance in km between two points on the earth."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


# Other ways NASA titles name a country.
COUNTRY_ALIASES = {"USA": "United States", "U.S.": "United States", "Democratic Republic of the Congo": "DR Congo",
                   "DRC": "DR Congo", "Republic of the Congo": "Congo", "Ivory Coast": "Côte d'Ivoire",
                   "Turkey": "Türkiye", "Burma": "Myanmar", "Czech Republic": "Czechia", "UK": "United Kingdom"}


@st.cache_resource
def country_pattern():
    """One search pattern with every country name (longest first, so 'South Sudan' wins over 'Sudan')."""
    names = {**COUNTRY_ALIASES}
    for country in pycountry.countries:
        short = country_name(country.alpha_2)
        for name in {country.name, getattr(country, "common_name", None), short}:
            if name:
                names[name] = short
    ordered = sorted(names, key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(re.escape(n) for n in ordered) + r")\b", re.IGNORECASE), \
        {n.lower(): v for n, v in names.items()}


def country_in_title(title):
    """'Flooding - Nigeria' -> 'Nigeria'; None when no country is named."""
    if not HAVE_GEOCODER or not isinstance(title, str):
        return None
    pattern, lookup = country_pattern()
    found = pattern.search(title)
    return lookup.get(found.group(1).lower()) if found else None


def place_names(lats, lons):
    """
    The country of each point, from the nearest town in an offline list of world towns.
    On land the nearest town always gives the country. At sea or on the ice (a storm, an iceberg), the
    point keeps its ocean or polar area unless a town is within 300 km.
    """
    names = [area_of(lat, lon) for lat, lon in zip(lats, lons)]
    located = [i for i, (lat, lon) in enumerate(zip(lats, lons)) if pd.notna(lat) and pd.notna(lon)]
    if not HAVE_GEOCODER or not located:
        return names
    towns = reverse_geocoder.search([(float(lats[i]), float(lons[i])) for i in located], mode=1)
    for i, town in zip(located, towns):
        at_sea = names[i] in ("Pacific Ocean", "Atlantic Ocean", "Indian Ocean", "Antarctica", "Arctic", "Other area")
        if not at_sea or km_between(lats[i], lons[i], float(town["lat"]), float(town["lon"])) <= 300:
            names[i] = country_name(town["cc"])
    return names


@st.cache_data(ttl=300)
def load_events():
    events = run_query(
        """
        SELECT event_key, source_name, display_name, title, event_time_utc,
               latitude, longitude, magnitude, depth_km, event_status, has_valid_point
        FROM mart.v_events
        """
    )
    for column in ("latitude", "longitude", "magnitude", "depth_km"):
        events[column] = pd.to_numeric(events[column], errors="coerce")
    events["event_time_utc"] = pd.to_datetime(events["event_time_utc"])
    events["day"] = events["event_time_utc"].dt.normalize()
    # Earthquakes: the country or state named in the USGS title.
    # NASA EONET titles often name no place, so its events get the country of their coordinates.
    events["region"] = events["title"].map(region_of)
    eonet = events["source_name"] == "eonet"
    events.loc[eonet, "region"] = place_names(events.loc[eonet, "latitude"].tolist(),
                                              events.loc[eonet, "longitude"].tolist())
    # Events reported only as an area (most floods) have no point: use a country named in the title.
    no_point = eonet & (events["region"] == "No exact location")
    from_name = events.loc[no_point, "title"].map(country_in_title)
    events.loc[no_point, "region"] = from_name.fillna("No exact location")
    return events


try:
    events = load_events()
except psycopg2.errors.UndefinedTable:
    st.error("The view mart.v_events is missing. Your database was created with an older schema.")
    st.markdown("Run this once in the project folder (it only adds what is missing), then reload the page:")
    st.code("docker compose exec -T postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> < sql/schema.sql", language="bash")
    st.stop()
except Exception as error:
    st.error("Could not read from the database. Check that the containers are running (docker compose ps).")
    st.caption(f"Details: {error}")
    st.stop()

if events.empty:
    st.info("The warehouse has no events yet. Run the pipeline in Airflow, then reload this page.")
    st.stop()


# =====================================================================
# Sidebar filters
# =====================================================================

first_day, last_day = events["day"].min().date(), events["day"].max().date()

st.sidebar.header("Filters")
PERIODS = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90, "Whole history": None, "Custom dates": "custom"}
choice = st.sidebar.selectbox("Period", list(PERIODS), index=1)
if PERIODS[choice] == "custom":
    picked = st.sidebar.date_input("Dates", value=(first_day, last_day), min_value=first_day, max_value=last_day)
    if len(picked) != 2:
        st.info("Pick an end date.")
        st.stop()
    start, end = picked
elif PERIODS[choice] is None:
    start, end = first_day, last_day
else:
    start, end = max(first_day, last_day - timedelta(days=PERIODS[choice] - 1)), last_day


SOURCES = {"Both sources": ["usgs", "eonet"], "USGS (earthquakes)": ["usgs"], "NASA EONET (other hazards)": ["eonet"]}
sources = SOURCES[st.sidebar.radio("Source", list(SOURCES))]


# Hazard types, most common first. They form the tab line under the header.
all_hazards = events["display_name"].value_counts().index.tolist()
PLURALS = {"Earthquake": "Earthquakes", "Wildfire": "Wildfires", "Flood": "Floods", "Severe storm": "Severe storms",
           "Volcano": "Volcanoes", "Landslide": "Landslides", "Drought": "Droughts",
           "Temperature extreme": "Temperature extremes"}
NAV = {"All events": None, **{PLURALS.get(h, h): h for h in all_hazards}}


def apply_filters(frame, lo, hi, hazard):
    """Keep the rows inside the date range, the sidebar filters and the chosen hazard tab."""
    keep = ((frame["day"].dt.date >= lo) & (frame["day"].dt.date <= hi) & frame["source_name"].isin(sources))
    if hazard:
        keep &= frame["display_name"] == hazard
    quake = (frame["display_name"] == "Earthquake") & frame["magnitude"].notna()
    return frame[keep & (~quake | (frame["magnitude"] >= min_magnitude))]


days = (end - start).days + 1
has_previous = start - timedelta(days=days) >= first_day
# Time step of the trend charts, chosen from the length of the period.
grain = "Day" if days <= 45 else "Week" if days <= 200 else "Month"
period_text = f"{start:%d %b %Y} – {end:%d %b %Y}"


# =====================================================================
# Header
# =====================================================================

latest_seen = run_query("SELECT max(last_seen_utc) AS last_seen FROM mart.v_source_coverage").loc[0, "last_seen"]
if pd.isna(latest_seen):
    refresh = '<span class="live" style="background:#9AA0A6"></span>No data loaded yet'
else:
    latest_seen = pd.Timestamp(latest_seen)
    fresh = latest_seen >= pd.Timestamp.now(tz="UTC").tz_localize(None) - pd.Timedelta(hours=3)
    refresh = ('<span class="live pulse" style="background:#6DB592"></span>' if fresh
               else '<span class="live" style="background:#CC8A73"></span>')
    refresh += f"Last refresh {latest_seen:%d %b %Y, %H:%M} UTC" + ("" if fresh else " (check Airflow)")
html(f"""
<div class="hero">
  <h1>🌍 DisasterLens</h1>
  <p>Earthquakes, wildfires, floods, storms and volcanoes around the world, refreshed every hour.</p>
  <span class="chip">{refresh}</span>
  <span class="chip">{period_text}</span>
  <span class="chip">Sources: USGS and NASA EONET</span>
</div>
""")


# =====================================================================
# Pipeline health: its own tab, because it is the same for every hazard type
# =====================================================================

def quality_sql():
    """The data quality statement from sql/queries.sql (everything after the 'Part 2' heading)."""
    text = (Path(__file__).resolve().parent.parent / "sql" / "queries.sql").read_text()
    return text[text.index("SELECT", text.index("Part 2: data quality rules")):]


def pipeline_health():
    """How the data gets here, recent runs, the quality rules checked live and the data per source."""
    st.subheader("How the data gets here")
    html("""
    <div class="flow">
      <div style="--accent:#4EA8DE"><b>USGS + NASA EONET</b>public APIs</div><span>→</span>
      <div style="--accent:#8577C8"><b>Airflow</b>runs every hour</div><span>→</span>
      <div style="--accent:#B388EB"><b>MinIO</b>raw files, unchanged</div><span>→</span>
      <div style="--accent:#FFA62B"><b>Spark</b>clean, map, deduplicate</div><span>→</span>
      <div style="--accent:#2EC4B6"><b>PostgreSQL</b>star schema and marts</div><span>→</span>
      <div style="--accent:#FF5A5F"><b>Dashboard</b>Streamlit</div>
    </div>
    """)

    runs = run_query(
        """
        SELECT run_id, status, started_at_utc, duration_seconds,
               rows_fetched, rows_inserted, rows_updated, rows_rejected, error_message
        FROM ops.pipeline_runs
        ORDER BY started_at_utc DESC NULLS LAST
        LIMIT 48
        """
    )
    if runs.empty:
        st.info("No pipeline runs have been logged yet.")
    else:
        runs["started_at_utc"] = pd.to_datetime(runs["started_at_utc"])
        runs["error_message"] = runs["error_message"].fillna("")
        ok = runs[runs["status"] == "success"]
        p1, p2, p3, p4 = st.columns(4)
        kpi(p1, "Success rate", f"{len(ok) / len(runs):.0%}", f'<span class="flat">last {len(runs)} runs</span>', "#6DB592")
        kpi(p2, "Failed runs", f"{len(runs) - len(ok)}", '<span class="flat">each task retried twice first</span>', "#D9686B")
        kpi(p3, "Typical run time", f"{runs['duration_seconds'].median():.0f} s", '<span class="flat">median</span>', "#6496C0")
        kpi(p4, "Last success", "none" if ok.empty else f"{ok.iloc[0]['started_at_utc']:%H:%M}",
            "" if ok.empty else f'<span class="flat">{ok.iloc[0]["started_at_utc"]:%d %b %Y} UTC</span>', "#8577C8")
        st.write("")

        st.subheader("Recent runs")
        timeline = alt.Chart(runs).mark_bar(cornerRadiusEnd=3, width=7).encode(
            x=alt.X("started_at_utc:T", title=None),
            y=alt.Y("duration_seconds:Q", title="Run time (seconds)"),
            color=alt.Color("status:N", title=None, legend=alt.Legend(orient="bottom"),
                            scale=alt.Scale(domain=["success", "failed"], range=["#6DB592", "#D9686B"])),
            tooltip=["run_id", "status", alt.Tooltip("duration_seconds:Q", title="Seconds", format=".0f"),
                     "rows_inserted", "rows_updated", "rows_rejected", "error_message"],
        )
        show_chart(timeline.properties(height=200))
        st.caption("One bar per hourly run. Hover a bar to see how many events it added or updated, or why it failed.")

    st.subheader("Data quality rules")
    st.caption("Checked live on the warehouse. A failed error rule means wrong data and stops the pipeline; "
               "a warning is worth a look.")
    try:
        rules = run_query(quality_sql())
        rules["result"] = ["pass" if passed else ("FAIL" if severity == "error" else "warning")
                           for passed, severity in zip(rules["passed"], rules["severity"])]
        st.dataframe(rules[["result", "rule_name", "severity", "failed_count", "detail"]], hide_index=True,
                     column_config={"result": "Result", "rule_name": "Rule", "severity": "Severity",
                                    "failed_count": "Failing", "detail": "What it checks"})
    except Exception as error:
        st.info("The data quality rules could not be loaded.")
        st.caption(f"Details: {error}")

    st.subheader("Data per source")
    coverage = run_query("SELECT * FROM mart.v_source_coverage ORDER BY source_name")
    st.dataframe(coverage, hide_index=True, column_config={
        "source_name": "Source", "total_events": "Events", "events_with_location": "With a location",
        "earliest_event_utc": st.column_config.DatetimeColumn("Oldest event", format="D MMM YYYY"),
        "latest_event_utc": st.column_config.DatetimeColumn("Newest event", format="D MMM YYYY, HH:mm"),
        "last_seen_utc": st.column_config.DatetimeColumn("Last loaded", format="D MMM YYYY, HH:mm"),
    })


# =====================================================================
# Hazard tab line: a radio button styled like tabs, so the whole page follows the choice
# (CSS for the old and the new Streamlit radio layout)
# =====================================================================

html("""
<style>
.st-key-hazard_nav div[role="radiogroup"] {gap: 4px 26px; flex-wrap: wrap; width: 100%;
    border-bottom: 1px solid rgba(255,255,255,.12); margin-bottom: 6px;}
.st-key-hazard_nav label {margin: 0 !important; padding: 8px 0 10px; cursor: pointer;
    border-bottom: 2px solid transparent; margin-bottom: -1px !important;}
.st-key-hazard_nav label p {font-size: 1.05rem !important;}
.st-key-hazard_nav label[data-baseweb="radio"] > div:first-child {display: none;}
.st-key-hazard_nav label[data-baseweb="radio"]:has(input:checked) {border-bottom-color: #C8642A;}
.st-key-hazard_nav label[data-baseweb="radio"]:has(input:checked) p {color: #C8642A;}
.st-key-hazard_nav [data-testid="stRadioOption"] > div > div:first-child {display: none;}
.st-key-hazard_nav [data-testid="stRadioOption"][data-selected="true"] {border-bottom-color: #C8642A;}
.st-key-hazard_nav [data-testid="stRadioOption"][data-selected="true"] p {color: #C8642A;}
.st-key-hazard_nav label:hover p {color: #C8642A;}
</style>
""")
nav_choice = st.radio("Hazard", list(NAV) + ["Pipeline health"], horizontal=True, key="hazard_nav",
                      label_visibility="collapsed")
if nav_choice == "Pipeline health":
    pipeline_health()
    st.stop()
hazard = NAV[nav_choice]

# Only earthquakes have a magnitude (NASA does not measure wildfires, floods or storms that way),
# so the slider only shows on the tabs that contain earthquakes.
if hazard in (None, "Earthquake"):
    min_magnitude = st.sidebar.slider(
        "Minimum earthquake magnitude", 0.0, 8.0, 2.5, 0.5,
        help="Only earthquakes have a magnitude, so this filter does not change wildfires, floods or storms. "
             "It starts at 2.5 because the one-year history only contains earthquakes of 2.5 or more; smaller "
             "ones only arrive with the hourly feed and would make the latest days look like a sudden spike.",
    )
else:
    min_magnitude = 2.5
label = nav_choice if hazard else "events"

view = apply_filters(events, start, end, hazard)
previous = apply_filters(events, start - timedelta(days=days), start - timedelta(days=1), hazard)

if view.empty:
    st.info(f"No {label.lower()} match these filters. Try a longer period or the other source in the sidebar.")
    st.stop()

quakes = view[(view["display_name"] == "Earthquake") & view["magnitude"].notna()]
others = view[view["display_name"] != "Earthquake"]
accent = color_of(hazard) if hazard else "#8577C8"


# =====================================================================
# Key numbers, compared with the period just before
# =====================================================================

def change(now, before):
    """'▲ 12% vs previous 30 days', or a short note when there is nothing to compare."""
    if not has_previous or before == 0:
        return '<span class="flat">no earlier period to compare</span>'
    pct = (now - before) / before * 100
    if abs(pct) < 0.5:
        return f'<span class="flat">same as the previous {days} days</span>'
    arrow, cls = ("▲", "up") if pct > 0 else ("▼", "down")
    return f'<span class="{cls}">{arrow} {abs(pct):.0f}%</span> <span class="flat">vs previous {days} days</span>'


def strongest_card(column):
    if quakes.empty:
        kpi(column, "Strongest earthquake", "–", '<span class="flat">no earthquake in this view</span>', "#B8628A")
    else:
        top = quakes.loc[quakes["magnitude"].idxmax()]
        kpi(column, "Strongest earthquake", f"M {top['magnitude']:.1f}",
            f"{top['region']} · {top['event_time_utc']:%d %b %Y}", "#B8628A")


def busiest_card(column, frame):
    if days >= 90:
        per = frame["event_time_utc"].dt.to_period("M").value_counts()
        kpi(column, "Busiest month", per.index[0].strftime("%B %Y"), f'<span class="flat">{per.iloc[0]:,} events</span>', "#8577C8")
    else:
        per = frame["day"].value_counts()
        kpi(column, "Busiest day", f"{per.index[0]:%d %b}", f'<span class="flat">{per.iloc[0]:,} events</span>', "#8577C8")


k1, k2, k3, k4 = st.columns(4)
kpi(k1, f"Earthquakes M{min_magnitude:g}+" if hazard == "Earthquake" else ("All events" if not hazard else nav_choice),
    f"{len(view):,}", change(len(view), len(previous)), accent)
if not hazard:
    n_quakes, n_before = int((view["display_name"] == "Earthquake").sum()), int((previous["display_name"] == "Earthquake").sum())
    kpi(k2, f"Earthquakes M{min_magnitude:g}+", f"{n_quakes:,}", change(n_quakes, n_before), color_of("Earthquake"))
    strongest_card(k3)
    kpi(k4, "Still open (EONET)", f"{int((others['event_status'] == 'open').sum()):,}",
        f'<span class="flat">{int((others["event_status"] == "closed").sum()):,} already closed</span>', "#5AAFA6")
elif hazard == "Earthquake" and not quakes.empty:
    strongest_card(k2)
    kpi(k3, "Average magnitude", f"{quakes['magnitude'].mean():.2f}",
        f'<span class="flat">{int((quakes["magnitude"] >= 6).sum()):,} of magnitude 6 or more</span>', "#D9A05A")
    regions = quakes["region"].value_counts()
    kpi(k4, "Most active region", regions.index[0], f'<span class="flat">{regions.iloc[0]:,} earthquakes</span>', "#6496C0")
else:
    kpi(k2, "Still open", f"{int((view['event_status'] == 'open').sum()):,}",
        '<span class="flat">NASA still reports them as active</span>', "#CC8A73")
    kpi(k3, "Closed", f"{int((view['event_status'] == 'closed').sum()):,}", '<span class="flat">over</span>', "#3F5878")
    busiest_card(k4, view)
st.write("")


# =====================================================================
# Findings written from the data in view
# =====================================================================

def findings(frame):
    """Three short findings, each computed live from the filtered events: (title, text, color)."""
    found = []
    counts = frame["display_name"].value_counts()

    if len(counts) > 1:
        text = (f"<b>{PLURALS.get(counts.index[0], counts.index[0])}</b> are <b>{counts.iloc[0] / len(frame):.0%}</b> "
                f"of all events, <b>{PLURALS.get(counts.index[1], counts.index[1])}</b> another "
                f"<b>{counts.iloc[1] / len(frame):.0%}</b>.")
        found.append(("Biggest share", text, color_of(counts.index[0])))
    else:
        # Place names come from the titles. Storm and iceberg titles are names, not places,
        # so the card only shows when one place really repeats.
        places = frame["region"].value_counts()
        if places.iloc[0] >= 3 and places.iloc[0] / len(frame) >= 0.05:
            text = (f"<b>{places.index[0]}</b> had <b>{places.iloc[0]:,}</b> of them "
                    f"({places.iloc[0] / len(frame):.0%})")
            if len(places) > 1 and places.iloc[1] >= 3:
                text += f", followed by <b>{places.index[1]}</b> ({places.iloc[1]:,})"
            found.append(("Where most happened", text + ".", accent))

    per_day = frame.groupby("day").size()
    if len(per_day) > 3:
        peak_day, peak = per_day.idxmax(), int(per_day.max())
        place = frame[frame["day"] == peak_day]["region"].value_counts()
        text = (f"<b>{peak_day:%d %b %Y}</b> had <b>{peak:,}</b>, "
                f"{peak / per_day.mean():.1f}× the daily average.")
        if not place.empty and place.iloc[0] / peak >= 0.4:
            text += f" {place.iloc[0] / peak:.0%} of them in <b>{place.index[0]}</b>."
        found.append(("Busiest day", text, "#8577C8"))

    seasonal = None
    if days >= 120:   # seasonality needs several full months
        monthly = frame.assign(month=frame["event_time_utc"].dt.to_period("M"))
        months = sorted(monthly["month"].unique())[1:-1]   # skip the incomplete first and last month
        for name, rows in monthly[monthly["month"].isin(months)].groupby("display_name"):
            per_month = rows.groupby("month").size().reindex(months, fill_value=0)
            if len(rows) >= 50 and len(months) >= 4 and per_month.min() > 0:
                ratio = per_month.max() / per_month.min()
                if ratio >= 1.8 and (seasonal is None or ratio > seasonal[0]):
                    seasonal = (ratio, name, per_month)
    if seasonal:
        ratio, name, per_month = seasonal
        found.append(("Seasonal pattern",
                      f"<b>{PLURALS.get(name, name)}</b> peak in <b>{per_month.idxmax().strftime('%B')}</b> "
                      f"({per_month.max():,}), <b>{ratio:.1f}×</b> the quietest month "
                      f"({per_month.idxmin().strftime('%B')}).", color_of(name)))
    elif hazard == "Earthquake" and quakes["depth_km"].notna().any():
        shallow = (quakes["depth_km"] < 70).mean()
        found.append(("Depth", f"<b>{shallow:.0%}</b> were shallow (under 70 km deep), the kind people feel most.",
                      "#D9A05A"))
    elif not hazard and not quakes.empty:
        regions = quakes["region"].value_counts()
        found.append(("Most active region",
                      f"<b>{regions.index[0]}</b> recorded <b>{regions.iloc[0]:,}</b> earthquakes, "
                      f"{regions.iloc[0] / len(quakes):.0%} of all.", color_of("Earthquake")))
    if hazard and hazard != "Earthquake" and len(found) < 3 and view["event_status"].notna().any():
        open_share = (view["event_status"] == "open").mean()
        found.append(("Still active", f"<b>{open_share:.0%}</b> of these {label.lower()} are still open.", "#CC8A73"))
    return found


cards = findings(view)
if cards:
    st.markdown("#### What the data says")
for column, (title, text, color) in zip(st.columns(len(cards)), cards):
    with column:
        html(f'<div class="insight" style="--accent:{color}"><div class="insight-title">{title}</div>'
             f'<div class="insight-text">{text}</div></div>')
st.write("")


# =====================================================================
# Map and charts
# =====================================================================

def draw_map(frame, size_by_magnitude=False, height=480):
    """Events as dots on a world map, colored by hazard type. Hover a dot for details."""
    frame = frame[frame["has_valid_point"] & frame["latitude"].notna()]
    if frame.empty:
        st.info("No events with a location match these filters.")
        return
    magnitude = frame["magnitude"]
    dots = pd.DataFrame({
        "latitude": frame["latitude"],
        "longitude": frame["longitude"],
        "color": frame["display_name"].map(lambda h: [int(color_of(h)[i:i + 2], 16) for i in (1, 3, 5)]),
        "radius": (magnitude.fillna(3).clip(lower=1) ** 3 * 500) if size_by_magnitude else 32000,
        "title": frame["title"].fillna("(no title)"),
        "hazard": frame["display_name"],
        "place": frame["region"].fillna(""),
        "coords": [f"{abs(lat):.1f}°{'N' if lat >= 0 else 'S'}, {abs(lon):.1f}°{'E' if lon >= 0 else 'W'}"
                   for lat, lon in zip(frame["latitude"], frame["longitude"])],
        "when": frame["event_time_utc"].dt.strftime("%d %b %Y, %H:%M UTC"),
        "detail": magnitude.map(lambda m: "" if pd.isna(m) else f"Magnitude {m:.1f}"),
    })
    layer = pdk.Layer(
        "ScatterplotLayer", data=dots, get_position="[longitude, latitude]", get_fill_color="color",
        get_radius="radius", radius_min_pixels=2.5, radius_max_pixels=26, opacity=0.65,
        pickable=True, auto_highlight=True,
    )
    tooltip = {"html": "<b>{title}</b><br/>{hazard}<br/>{place} · {coords}<br/>{when}<br/>{detail}",
               "style": {"backgroundColor": "#1f2433", "color": "white", "fontSize": "13px"}}
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=pdk.ViewState(latitude=20, longitude=10, zoom=1),
                             map_style="dark", tooltip=tooltip), height=height)


def trend_chart(frame, height=320):
    """Stacked area of events per day, week or month and hazard type, with the busiest step marked."""
    when = frame["event_time_utc"].dt.to_period({"Day": "D", "Week": "W", "Month": "M"}[grain]).dt.start_time
    counts = frame.assign(when=when).groupby(["when", "display_name"]).size().unstack(fill_value=0)
    # Every hazard needs a value at every step of the period, 0 when nothing happened,
    # or the stacked layers break and quiet days disappear from the line.
    freq = {"Day": "D", "Week": "W", "Month": "M"}[grain]
    steps = pd.period_range(pd.Timestamp(start), pd.Timestamp(end), freq=freq).start_time
    counts = counts.reindex(steps.union(counts.index), fill_value=0)
    counts.index.name = "when"
    data = counts.stack().rename("events").reset_index()
    names = [h for h in all_hazards if h in set(data["display_name"])]
    data["order"] = data["display_name"].map(names.index)
    totals = data.groupby("when")["events"].sum().reset_index()
    peak = totals.loc[[totals["events"].idxmax()]].copy()
    peak_format = {"Day": "%d %b %Y", "Week": "week of %d %b", "Month": "%B %Y"}[grain]
    peak["label"] = f"Peak: {peak['when'].iloc[0].strftime(peak_format)} · {peak['events'].iloc[0]:,}"
    x = alt.X("when:T", title=None)
    area = alt.Chart(data).mark_area(interpolate="monotone", opacity=0.85, line=True).encode(
        x=x, y=alt.Y("events:Q", title=f"Events per {grain.lower()}", stack=True),
        color=alt.Color("display_name:N", title=None, scale=color_scale(names), sort=names,
                        legend=alt.Legend(orient="bottom") if len(names) > 1 else None),
        order=alt.Order("order:Q"),
        tooltip=[alt.Tooltip("when:T", title=grain), alt.Tooltip("display_name:N", title="Hazard"),
                 alt.Tooltip("events:Q", title="Events", format=",")],
    )
    marker = alt.Chart(peak).mark_point(filled=True, size=80, color="white").encode(x=x, y="events:Q")
    text = alt.Chart(peak).mark_text(dy=-14, color="white", fontWeight="bold").encode(x=x, y="events:Q", text="label:N")
    show_chart((area + marker + text).properties(height=height))


def hazard_bars(frame, height=320):
    """Events per hazard type, with the count and the share written next to each bar."""
    data = frame["display_name"].value_counts().reset_index()
    data.columns = ["hazard", "events"]
    data["label"] = [f"{n:,} · {n / data['events'].sum():.0%}" for n in data["events"]]
    names = data["hazard"].tolist()
    bars = alt.Chart(data).mark_bar(cornerRadiusEnd=4).encode(
        x=alt.X("events:Q", title="Events", scale=alt.Scale(domainMax=float(data["events"].max()) * 1.35)),
        y=alt.Y("hazard:N", sort=names, title=None),
        color=alt.Color("hazard:N", scale=color_scale(names), legend=None),
        tooltip=[alt.Tooltip("hazard:N", title="Hazard"), alt.Tooltip("events:Q", format=",")],
    )
    labels = bars.mark_text(align="left", dx=5, color="#e6e9ef").encode(text="label:N")
    show_chart((bars + labels).properties(height=height))


def region_bars(frame, unit, height=370):
    """The 10 places with the most events, in shades of blue (brighter = more)."""
    places = frame["region"].dropna()
    data = places[places != "No exact location"].value_counts().head(10).reset_index()
    data.columns = ["region", "events"]
    if data.empty:
        st.info("None of these events has a location that can be placed in a country.")
        return
    x = alt.X("events:Q", title=unit, scale=alt.Scale(domainMax=float(data["events"].max()) * 1.12))
    y = alt.Y("region:N", sort="-x", title=None, axis=alt.Axis(labelLimit=220))
    bars = alt.Chart(data).mark_bar(cornerRadiusEnd=4).encode(
        x=x, y=y,
        color=alt.Color("events:Q", legend=None, scale=alt.Scale(range=["#2A4A6E", "#7EA6CF"])),
        tooltip=[alt.Tooltip("region:N", title="Where"), alt.Tooltip("events:Q", title=unit, format=",")],
    )
    labels = alt.Chart(data).mark_text(align="left", dx=5, color="#e6e9ef").encode(
        x=x, y=y, text=alt.Text("events:Q", format=","))
    show_chart((bars + labels).properties(height=height))


def day_calendar(color_range):
    """
    One square per day over the whole history, one column per week, so the calendar always has the
    same size. Days outside the chosen period are grey.
    """
    history = apply_filters(events, first_day, last_day, hazard)
    daily = (history.groupby("day").size()
             .reindex(pd.date_range(pd.Timestamp(first_day), pd.Timestamp(last_day)), fill_value=0)
             .rename_axis("day").reset_index(name="events"))
    daily["week"] = daily["day"] - pd.to_timedelta(daily["day"].dt.dayofweek, unit="D")
    daily["weekday"] = daily["day"].dt.strftime("%a")
    in_period = (daily["day"] >= pd.Timestamp(start)) & (daily["day"] <= pd.Timestamp(end))
    x = alt.X("yearmonthdate(week):O", title=None,
              axis=alt.Axis(labelExpr="date(datum.value) <= 7 ? timeFormat(datum.value, '%b') : ''",
                            labelAngle=0, ticks=False, domain=False))
    y = alt.Y("weekday:N", sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], title=None,
              axis=alt.Axis(labelOverlap=False, labelFontSize=11))   # show all seven day names
    tooltip = [alt.Tooltip("day:T", title="Day", format="%a %d %b %Y"), alt.Tooltip("events:Q", title="Events", format=",")]
    # Grey squares for the whole history, the chosen period colored on top (on its own scale).
    year = alt.Chart(daily).mark_rect(cornerRadius=2, stroke="#0e1117", strokeWidth=1, color="#222836").encode(
        x=x, y=y, tooltip=tooltip)
    period = alt.Chart(daily[in_period]).mark_rect(cornerRadius=2, stroke="#0e1117", strokeWidth=1).encode(
        x=x, y=y, tooltip=tooltip,
        color=alt.Color("events:Q", title="Per day", scale=alt.Scale(range=color_range)))
    calendar = year + period
    show_chart(calendar.properties(width=alt.Step(17), height=alt.Step(17)), stretch=False)   # real squares


# =====================================================================
# Views under the hazard line
# =====================================================================

tab_charts, tab_events = st.tabs(["Charts", "Event list"])

with tab_charts:
    if not hazard:
        # ------------------------------------------------ All events: what, where and when?
        st.subheader("Where events happened")
        legend([h for h in all_hazards if h in set(view["display_name"])])
        draw_map(view)
        st.caption(f"{int(view['has_valid_point'].sum()):,} of {len(view):,} events have a single location and are "
                   "drawn. Hover a dot for details, scroll to zoom.")
        left, right = st.columns([2, 3])
        with left:
            st.subheader("What happened")
            hazard_bars(view)
        with right:
            st.subheader("Where it happened")
            region_bars(view, "Events", height=320)
            st.caption("Earthquakes use the place in the USGS title; other hazards the country of their coordinates.")
        st.subheader("When it happened")
        day_calendar(CALENDAR_RANGE)
        st.caption("One square per day over the whole history, one column per week. Brighter squares are busier "
                   "days; days outside the chosen period are grey. Hover a square for the exact number.")

    elif hazard == "Earthquake":
        # ------------------------------------------------ Earthquakes: where, how strong, how often?
        st.subheader("Earthquake map")
        draw_map(quakes, size_by_magnitude=True)
        st.caption("Bigger circles are stronger earthquakes. They line up along the edges of tectonic plates, "
                   "such as the Pacific Ring of Fire.")
        left, right = st.columns(2)
        with left:
            st.subheader("Strongest earthquakes")
            strongest = quakes.nlargest(10, "magnitude")[["magnitude", "title", "depth_km", "event_time_utc"]]
            st.dataframe(strongest, hide_index=True, height=388, column_config={
                "magnitude": st.column_config.NumberColumn("Magnitude", format="%.1f"),
                "title": "Where",
                "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.0f"),
                "event_time_utc": st.column_config.DatetimeColumn("When (UTC)", format="D MMM YYYY, HH:mm"),
            })
        with right:
            st.subheader("Most active regions")
            region_bars(quakes, "Earthquakes")
        st.subheader("Earthquakes per day")
        day_calendar(["#1a1f2c", color_of("Earthquake")])
        st.caption("One square per day over the whole history; days outside the chosen period are grey. "
                   "A bright square is a busy day, usually an aftershock sequence or a swarm in one place.")

    else:
        # ------------------------------------------------ One EONET hazard: where, when, what is active now?
        st.subheader(f"Where {nav_choice.lower()} happened")
        draw_map(view)
        without_point = int((~view["has_valid_point"]).sum())
        st.caption("Hover a dot for details, scroll to zoom."
                   + (f" {without_point:,} events are reported as an area instead of a point and are not drawn."
                      if without_point else ""))
        left, right = st.columns(2)
        with left:
            st.subheader("When they happened")
            trend_chart(view, height=300)
        with right:
            st.subheader("Most active countries" if HAVE_GEOCODER else "Most active areas")
            region_bars(view, nav_choice, height=300)
            st.caption("The country comes from each event's coordinates (the nearest town). Events reported only as "
                       "an area use the country named in their title. Events far out at sea or on the ice show their "
                       "ocean or polar area instead." if HAVE_GEOCODER else
                       "Install the packages in dashboard/requirements.txt to see countries instead of areas.")
        st.subheader("Active right now")
        still_open = view[view["event_status"] == "open"].nlargest(15, "event_time_utc")
        if still_open.empty:
            st.caption(f"None of these {nav_choice.lower()} are still open.")
        else:
            st.dataframe(still_open[["event_time_utc", "title"]], hide_index=True, column_config={
                "event_time_utc": st.column_config.DatetimeColumn("Last update (UTC)", format="D MMM YYYY"),
                "title": "Event",
            })


# ---------------------------------------------------------------- Event list: find a specific event
with tab_events:
    search = st.text_input("Search by place or name", placeholder="for example: Alaska, Japan, fire")
    shown = view
    if search:
        text = view["title"].fillna("") + " " + view["region"]
        shown = view[text.str.contains(search, case=False, regex=False)]
    shown = shown.sort_values("event_time_utc", ascending=False)
    columns = ["event_time_utc", "display_name", "title", "magnitude", "depth_km", "event_status", "source_name"]
    shown = shown.assign(event_status=shown["event_status"].fillna(""))
    st.caption(f"{len(shown):,} {label.lower()} match. The newest 1,000 are listed; the download contains all of them.")
    st.dataframe(shown[columns].head(1000), hide_index=True, height=480, column_config={
        "event_time_utc": st.column_config.DatetimeColumn("When (UTC)", format="D MMM YYYY, HH:mm"),
        "display_name": "Hazard", "title": "Event",
        "magnitude": st.column_config.NumberColumn("Magnitude", format="%.1f"),
        "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.0f"),
        "event_status": "Status", "source_name": "Source",
    })
    st.download_button("Download as CSV", shown[columns].to_csv(index=False).encode("utf-8"),
                       file_name="disasterlens_events.csv", mime="text/csv")


st.divider()
st.caption(
    "USGS events are instrument observations. EONET events are curated reports built from news and satellite "
    "sources. Counts show how many events were reported, not how severe they were. The same real earthquake "
    "can appear in both sources; they are not merged."
)
