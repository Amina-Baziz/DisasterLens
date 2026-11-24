# Architecture

This document describes how DisasterLens is built and why. The step by step view of one run and the setup instructions are in `README.md`, and the failure tests are in `docs/runbook.md`.

## Overview

```mermaid
flowchart LR
    A[USGS API] --> C[Airflow]
    B[NASA EONET API] --> C
    C -->|raw JSON| D[(MinIO data lake)]
    D --> E[PySpark transform]
    E -->|staging.stg_event| F[(PostgreSQL)]
    F -->|idempotent merge| G[dw: star schema]
    G --> H[mart: views]
    H --> I[Streamlit dashboard]
    C -->|run log and quality results| J[ops schema]
    J --> I
```

The pipeline is a batch pipeline. Airflow starts it every hour, and a second DAG loads history by hand. Each layer keeps its own copy of the data, so a problem can be traced back one step at a time.

## Layers

| Layer | Technology | What it holds | Why |
| --- | --- | --- | --- |
| Orchestration | Airflow 2.10 (LocalExecutor) | Two DAGs: `disasterlens_pipeline` (hourly) and `disasterlens_backfill` (manual) | Schedules the tasks, retries failures twice, and shows every run in a UI |
| Data lake | MinIO (S3 compatible) | The raw API responses, unchanged, with a small metadata file per response | Keeps the original data so any run can be repeated or checked |
| Transform | PySpark 3.5 (local mode) | Cleaning, mapping, rejecting bad rows, removing duplicates | Handles both sources with one set of rules and counts every rejected row |
| Warehouse | PostgreSQL 16 | Four schemas: `staging`, `dw`, `mart`, `ops` | One database that holds the tables the dashboard needs and the run log |
| Dashboard | Streamlit, Altair (charts), pydeck (maps) | One tab per hazard type plus Pipeline health, over the `mart` and `ops` schemas | Shows the data, live findings and the health of the pipeline. Written in Python like the rest of the project, so no separate BI server is needed |

## Data lake layout

```
raw/source=<usgs|eonet>/ingest_date=YYYY-MM-DD/run_id=<run id>/response.json
raw/source=<usgs|eonet>/ingest_date=YYYY-MM-DD/run_id=<run id>/_meta.json
```

The folder names carry the source, the day and the run, so one run can be found, downloaded and processed on its own. `_meta.json` stores the exact URL that was called and when.

## Warehouse

| Schema | Tables and views | Purpose |
| --- | --- | --- |
| `staging` | `stg_event` | Clean rows written by Spark, rebuilt on every run |
| `dw` | `fact_event`, `dim_date`, `dim_source`, `dim_hazard_type` | Star schema. One fact row per source event, holding its latest known version |
| `mart` | `mv_daily_hazard_summary` (materialized view), `v_events`, `v_event_map`, `v_latest_events`, `v_source_coverage` | What the dashboard reads |
| `ops` | `pipeline_runs`, `dq_results` | One row per run, and the result of every data quality rule per run |

The fact table is keyed by `event_key` (the source name and the source's own event id). The dimensions hold the labels once: two sources, 13 hazard types in 6 groups, and a calendar for 2020 to 2030. The field by field mapping from each API to these columns is in `docs/field_mapping.md`.

The materialized view holds the events per day, source and hazard type. It is refreshed after every merge without blocking the dashboard. `v_events` joins every event with its labels; the dashboard loads it once (about 40,000 rows, cached for 5 minutes) and filters it in memory, so magnitude, depth and region filters react instantly.

## Dashboard

The dashboard (`dashboard/app.py`) reads `mart.v_events` once, keeps it in memory for 5 minutes and does every filter and chart in pandas, so the page reacts instantly. It only reads; it never writes to the warehouse.

| Part | What it shows | Why |
| --- | --- | --- |
| Header | Last refresh time (green when the data is less than 3 hours old) and the chosen period | Shows at a glance that the pipeline is alive |
| Hazard tab line | All events, one tab per hazard type, and Pipeline health | One click changes the whole page, so each tab tells one story |
| Key numbers | Four numbers, compared with the period just before (▲/▼) | The most important facts first |
| What the data says | Three findings written from the filtered data (biggest share, busiest day, seasonal peak or most active place) | Insights in words, not only charts |
| Charts | Map, what happened, where it happened, when it happened (one square per day) | What, where and when, each answered once |
| Event list | Every matching event, searchable, with a CSV download | Find one event during a demo |
| Pipeline health | Pipeline flow, success rate, recent runs, quality rules checked live, data per source | Shows that the data can be trusted |

Choices made while building it:

- **The magnitude filter starts at 2.5 and only shows where earthquakes are.** The backfill only loads earthquakes of 2.5 or more, while the hourly feed also brings smaller ones. Mixing them would make the latest days look like a sudden spike. NASA events have no magnitude, so the filter is hidden on their tabs.
- **Places are computed in the dashboard, not stored.** Earthquakes take the place named in the USGS title. NASA titles often name no place, so their events get the country of their coordinates from an offline list of world towns (`reverse_geocoder`, `pycountry`); area-only events use a country named in their title. No external service is called.
- **No invented locations.** Events that NASA reports only as an area (most recent floods) have no point. They are counted and placed in a country by title, but not drawn on the map.
- **Calm colors.** Each hazard type has one muted color, used on every chart and map, so a color always means the same thing.

## Design decisions

- **Raw data first.** Every response is saved in the lake before anything else happens. If the transform or the merge has a bug, the run can be repeated from the lake without calling the APIs again.
- **One common format for two sources.** USGS and EONET have different fields. Spark turns both into the same columns, maps every event to one of our hazard types, and rejects rows that cannot be used (for example a missing id), counting the reason for each.
- **Idempotent merge.** The merge uses `INSERT ... ON CONFLICT (event_key) DO UPDATE`. An event is only changed when the source reports a newer update time, `first_seen_utc` is never overwritten, and `last_seen_utc` always moves forward. Running the same data twice changes nothing, so a retry or a rerun is always safe.
- **Failures are visible.** Each task retries twice, two minutes apart. If the run still fails, the tasks after it are marked `upstream_failed`, `record_run` still runs, and the error is saved in `ops.pipeline_runs`.
- **Data quality as part of the run.** The last task checks the rules in `sql/queries.sql` and stores the results. A failed error rule fails the task, and a failed warning rule only shows on the dashboard.
- **Roles for least privilege.** The schema defines two roles without login: `pipeline_writer` (reads and writes `staging`, `dw`, `ops`) and `dashboard_reader` (reads `mart` and `ops`). They describe the intended separation. In this local setup the services connect with the single database user from `.env`.
- **Local mode on purpose.** Spark runs inside the Airflow container on one node. The data is about 40,000 events per year, which a single node handles in seconds, and it keeps the project easy to start with one command.

## Limits

- The two sources are not merged. The same real earthquake can appear in both.
- The backfill loads earthquakes of magnitude 2.5 or higher, because the USGS archive returns at most 20,000 events per request.
- The counts show how many events were reported, not how severe they were.
- Events that NASA reports only as an area are not drawn on the maps.
- Countries come from the nearest town to an event's point, so an event close to a border can be placed in the neighbouring country.