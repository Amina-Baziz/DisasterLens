# Field mapping

This file lists where every column of the warehouse comes from. Both sources are
converted into one common format by `src/transform.py`, loaded into
`staging.stg_event`, and then merged into `dw.fact_event`.

## Sources

| Source | What it is | Kind |
|---|---|---|
| USGS | Earthquake catalog, measured by instruments | observation |
| NASA EONET | Natural events (wildfires, storms, volcanoes and more), curated from news and satellite sources | curated report |

## Column mapping

| Warehouse column | USGS field | EONET field | Notes |
|---|---|---|---|
| `event_key` | `"usgs:" + id` | `"eonet:" + id` | Primary key. Source name plus source id, so ids from different sources never clash. |
| `source` | fixed `usgs` | fixed `eonet` | Becomes `source_key` through `dw.dim_source`. |
| `source_event_id` | `features[].id` | `events[].id` | Rows without an id are rejected (`missing_id`). |
| `source_category` | `properties.type` | `categories[0].id` | The original category, kept as delivered. |
| `hazard_type` | from the hazard mapping below | from the hazard mapping below | Becomes `hazard_key` through `dw.dim_hazard_type`. Categories not in the mapping become `other`. |
| `title` | `properties.place` | `title` | |
| `event_time_utc` | `properties.time` (milliseconds since 1970, converted to UTC) | latest `date` among all `geometry` entries | Rows without a time are rejected (`missing_time`). Also gives `date_key`. |
| `latitude` | `geometry.coordinates[1]` | latitude of the most recent `Point` in `geometry` | Empty when outside -90 to 90 or when there is no point. |
| `longitude` | `geometry.coordinates[0]` | longitude of the most recent `Point` in `geometry` | Empty when outside -180 to 180 or when there is no point. |
| `magnitude` | `properties.mag` | not provided | Can be empty for USGS as well. |
| `depth_km` | `geometry.coordinates[2]` | not provided | |
| `event_status` | not provided | `closed` if `closed` has a value, otherwise `open` | |
| `has_valid_point` | true when latitude and longitude are both present | same | EONET events that only have an area shape (polygon) are kept with `false` and an empty location. |
| `source_updated_utc` | `properties.updated` (milliseconds, converted to UTC) | latest `date` among all `geometry` entries | Used to decide which copy of a duplicate is the newest. |
| `geometry_count` | fixed 1 | number of entries in `geometry` | Staging only. |
| `raw_object_path` | path of the raw file the row came from | same | Lets any row be traced back to the data lake. |
| `run_id` | id of the pipeline run that loaded it | same | |
| `ingested_at_utc` | time Spark processed the row | same | Staging only. |
| `first_seen_utc` | time the row was first loaded | same | Warehouse only, set once. |
| `last_seen_utc` | time the row was last loaded | same | Warehouse only, updated on every load. |

## Hazard mapping

| Source | Original category | Hazard type | Hazard group |
|---|---|---|---|
| usgs | earthquake | earthquake | geophysical |
| eonet | earthquakes | earthquake | geophysical |
| eonet | volcanoes | volcano | geophysical |
| eonet | landslides | landslide | geophysical |
| eonet | wildfires | wildfire | climatological |
| eonet | drought | drought | climatological |
| eonet | tempExtremes | temperature_extreme | climatological |
| eonet | severeStorms | severe_storm | meteorological |
| eonet | dustHaze | dust_haze | meteorological |
| eonet | snow | snow | meteorological |
| eonet | floods | flood | hydrological |
| eonet | seaLakeIce | sea_lake_ice | cryospheric |
| eonet | waterColor | water_color | other |
| any | anything else | other | other |

## Rows that are rejected

| Reason | Rule |
|---|---|
| `missing_id` | The event has no id. |
| `missing_time` | The event has no usable time. |
| `not_an_earthquake` | A USGS event whose type is not `earthquake` (for example quarry blasts). |
| `manmade_event` | An EONET event in the `manmade` category. |

Duplicates are removed by event key. When the same event appears more than once,
the copy with the newest `source_updated_utc` is kept.

## Data contract notes

- All times are stored in UTC.
- Both sources can report the same real earthquake. The two records are kept
  separate because their ids differ.
- Counts show how many events were reported, not how severe they were.
