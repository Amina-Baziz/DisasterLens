-- DisasterLens: analytics queries and data quality rules.
-- Every statement ends with a semicolon, so the whole file can be run at once
-- (in DBeaver: Alt+X) and each statement shows its own result tab.


-- =====================================================================
-- Part 1: analytics queries
-- =====================================================================

-- Q1. How many events per hazard type, split by source?
SELECT
    s.source_name,
    h.display_name AS hazard,
    count(*)       AS events
FROM dw.fact_event f
JOIN dw.dim_source      s ON s.source_key = f.source_key
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
GROUP BY s.source_name, h.display_name
ORDER BY events DESC;

-- Q2. Daily trend: events per day and hazard group (reads the pre-computed summary).
SELECT
    full_date,
    hazard_group,
    sum(event_count) AS events
FROM mart.mv_daily_hazard_summary
GROUP BY full_date, hazard_group
ORDER BY full_date, hazard_group;

-- Q3. Earthquake magnitude buckets (USGS and EONET earthquakes that have a magnitude).
SELECT
    floor(f.magnitude)::int AS magnitude_from,
    count(*)                AS earthquakes
FROM dw.fact_event f
JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
WHERE h.hazard_type = 'earthquake'
  AND f.magnitude IS NOT NULL
GROUP BY floor(f.magnitude)::int
ORDER BY magnitude_from;

-- Q4. The 20 most recent events.
SELECT *
FROM mart.v_latest_events
LIMIT 20;

-- Q5. Coverage per source: totals, how many have a location, and how fresh the data is.
SELECT *
FROM mart.v_source_coverage;


-- =====================================================================
-- Part 2: data quality rules
-- One row per rule. failed_count = 0 means the rule passed.
-- severity 'error'   = the data is wrong and must be fixed.
-- severity 'warning' = worth a look, but the data can still be used.
-- =====================================================================

SELECT
    rule_name,
    severity,
    failed_count,
    failed_count = 0 AS passed,
    detail
FROM (

    -- No two rows share the same event_key.
    SELECT 'pk_unique' AS rule_name, 'error' AS severity,
           (count(*) - count(DISTINCT event_key))::bigint AS failed_count,
           'duplicate event_key values' AS detail
    FROM dw.fact_event

    UNION ALL
    -- Event id, source and hazard type are never empty.
    SELECT 'required_fields', 'error',
           count(*) FILTER (WHERE source_event_id IS NULL OR source_event_id = ''
                               OR source_key IS NULL OR hazard_key IS NULL),
           'rows with a missing event id, source or hazard type'
    FROM dw.fact_event

    UNION ALL
    -- Latitude is between -90 and 90, longitude between -180 and 180.
    SELECT 'coords_in_range', 'error',
           count(*) FILTER (WHERE latitude NOT BETWEEN -90 AND 90
                               OR longitude NOT BETWEEN -180 AND 180),
           'rows with coordinates outside the valid range'
    FROM dw.fact_event

    UNION ALL
    -- The daily summary adds up to the number of rows in the fact table.
    SELECT 'summary_matches_fact', 'error',
           abs((SELECT coalesce(sum(event_count), 0) FROM mart.mv_daily_hazard_summary)
               - (SELECT count(*) FROM dw.fact_event))::bigint,
           'difference between the daily summary total and the fact table row count'

    UNION ALL
    -- No event is more than 1 day in the future.
    SELECT 'no_future_events', 'warning',
           count(*) FILTER (WHERE event_time_utc > (now() AT TIME ZONE 'utc') + interval '1 day'),
           'events dated more than 1 day in the future'
    FROM dw.fact_event

    UNION ALL
    -- Share of EONET events without a point location (warns above 20 percent).
    SELECT 'missing_point_share', 'warning',
           CASE WHEN coalesce(100.0 * count(*) FILTER (WHERE NOT f.has_valid_point)
                              / NULLIF(count(*), 0), 0) > 20 THEN 1 ELSE 0 END,
           round(coalesce(100.0 * count(*) FILTER (WHERE NOT f.has_valid_point)
                          / NULLIF(count(*), 0), 0), 1) || ' percent of EONET events have no point location'
    FROM dw.fact_event f
    JOIN dw.dim_source s ON s.source_key = f.source_key
    WHERE s.source_name = 'eonet'

    UNION ALL
    -- Events that fell into the 'other' hazard type.
    SELECT 'unmapped_category', 'warning',
           count(*),
           'events mapped to other because their category is not in our list'
    FROM dw.fact_event f
    JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
    WHERE h.hazard_type = 'other'

    UNION ALL
    -- Rows removed by Spark in the latest run (quarry blasts, man-made events, bad rows).
    SELECT 'rejected_rows', 'warning',
           coalesce((SELECT rows_rejected FROM ops.pipeline_runs
                     ORDER BY finished_at_utc DESC NULLS LAST LIMIT 1), 0)::bigint,
           'rows rejected by Spark in the latest run'

    UNION ALL
    -- The latest data is less than 3 hours old.
    SELECT 'freshness', 'warning',
           CASE WHEN max(last_seen_utc) IS NULL
                  OR max(last_seen_utc) < (now() AT TIME ZONE 'utc') - interval '3 hours'
                THEN 1 ELSE 0 END,
           'latest data seen at ' || coalesce(max(last_seen_utc)::text, 'never')
    FROM dw.fact_event

) AS checks
ORDER BY CASE severity WHEN 'error' THEN 0 ELSE 1 END, rule_name;