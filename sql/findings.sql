-- Numbers used in the report findings. One query, one result table. Run it in DBeaver.
SELECT sec, label, val
FROM (
    SELECT 'A hazard type' AS sec, h.display_name AS label, count(*)::numeric AS val
    FROM dw.fact_event f JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
    WHERE f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
    GROUP BY h.display_name
    UNION ALL
    SELECT 'B magnitude band',
           CASE WHEN f.magnitude >= 7 THEN '7 and above'
                WHEN f.magnitude >= 6 THEN '6 to 6.9'
                WHEN f.magnitude >= 5 THEN '5 to 5.9'
                WHEN f.magnitude >= 4 THEN '4 to 4.9'
                WHEN f.magnitude >= 3 THEN '3 to 3.9'
                ELSE 'under 3' END,
           count(*)
    FROM dw.fact_event f JOIN dw.dim_source s ON s.source_key = f.source_key
    WHERE s.source_name = 'usgs' AND f.magnitude IS NOT NULL
      AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
    GROUP BY 2
    UNION ALL
    (SELECT 'C strongest earthquakes',
            f.event_time_utc::date || ' | ' || coalesce(f.title, ''),
            f.magnitude
     FROM dw.fact_event f JOIN dw.dim_source s ON s.source_key = f.source_key
     WHERE s.source_name = 'usgs' AND f.magnitude IS NOT NULL
       AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
     ORDER BY f.magnitude DESC LIMIT 5)
    UNION ALL
    (SELECT 'D busiest earthquake days', f.event_time_utc::date::text, count(*)
     FROM dw.fact_event f JOIN dw.dim_source s ON s.source_key = f.source_key
     WHERE s.source_name = 'usgs'
       AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
     GROUP BY 2 ORDER BY 3 DESC LIMIT 5)
    UNION ALL
    SELECT 'E wildfires per month', to_char(f.event_time_utc, 'YYYY-MM'), count(*)
    FROM dw.fact_event f JOIN dw.dim_hazard_type h ON h.hazard_key = f.hazard_key
    WHERE h.hazard_type = 'wildfire'
      AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
    GROUP BY 2
    UNION ALL
    SELECT 'F EONET open or closed', coalesce(f.event_status, 'unknown'), count(*)
    FROM dw.fact_event f JOIN dw.dim_source s ON s.source_key = f.source_key
    WHERE s.source_name = 'eonet'
      AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
    GROUP BY 2
    UNION ALL
    SELECT 'G earthquake depth',
           CASE WHEN f.depth_km < 70 THEN 'shallow (under 70 km)'
                WHEN f.depth_km <= 300 THEN 'intermediate (70 to 300 km)'
                ELSE 'deep (over 300 km)' END,
           count(*)
    FROM dw.fact_event f JOIN dw.dim_source s ON s.source_key = f.source_key
    WHERE s.source_name = 'usgs' AND f.depth_km IS NOT NULL
      AND f.event_time_utc >= '2025-12-01' AND f.event_time_utc < '2026-11-20'
    GROUP BY 2
) x
ORDER BY sec,
         CASE WHEN sec = 'E wildfires per month' THEN label END,
         val DESC;

-- Where the busiest earthquake day happened (finding 5).
SELECT regexp_replace(f.title, '^.* of ', '') AS place, count(*) AS quakes
FROM dw.fact_event f
JOIN dw.dim_source s ON s.source_key = f.source_key
WHERE s.source_name = 'usgs' AND f.event_time_utc::date = '2025-12-07'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 8;