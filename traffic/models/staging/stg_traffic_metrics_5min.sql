
SELECT 
    window_start,
    window_end,
    NULLIF(TRIM(area), '') AS area,
    NULLIF(TRIM(road_segment_id), '') AS road_segment_id,
    NULLIF(TRIM(road_name), '') AS road_name,
    total_events,
    avg_vehicle_count,
    avg_speed_kmph,
    avg_occupancy_pct,

    CASE
        WHEN LOWER(TRIM(peak_congestion_level)) = 'low' THEN 1
        WHEN LOWER(TRIM(peak_congestion_level)) = 'medium' THEN 2
        WHEN LOWER(TRIM(peak_congestion_level)) = 'high' THEN 3
        WHEN LOWER(TRIM(peak_congestion_level)) = 'severe' THEN 4
        ELSE NULL
    END AS peak_congestion_rank,

    CASE
        WHEN LOWER(TRIM(peak_congestion_level)) = 'low'
            THEN 'Low'
        WHEN LOWER(TRIM(peak_congestion_level)) = 'medium'
            THEN 'Medium'
        WHEN LOWER(TRIM(peak_congestion_level)) = 'high'
            THEN 'High'
        WHEN LOWER(TRIM(peak_congestion_level)) = 'severe'
            THEN 'Severe'
        ELSE NULLIF(TRIM(peak_congestion_level), '')
    END AS peak_congestion_level

FROM {{ source('aggregated', 'TRAFFIC_METRICS_5MIN') }}
