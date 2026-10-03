SELECT
    e.event_id,
    TO_NUMBER(TO_CHAR(TO_DATE(e.event_ts), 'YYYYMMDD')) AS date_key,
    e.road_segment_id,
    MD5(LOWER(TRIM(e.weather))) AS weather_key,
    MD5(LOWER(TRIM(e.incident_type))) AS incident_key,

    e.event_ts,
    e.vehicle_count,
    e.avg_speed_kmph,
    e.occupancy_pct,
    e.congestion_level,
    e.incident_duration_minutes,
FROM {{ ref('int_traffic_events_deduplicated') }} AS e