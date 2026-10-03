SELECT
    TO_NUMBER(TO_CHAR(TO_DATE(window_start), 'YYYYMMDD')) AS date_key,
    road_segment_id,

    window_start,
    window_end,
    area,
    road_name,

    total_events,
    avg_vehicle_count,
    avg_speed_kmph,
    avg_occupancy_pct,
    peak_congestion_rank,
    peak_congestion_level

FROM {{ ref('stg_traffic_metrics_5min') }}