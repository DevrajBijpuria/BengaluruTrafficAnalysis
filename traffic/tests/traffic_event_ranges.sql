
SELECT *
FROM {{ ref('stg_traffic_events') }}
WHERE
       (latitude IS NOT NULL AND latitude NOT BETWEEN -90 AND 90)
    OR (longitude IS NOT NULL AND longitude NOT BETWEEN -180 AND 180)
    OR (vehicle_count IS NOT NULL AND vehicle_count < 0)
    OR (avg_speed_kmph IS NOT NULL AND avg_speed_kmph < 0)
    OR (occupancy_pct IS NOT NULL AND occupancy_pct NOT BETWEEN 0 AND 100)
    OR (incident_duration_minutes IS NOT NULL
        AND incident_duration_minutes < 0)
    OR (hour_of_day IS NOT NULL AND hour_of_day NOT BETWEEN 0 AND 23)
