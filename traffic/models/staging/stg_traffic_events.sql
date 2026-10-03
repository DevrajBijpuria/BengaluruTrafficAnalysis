SELECT 
 NULLIF(TRIM(event_id),'') AS event_id,
 event_ts,
 NULLIF(TRIM(area),'') AS area,
 NULLIF(TRIM(road_segment_id),'') AS road_segment_id,
 NULLIF(TRIM(road_name),'') AS road_name,
 latitude,
 longitude,
 vehicle_count,
 avg_speed_kmph,
 occupancy_pct,
 CASE 
   WHEN LOWER(TRIM(congestion_level)) = 'low' THEN 'Low'
   WHEN LOWER(TRIM(congestion_level)) = 'medium' THEN 'Medium'
   WHEN LOWER(TRIM(congestion_level)) = 'high' THEN 'High'
   WHEN LOWER(TRIM(congestion_level)) = 'severe' THEN 'Severe'
   ELSE NULLIF(TRIM(congestion_level), '') 
 END AS congestion_level,
 NULLIF(TRIM(weather), '') AS weather,
 NULLIF(TRIM(incident_type), '') AS incident_type,
 incident_duration_minutes,
 is_weekend,
 hour_of_day,
 NULLIF(TRIM(day_of_week), '') AS day_of_week,
 NULLIF(TRIM(data_source), '') AS data_source,
 generated_at,
 NULLIF(TRIM(anomaly_type), '') AS anomaly_type
   FROM {{source('raw','RAW_TRAFFIC_DATA')}}