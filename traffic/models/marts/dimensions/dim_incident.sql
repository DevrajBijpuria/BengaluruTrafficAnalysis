SELECT 
  DISTINCT MD5(LOWER(TRIM(incident_type))) AS incident_key,
  TRIM(incident_type) AS  incident_type
    FROM {{ref('int_traffic_events_deduplicated')}}
      WHERE NULLIF(TRIM(incident_type), '') IS NOT NULL