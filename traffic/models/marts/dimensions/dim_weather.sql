SELECT 
  DISTINCT MD5(LOWER(TRIM(weather))) AS weather_key,
  TRIM(weather) AS weather
   FROM {{ref('int_traffic_events_deduplicated')}}
     WHERE NULLIF(TRIM(weather),'') IS NOT NULL