SELECT
    road_segment_id,
    area,
    road_name,
    latitude,
    longitude 
     FROM ( 
         SELECT  
            road_segment_id,
            area,
            road_name,
            latitude,
            longitude, 
            ROW_NUMBER() OVER (PARTITION BY road_segment_id ORDER BY generated_at DESC NULLS LAST , event_ts DESC NULLS LAST) as rn 
             FROM {{ ref('int_traffic_events_deduplicated')}}
              WHERE road_segment_id IS NOT NULL
     ) t  
        WHERE rn =1