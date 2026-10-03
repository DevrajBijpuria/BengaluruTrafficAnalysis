WITH ranked as(
     SELECT * , 
       ROW_NUMBER()OVER( PARTITION BY event_id ORDER BY generated_at DESC NULLS LAST , event_ts DESC NULLS LAST) AS rn
          FROM {{ref('stg_traffic_events')}}
           WHERE event_id IS NOT NULL
)

SELECT * FROM ranked WHERE rn =1