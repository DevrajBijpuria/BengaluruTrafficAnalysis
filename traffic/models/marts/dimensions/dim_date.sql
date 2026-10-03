WITH dates AS(
     SELECT DISTINCT TO_DATE(event_ts) AS date_day 
      FROM {{ref('int_traffic_events_deduplicated')}}
        WHERE event_ts IS NOT NULL

     UNION
     
     SELECT DISTINCT TO_DATE(window_start) AS date_day 
       FROM {{ref('stg_traffic_metrics_5min')}}
        WHERE window_start IS NOT NULL
)

SELECT 
  TO_NUMBER(TO_CHAR(date_day,'YYYYMMDD')) AS date_key,
  date_day,
  YEAR(date_day) AS year,
  QUARTER(date_day) AS quarter,
  MONTH(date_day) AS month,
  MONTHNAME(date_day) AS month_name,
  DAY(date_day) AS day_of_month,
  DAYOFWEEKISO(date_day) AS day_of_week,
  DAYNAME(date_day) AS day_name,
  IFF(DAYOFWEEKISO(date_day) IN (6,7),TRUE,FALSE) AS is_weekend
    FROM dates
