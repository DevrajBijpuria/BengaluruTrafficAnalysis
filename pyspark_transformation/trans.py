
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("TrafficEvent")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic-events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 2000)
    .load()
)

traffic_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("area", StringType(), True),
    StructField("road_segment_id", StringType(), True),
    StructField("road_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed_kmph", DoubleType(), True),
    StructField("occupancy_pct", DoubleType(), True),
    StructField("congestion_level", StringType(), True),
    StructField("weather", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("incident_duration_minutes", IntegerType(), True),
    StructField("is_weekend", BooleanType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("generated_at", StringType(), True),
    StructField("anomaly_type", StringType(), True)
])

traffic_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), traffic_schema).alias("data"))
    .select("data.*")
)

clean_event = traffic_df.withColumn(
    "event_ts",
    F.to_timestamp("event_timestamp")
)

clean_event = (
    clean_event
    .withColumn(
        "incident_type",
        F.when(
            F.col("incident_type").isin("Nan", "nan", ""),
            None
        ).otherwise(F.col("incident_type"))
    )
    .withColumn(
        "anomaly_type",
        F.when(
            F.col("anomaly_type").isin("Nan", "nan", ""),
            None
        ).otherwise(F.col("anomaly_type"))
    )
)

clean_event = clean_event.filter(
    (F.col("event_id").isNotNull()) &
    (F.col("event_ts").isNotNull()) &
    (F.col("area").isNotNull()) &
    (F.col("road_segment_id").isNotNull()) &
    (F.col("vehicle_count") >= 0) &
    (F.col("avg_speed_kmph") >= 0) &
    (
        F.col("occupancy_pct").isNotNull() &
        F.col("occupancy_pct").between(0, 100)
    ) &
    (F.col("congestion_level").isin("Low", "Medium", "High", "Severe"))
)

cleaned_events = clean_event.select(
    "event_id",
    "event_timestamp",
    "event_ts",
    "area",
    "road_segment_id",
    "road_name",
    "latitude",
    "longitude",
    "vehicle_count",
    "avg_speed_kmph",
    "occupancy_pct",
    "congestion_level",
    "weather",
    "incident_type",
    "incident_duration_minutes",
    "is_weekend",
    "hour_of_day",
    "day_of_week",
    "data_source",
    "generated_at",
    "anomaly_type"
)

windowed_events = (
    clean_event
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id"])
)

windowed_events = windowed_events.withColumn(
    "congestion_rank",
    F.when(F.col("congestion_level") == "Low", 1)
    .when(F.col("congestion_level") == "Medium", 2)
    .when(F.col("congestion_level") == "High", 3)
    .when(F.col("congestion_level") == "Severe", 4)
)

traffic_metrics = (
    windowed_events
    .groupBy(
        F.window("event_ts", "5 minutes"),
        "area",
        "road_segment_id",
        "road_name"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.avg("vehicle_count").alias("avg_vehicle_count"),
        F.avg("avg_speed_kmph").alias("avg_speed_kmph"),
        F.avg("occupancy_pct").alias("avg_occupancy_pct"),
        F.max("congestion_rank").alias("peak_congestion_rank")
    )
)

traffic_metrics = traffic_metrics.withColumn(
    "peak_congestion_level",
    F.when(F.col("peak_congestion_rank") == 1, "Low")
    .when(F.col("peak_congestion_rank") == 2, "Medium")
    .when(F.col("peak_congestion_rank") == 3, "High")
    .when(F.col("peak_congestion_rank") == 4, "Severe")
)

traffic_metrics = traffic_metrics.select(
    F.col("window.start").alias("window_start"),
    F.col("window.end").alias("window_end"),
    "area",
    "road_segment_id",
    "road_name",
    "event_count",
    F.round("avg_vehicle_count", 2).alias("avg_vehicle_count"),
    F.round("avg_speed_kmph", 2).alias("avg_speed_kmph"),
    F.round("avg_occupancy_pct", 2).alias("avg_occupancy_pct"),
    "peak_congestion_level"
)

s3_bucket = "real-time-traffic-devrajbijpuria"

raw_path = f"s3a://{s3_bucket}/raw/traffic_events/"
metrics_path = f"s3a://{s3_bucket}/aggregated/5min/"

raw_checkpoint = f"s3a://{s3_bucket}/checkpoints/raw/"
metrics_checkpoint = f"s3a://{s3_bucket}/checkpoints/5min/"

raw_query = (
    cleaned_events.writeStream
    .queryName("traffic_raw_to_s3")
    .format("parquet")
    .outputMode("append")
    .option("path", raw_path)
    .option("checkpointLocation", raw_checkpoint)
    .start()
)

metrics_query = (
    traffic_metrics.writeStream
    .queryName("traffic_metrics_to_s3")
    .format("parquet")
    .outputMode("append")
    .option("path", metrics_path)
    .option("checkpointLocation", metrics_checkpoint)
    .start()
)

print("Raw query active:", raw_query.isActive)
print("Metrics query active:", metrics_query.isActive)

spark.streams.awaitAnyTermination()
