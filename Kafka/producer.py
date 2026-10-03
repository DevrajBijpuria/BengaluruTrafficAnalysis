
import pandas as pd
import json
from pathlib import Path
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

folder = Path(r"C:\Users\dbijp\OneDrive\Desktop\DEVRAJ\Data_Pipeline_pro1\Real_Time_Traffic\output")
topic = "traffic-events"

def mssg_sending_failure(err, msg):
    if err:
        print(f"Message sending failed: {err}")

for f in sorted(folder.glob("traffic_events_part-*csv")):
    print(f"Reading file: {f.name}")

    for chunk in pd.read_csv(f, chunksize=10000):
        for row in chunk.to_dict(orient="records"):
            msg = json.dumps(row, default=str)

            producer.produce(
                topic=topic,
                key=str(row["road_segment_id"]),
                value=msg,
                callback=mssg_sending_failure
            )

            producer.poll(0)

    print(f"Finished processing: {f.name}")

print("All CSV files processed. Waiting for Kafka delivery...")
producer.flush()
print("All messages have been sent to Kafka.")
