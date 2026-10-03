from confluent_kafka import Consumer
import json
consumer=Consumer({"bootstrap.servers":"localhost:9092",
                    "group.id":"0",
                    "auto.offset.reset":"earliest"})
consumer.subscribe(["traffic-events"])
try:
  while True:
      msg=consumer.poll(1.0)
      if msg is None:
          print("No msg received")
          continue
      if msg.error() is not None :
          print(f"Error form consumer side is {msg.error()}")
          continue
      try:
          records=json.loads(msg.value().decode("utf-8"))
          print(records)
      except(json.JSONDecodeError,UnicodeDecodeError) as e :
          print("Invalid msg: {e}")
except KeyboardInterrupt:
    print("Consumer Stopped") 
finally :
    consumer.close()       
