import json
import os

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")

def consume():
    consumer = KafkaConsumer(
        "test-topic",
        bootstrap_servers=["brokers:9093"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-consumers",
        value_deserializer=lambda msg: json.loads(msg.decode()),
        consumer_timeout_ms=3000
    )

    messages = []
    for message in consumer:
        print(f"CONSUMED {message.value}")
        messages.append(f"test_point,location=Bozeman point_val={message.value.get('num')}")

    _insert_messages(messages)

def _insert_messages(messages):
    with InfluxDBClient(org=INFLUX_ORG, token=INFLUX_TOKEN, url="http://influx:8086") as client:

        writer = client.write_api(write_options=SYNCHRONOUS)

        for message in messages:
            writer.write(INFLUX_BUCKET, INFLUX_ORG, message)
            print(f"WROTE {message} to bucket")

            query_api = client.query_api()
            query = ' from(bucket:"test_bucket")\
            |> range(start: -10m)\
            |> filter(fn:(r) => r._measurement == "test_point")\
            |> filter(fn:(r) => r.location == "Bozeman")\
            |> filter(fn:(r) => r._field == "point_val" )'
            tables = query_api.query(query, org=INFLUX_ORG)
            for table in tables:
                for record in table.records:
                    print(record.get_field(), record.get_value())

if __name__ == "__main__":
    consume()
