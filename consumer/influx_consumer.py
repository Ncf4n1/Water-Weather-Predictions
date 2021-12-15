import json
import os
import s2cell
from datetime import datetime
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")

def consume(topic: str):
    """Consume messages from the given topic and structure in Influx Line Protocol"""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["brokers:9093"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="water-weather-consumers",
        value_deserializer=lambda msg: json.loads(msg.decode()),
        consumer_timeout_ms=2000
    )

    messages = []
    for message in consumer:
        print(f"CONSUMED {message.value}")

        code = message.value.get("code")
        name = message.value.get("name")
        discharge = message.value.get("discharge")

        time_obj = datetime.strptime(message.value.get("time"), '%Y-%m-%dT%H:%M:%S.%f%z')
        recorded_at = int(time_obj.timestamp() * 1000000000)

        lat = message.value.get("geolocation").get("latitude")
        lon = message.value.get("geolocation").get("longitude")

        cell_id = s2cell.lat_lon_to_token(lat, lon)

        messages.append(f"test_point,location={code},s2_cell_id={cell_id} station_name=\"{name}\",discharge={discharge},lat={lat},lon={lon}")

    _insert_messages(messages)

def _insert_messages(messages: list[str]):
    """Write messages to the Influx Cloud hydrograph_time_series bucket"""
    with InfluxDBClient(org=INFLUX_ORG, token=INFLUX_TOKEN, url="https://europe-west1-1.gcp.cloud2.influxdata.com") as client:

        writer = client.write_api(write_options=SYNCHRONOUS)

        writer.write(INFLUX_BUCKET, INFLUX_ORG, messages)
        print(f"WROTE messages to bucket")


if __name__ == "__main__":
    consume("Gallatin")
    consume("Jefferson")
    consume("Madison")
