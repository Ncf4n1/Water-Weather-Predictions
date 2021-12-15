import json
import requests
from time import sleep
from datetime import datetime
from kafka import KafkaProducer

def api_to_kafka(endpoint: str):
    """Executes the API request to USGS"""
    response = requests.get(endpoint)
    river_json = json.dumps(response.json(), indent=3)
    river_dict = (json.loads(river_json))
    parse_data(river_dict)


def parse_data(river_dict: dict):
    """Parses out the necessary information to produce events to Kafka"""
    data_dict = river_dict["value"]["timeSeries"][0]["sourceInfo"]
    name = data_dict["siteName"]
    code = data_dict["siteCode"][0]["value"]
    geolocation = data_dict["geoLocation"]["geogLocation"]
    time_series = river_dict["value"]["timeSeries"][0]["values"][0]["value"]
    produce_hydrograph_events(name, code, geolocation, time_series)


def produce_hydrograph_events(name: str, code: str, geolocation: dict, time_series: dict):
    """Produces all events for a hydrograph to a topic in Kafka"""
    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=["brokers:9093"],
        value_serializer=lambda msg: json.dumps(msg).encode()
    )

    topic = name.split()[0]  # Write topics as river name

    for event in time_series:
        # Build payload
        discharge = event["value"]
        time = event["dateTime"]
        payload = {"name": name, "code": code, "geolocation": geolocation, "time": time, "discharge": discharge}

        # Do time conversion for Kafka
        time_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f%z')
        epoch_ms = int(time_obj.timestamp() * 1000)
        print(epoch_ms)

        # Produce event to topic
        producer.send(topic=topic, value=payload, timestamp_ms=epoch_ms)
        print(f"SENT {payload}")
        # sleep(0.1)  # TODO - Is this necessary or was this just for your test script?


if __name__ == "__main__":
    # USGS Endpoint URLs (for 7-Day period, returning only the discharge in cfs)
    gallatin_endpoint = "http://waterservices.usgs.gov/nwis/iv/?site=06043500&period=P7D&format=json&parameterCd=00060"
    jefferson_endpoint = "http://waterservices.usgs.gov/nwis/iv/?site=06026500&period=P7D&format=json&parameterCd=00060"
    lowerMadison_endpoint = "http://waterservices.usgs.gov/nwis/iv/?site=06041000&period=P7D&format=json&parameterCd=00060"
    upperMadison_endpoint = "http://waterservices.usgs.gov/nwis/iv/?site=06038800&period=P7D&format=json&parameterCd=00060"
    # Produce to Kafka for all four river sites
    api_to_kafka(gallatin_endpoint)
    api_to_kafka(jefferson_endpoint)
    api_to_kafka(lowerMadison_endpoint)
    api_to_kafka(upperMadison_endpoint)
