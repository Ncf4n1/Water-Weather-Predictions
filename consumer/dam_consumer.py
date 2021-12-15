from datetime import datetime
import json
from time import sleep
from statistics import mean
from kafka import KafkaConsumer, KafkaProducer


def consume(topic: str):
    """Consumes the Madison River topic to analyze for a dam release"""
    # Initialize consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["brokers:9093"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="water-weather-consumers",
        value_deserializer=lambda msg: json.loads(msg.decode()),
        consumer_timeout_ms=2000
    )
    # Read events and pass to the dam release producer
    messages = []
    for message in consumer:
        print(f"CONSUMED {message.value}")
        messages.append(message.value)
    produce_dam_releases(messages)


def produce_dam_releases(messages: list):
    """Takes a running average of the discharge and writes to a topic if the average increases significantly, indicating a dam release."""
    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=["brokers:9093"],
        value_serializer=lambda msg: json.dumps(msg).encode()
    )

    # Iterate over events and maintain an average of the last 10 discharges on the Lower Madison
    lastTenValues = []
    i = 1  # Dam release counter
    for event in messages:
        if event["code"] == "06041000":
            discharge = int(event["discharge"])
            lastTenValues.append(discharge)
            if len(lastTenValues) > 10:
                lastTenValues.pop(0)
            runningAvg = mean(lastTenValues)

            # If an event's discharge is significantly higher (1.2x) than the running average, flag and write to Kafka
            if discharge > 1.2 * runningAvg:  # TODO - Test if the 1.2 coefficient should be increased or decreased
                # Do time conversion for Kafka
                time = event["time"]
                time_obj = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f%z')
                epoch_ms = int(time_obj.timestamp() * 1000)

                # Build payload
                payload = {"name": "Madison Dam Release " + str(i), "time": time, "discharge": discharge,
                           "code": event["code"], "geolocation": event["geolocation"]}
                i += 1

                # Produce dam release event to topic
                print("Madison River dam release detected at " + time + "!")
                producer.send(topic="Madison_Dam_Release", value=payload, timestamp_ms=epoch_ms)
                print(f"SENT {payload}")
                sleep(0.01)


if __name__ == "__main__":
    consume("Madison")
