import json
from time import sleep

from kafka import KafkaProducer


def produce():

    producer = KafkaProducer(
        bootstrap_servers=["brokers:9093"],
        value_serializer=lambda msg: json.dumps(msg).encode()
    )

    for i in range(10):
        payload = {"num": i}
        producer.send("test-topic", value=payload)
        print(f"SENT {payload}")
        sleep(0.5)

if __name__ == "__main__":
    produce()
