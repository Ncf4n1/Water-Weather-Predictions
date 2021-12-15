import json
import psycopg2
from psycopg2.sql import Literal, SQL
from kafka import KafkaConsumer

def consume(topic: str):
    """Consumes the Hydrograph River messages from Kafka"""
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
        messages.append(message.value)

    if len(messages):
        _insert_messages(messages)

def _insert_messages(messages: list[dict]):
    """Bulk Inserts consumed messages from Kafka into the PostGIS Database"""
    with psycopg2.connect(
        database="postgres",
        host="pgdb",
        user="postgres",
        password="pg_password",
        port="5432"
    ) as connection:

        # Build bulk insert command with safe composable SQL
        messages = [
            SQL("""
            (
                {code},
                {name},
                {discharge},
                {recorded},
                ST_SetSRID(ST_MakePoint({x}, {y}), 4326)
            )
            """).format(
                code=Literal(message.get("code")),
                name=Literal(message.get("name")),
                discharge=Literal(message.get("discharge")),
                recorded=Literal(message.get("time")),
                x=Literal(message.get("geolocation").get("latitude")),
                y=Literal(message.get("geolocation").get("longitude"))
            ) for message in messages
        ]

        cursor = connection.cursor()

        cursor.execute(
        SQL("""
            INSERT INTO water_data
            (station_code, station_name, discharge, recorded_at, station_point)
            VALUES {values}
            """).format(values=SQL(", ").join(messages))
        )


if __name__ == "__main__":
    consume("Gallatin")
    consume("Jefferson")
    consume("Madison")
