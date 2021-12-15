import json
import psycopg2

from kafka import KafkaConsumer

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
        messages.append(message.value)

    _insert_messages(messages)

def _insert_messages(messages):
    with psycopg2.connect(
        database="postgres",
        host="pgdb",
        user="postgres",
        password="pg_password",
        port="5432"
    ) as connection:

        cursor = connection.cursor()
        cursor.execute("SELECT version()")
        v = cursor.fetchone()

        print(f"Established connection to {v}")

if __name__ == "__main__":
    consume()
