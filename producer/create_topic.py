from kafka.admin import KafkaAdminClient, NewTopic


def create(topic_name):
    admin = KafkaAdminClient(
        bootstrap_servers=["brokers:9093"],
        client_id="water-weather-predictions"
    )

    topics = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    admin.create_topics(new_topics=topics, validate_only=False)

    print(f"CREATED TOPIC {topic_name}")

if __name__ == "__main__":
    create("Gallatin")
    create("Jefferson")
    create("Madison")
    create("Madison_Dam_Release")
