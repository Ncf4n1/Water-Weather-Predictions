from kafka.admin import KafkaAdminClient, NewTopic


def create(topic_name):
    admin = KafkaAdminClient(
        bootstrap_servers=["brokers:9093"],
        client_id="test-id"
    )

    topics = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    admin.create_topics(new_topics=topics, validate_only=False)

    print(f"CREATED TOPIC {topic_name}")

if __name__ == "__main__":
    create("test-topic")
