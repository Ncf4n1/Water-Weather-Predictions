from kafka.admin import KafkaAdminClient, NewTopic


def delete(topic_name):
    admin = KafkaAdminClient(
        bootstrap_servers=["brokers:9093"],
        client_id="test-id"
    )

    admin.delete_topics(topics=[topic_name])

    print(f"DELETED TOPIC {topic_name}")

if __name__ == "__main__":
    delete("test-topic")
