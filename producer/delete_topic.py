from kafka.admin import KafkaAdminClient, NewTopic


def delete(topic_name):
    admin = KafkaAdminClient(
        bootstrap_servers=["brokers:9093"],
        client_id="water-weather-predictions"
    )

    admin.delete_topics(topics=[topic_name])

    print(f"DELETED TOPIC {topic_name}")

if __name__ == "__main__":
    delete("Gallatin")
    delete("Jefferson")
    delete("Madison")
    delete("Madison_Dam_Release")
