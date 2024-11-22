from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення топіків
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topics = [
    NewTopic(name="building_sensors_topic", num_partitions=2, replication_factor=1),
    NewTopic(name="temperature_alerts_topic", num_partitions=2, replication_factor=1),
    NewTopic(name="humidity_alerts_topic", num_partitions=2, replication_factor=1)
]

try:
    existing_topics = admin_client.list_topics()
    topics_to_create = [topic for topic in topics if topic.name not in existing_topics]

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print("Topics created successfully.")
    else:
        print("No topics to create. All topics already exist.")

except Exception as e:
    print(f"An error occurred: {e}")

print("Available topics:", admin_client.list_topics())
admin_client.close()
