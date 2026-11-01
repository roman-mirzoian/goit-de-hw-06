import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS_TOPIC, BUILDING_SENSORS_TOPIC, MY_NAME, kafka_config

admin_client = KafkaAdminClient(**kafka_config)

topics = [
    NewTopic(name=name, num_partitions=2, replication_factor=1)
    for name in (BUILDING_SENSORS_TOPIC, ALERTS_TOPIC)
]

for topic in topics:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")

# Topic metadata reaches the broker with a small delay, so wait until all topics are visible
expected_topics = {topic.name for topic in topics}
for _ in range(10):
    if expected_topics.issubset(admin_client.list_topics()):
        break
    time.sleep(1)

print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
