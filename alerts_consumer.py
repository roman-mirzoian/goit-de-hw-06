import json

from kafka import KafkaConsumer

from configs import ALERTS_TOPIC, MY_NAME, kafka_config

consumer = KafkaConsumer(
    ALERTS_TOPIC,
    **kafka_config,
    group_id=f"{MY_NAME}_streaming_alerts_consumer",
    auto_offset_reset="earliest",
)

print(f"Listening to '{ALERTS_TOPIC}'...")

try:
    for record in consumer:
        alert = json.loads(record.value.decode("utf-8"))
        print(json.dumps(alert, ensure_ascii=False))
except KeyboardInterrupt:
    print("Alerts consumer stopped")
finally:
    consumer.close()
