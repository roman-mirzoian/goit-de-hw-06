import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from configs import BUILDING_SENSORS_TOPIC, kafka_config

# One run of the script is one sensor: the ID stays the same until the script stops
SENSOR_ID = random.randint(1000, 9999)
SEND_INTERVAL_SECONDS = 2

producer = KafkaProducer(
    **kafka_config,
    key_serializer=lambda key: str(key).encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(f"Sensor {SENSOR_ID} started, sending data to '{BUILDING_SENSORS_TOPIC}'")

try:
    while True:
        data = {
            "id": SENSOR_ID,
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        producer.send(BUILDING_SENSORS_TOPIC, key=SENSOR_ID, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(SEND_INTERVAL_SECONDS)
except KeyboardInterrupt:
    print(f"Sensor {SENSOR_ID} stopped")
finally:
    producer.close()
