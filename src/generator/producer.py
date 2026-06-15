import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["kafka:29092"]
TOPIC = "iot-events"
DEVICE_IDS = ["device-1", "device-2", "device-3", "device-4", "device-5"]
DEVICE_TYPE_IDS = [1, 2, 3, 4, 5]

def build_message():
    return {
        "device_id": random.choice(DEVICE_IDS),
        "device_type_id": random.choice(DEVICE_TYPE_IDS),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15.0, 40.0), 2),
        "humidity": round(random.uniform(20.0, 90.0), 2),
    }

def run():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        while True:
            msg = build_message()
            producer.send(TOPIC, value=msg)
            print(msg)
            time.sleep(1)
    finally:
        producer.close()

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        pass
