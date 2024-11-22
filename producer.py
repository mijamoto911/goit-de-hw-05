from kafka import KafkaProducer
from configs import kafka_config
from datetime import datetime
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_id = random.randint(1000, 9999)

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2)
        }
        producer.send('building_sensors_topic', value=data)
        print(f"Sent: {data}")
        time.sleep(5)

except KeyboardInterrupt:
    print("Sensor stopped.")

finally:
    producer.close()
