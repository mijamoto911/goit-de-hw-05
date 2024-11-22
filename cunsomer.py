from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    'building_sensors_topic',
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_3'
)
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
try:
    for message in consumer:
        try:
            data = message.value
            alerts = []

            # Перевірка температури
            if data['temperature'] > 40:
                alerts.append({
                    "sensor_id": data['sensor_id'],
                    "timestamp": data['timestamp'],
                    "temperature": data['temperature'],
                    "message": "Temperature exceeds 40°C!"
                })

            # Перевірка вологості
            if data['humidity'] > 80 or data['humidity'] < 20:
                alerts.append({
                    "sensor_id": data['sensor_id'],
                    "timestamp": data['timestamp'],
                    "humidity": data['humidity'],
                    "message": "Humidity is out of range (20%-80%)!"
                })

            # Відправка сповіщень
            for alert in alerts:
                try:
                    topic = 'temperature_alerts_topic' if 'temperature' in alert else 'humidity_alerts_topic'
                    producer.send(topic, value=alert)
                    print(f"Alert sent to {topic}: {alert}")
                except Exception as e:
                    print(f"Failed to send alert to {topic}: {e}")

        except Exception as e:
            print(f"Failed to process message: {e}")

except KeyboardInterrupt:
    print("Stopped by user.")

finally:
    try:
        consumer.close()
        producer.close()
        print("Kafka connections closed.")
    except Exception as e:
        print(f"Failed to close Kafka connections: {e}")
