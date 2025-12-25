import os
import json
from datetime import datetime

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


# -------------------------------
# Kafka configuration
# -------------------------------
kafka_config = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OVGUUNL6HJN7OOOA',
    'sasl.password': 'cfltMhFSGcRPlSduCIqHenF3vFB7d4aCgVbqsPKISiFh9NOw/vxRJgb2AphZ6CRQ',
    'group.id': 'group31',
    'auto.offset.reset': 'latest'
}


# -------------------------------
# Schema Registry configuration
# -------------------------------
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-193737w.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('POSWZTAGE2NRBMIH', 'cfltL/d3XFqNVwaYboBxXJd2W3EA27kYs1ARJztcJEs2SClk9h+TzbeZFrGaxzZw')
})


# Fetch latest Avro schema
subject_name = 'product_updates-value-value'
schema_str = schema_registry_client.get_latest_version(
    subject_name
).schema.schema_str


# -------------------------------
# Deserializers
# -------------------------------
key_deserializer = StringDeserializer('utf_8')
value_deserializer = AvroDeserializer(schema_registry_client, schema_str)


# -------------------------------
# Kafka Consumer
# -------------------------------
consumer = DeserializingConsumer({
    **kafka_config,
    'key.deserializer': key_deserializer,
    'value.deserializer': value_deserializer
})


# -------------------------------
# Helper functions
# -------------------------------
def datetime_encoder(obj):
    """Convert datetime objects to ISO format for JSON."""
    if isinstance(obj, datetime):
        return obj.isoformat()

def write_to_json_file(json_string: str, file_path: str):
    """Append JSON string to a file."""
    with open(file_path, 'a', encoding='utf-8') as file:
        file.write(json_string + '\n')

# -------------------------------
# UNIQUE FILE PER CONSUMER
# -------------------------------
consumer_pid = os.getpid()
file_path = f'consumer_{consumer_pid}.json'

print(f"Consumer PID {consumer_pid} writing to {file_path}")




# -------------------------------
# Subscribe to topic
# -------------------------------
consumer.subscribe(['product_updates-value'])


# -------------------------------
# Consume messages
# -------------------------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value()

        # Convert category to lowercase
        value['category'] = value['category'].lower()

        # Apply discount for category a
        if value['category'] == 'category a':
            value['price'] = round(value['price'] * 0.5, 2)

        print(
            # f"Successfully consumed record "
            f"Consumed by PID {consumer_pid} | "
            f"Key={msg.key()} | Value={value}"
            # f"with key {msg.key()} and value {value}"
        )

        json_string = json.dumps(value, default=datetime_encoder)
        
        with open(file_path, 'a', encoding='utf-8') as file:
            file.write(json_string + '\n')

except KeyboardInterrupt:
    print("Consumer interrupted by user.")

finally:
    consumer.close()
    print("Kafka consumer closed.")


