import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import mysql.connector


def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"✅ Record {msg.key()} successfully produced to "
            f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def main():
    # ---------------- Kafka Config ----------------
    kafka_config = {
        'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'OVGUUNL6HJN7OOOA',
        'sasl.password': 'cfltMhFSGcRPlSduCIqHenF3vFB7d4aCgVbqsPKISiFh9NOw/vxRJgb2AphZ6CRQ'
    }

    # ---------------- Schema Registry ----------------
    schema_registry_client = SchemaRegistryClient({
        'url': 'https://psrc-193737w.us-east1.gcp.confluent.cloud',
        'basic.auth.user.info': '{}:{}'.format('POSWZTAGE2NRBMIH', 'cfltL/d3XFqNVwaYboBxXJd2W3EA27kYs1ARJztcJEs2SClk9h+TzbeZFrGaxzZw')
    })

    subject_name = 'product_updates-value-value'
    schema_str = schema_registry_client \
        .get_latest_version(subject_name) \
        .schema.schema_str

    key_serializer = StringSerializer("utf_8")
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer = SerializingProducer({
        **kafka_config,
        'key.serializer': key_serializer,
        'value.serializer': avro_serializer
    })

    # ---------------- MySQL Connection ----------------
    connection = mysql.connector.connect(
        host='127.0.0.1',
        user='Kafka',
        password='kafka12',
        database='Kafka'
    )
    cursor = connection.cursor()

    # ---------------- Load config.json ----------------
    config_data = {}
    last_read_timestamp = "1900-01-01 00:00:00"

    try:
        with open("config.json", "r") as f:
            config_data = json.load(f)
            last_read_timestamp = config_data.get(
                "last_read_timestamp", last_read_timestamp
            )
    except FileNotFoundError:
        pass

    # ---------------- Fetch Incremental Data ----------------
    query = f"""
        SELECT *
        FROM products
        WHERE last_updated > '{last_read_timestamp}'
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        print("ℹ️ No new rows to fetch.")
    else:
        columns = [col[0] for col in cursor.description]

        for row in rows:
            value = dict(zip(columns, row))
            producer.produce(
                topic='product_updates-value',
                key=str(value['ID']),
                value=value,
                on_delivery=delivery_report
            )

        producer.flush()

    # ---------------- Update last_read_timestamp ----------------
    cursor.execute("SELECT MAX(last_updated) FROM products")
    max_date = cursor.fetchone()[0]

    if max_date:
        config_data["last_read_timestamp"] = max_date.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with open("config.json", "w") as f:
            json.dump(config_data, f)

    # ---------------- Cleanup ----------------
    cursor.close()
    connection.close()

    print("🚀 Data successfully published to Kafka")


if __name__ == "__main__":
    main()


