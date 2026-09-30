import base64
import json
import psycopg2
import time
from confluent_kafka import Producer
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pydantic import ValidationError
from datetime import datetime
from config import KAFKA_BOOTSTRAP_SERVERS, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
from models import Order

schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# with open("order_schema.avsc", "r") as f:
#     order_schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client
)

consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": avro_deserializer,
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe(["orders"])

dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

for attempt in range(1,6):
    try:
        conn = psycopg2.connect(
            host = POSTGRES_HOST,
            dbname= POSTGRES_DB,
            port = POSTGRES_PORT,
            user= POSTGRES_USER,
            password= POSTGRES_PASSWORD,
            options = "-c lock_timeout=3000"
        )
        cursor = conn.cursor()
        print("Connected to Postgres successfully")
        break
    except psycopg2.OperationalError as e:
        print(f"Postgres not ready (attempt {attempt}/5): {e}")
        time.sleep(3)
else:
    print("Could not connect to Postgres after 5 attempts")
    exit(1)

UPSERT_QUERY = """
INSERT INTO orders (order_id, user_name, item, quantity, discount)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (order_id)
DO UPDATE SET
    quantity = EXCLUDED.quantity,
    discount = EXCLUDED.discount,
    updated_at = now();
"""
def write_to_postgres(order):
    try:
        cursor.execute(UPSERT_QUERY, (
            str(order.order_id), order.user, order.item, order.quantity, order.discount
        ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"DB write failed... {e}")
        return False

def send_to_dlq(raw_order, error_msg, stage):
    dlq_payload = {
        "original_message": raw_order,
        "failure_stage": stage,
        "error_reason": error_msg,
        "failed_at": str(datetime.now())
    }
    dlq_producer.produce(topic="orders-dlq", value=json.dumps(dlq_payload).encode("utf-8"))
    dlq_producer.flush()

print("Consumer is running and subscribed to orders topic")
try:
    while True:
        try:
            msg = consumer.poll(1.0)
        except Exception as e:
            try:
                raw_bytes = e.kafka_message.value()
                raw_encoded = base64.b64encode(raw_bytes).decode("utf-8")
            except Exception as e:
                raw_encoded = "<raw bytes unavailable>"
            send_to_dlq(
                {"raw_bytes_base64": raw_encoded}, str(e),"schema_violation")
            consumer.commit()
            print(f"send to dlq (schema violation): {e}")
            continue
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        raw_order = msg.value()
        try:
            order = Order(**raw_order)
            print(f" orders received: {order}")
        except ValidationError as e:
            send_to_dlq(raw_order, str(e),"validation")
            consumer.commit()
            print(f"send to dlq (validation failed):{e}")
            continue

        success = write_to_postgres(order)
        if not success:
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                time.sleep(2)
                print(f"retrying db write {attempt}/{max_retries}")
                success = write_to_postgres(order)
                if success:
                    break
        if success:
            consumer.commit()
            print(f"Written to Postgres: {order.order_id}")
        else:
            send_to_dlq(raw_order,"DB write failed after 3 retries", "database write")
            consumer.commit()
            print(f"sent to dlq (db write failed):{order.order_id}")

except KeyboardInterrupt:
    print("Closing Consumer")
finally:
    cursor.close()
    conn.close()
    consumer.close()
