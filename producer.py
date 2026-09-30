import uuid
import time
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from config import KAFKA_BOOTSTRAP_SERVERS
from tenacity import stop_after_attempt, retry, wait_exponential, retry_if_exception_type
import logging
import random

logging.basicConfig(
    filename='producer.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("producer")
logger.setLevel(logging.INFO)
logger.info("Starting producer...")

schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("order_schema.avsc", "r") as f:
    order_schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    order_schema_str
)

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
    "retries": 10,
    "retry.backoff.ms": 1000,
    "acks": "all",
    "enable.idempotence": True
}
producer = SerializingProducer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivery succeeded: {msg.key()}")

@retry(
    stop= stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2,  max=30),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True
)

def fetch_user_data():
    response = requests.get("https://randomuser.me/api/", timeout=5)
    response.raise_for_status()
    return response.json()

items = ["chicken bowl","veg wrap", "paneer tikka", "sushi roll", "burger"]

logger.info("Producer running ...")

try:
    while True:
        try:
            data = fetch_user_data()
            user = data["results"][0]["name"]["first"]
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if 400 <= status_code < 500:
                logger.critical(f"Request failed with status code {status_code}. URL: {e.request.url}. Stopping producer...")
                raise  SystemExit(1)
            else:
                logger.warning(f"server error {status_code}, retrying: {e}")
                time.sleep(5)
                continue
        except requests.RequestException as e:
            print(f"API call failed after retries {e}")
            time.sleep(5)
            continue
        order = {
                "order_id": str(uuid.uuid4()),
                "user": user,
                "item": items[uuid.uuid4().int % len(items)],
                "quantity": random.randint(1, 5),
                "discount": random.randint(1,5)
        }
        # value = json.dumps(order).encode("utf-8")
        producer.produce(topic="orders", value=order, key=user, on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(1)
        logger.info(f"order sent for user: {user}")
except KeyboardInterrupt:
    logger.info("producer stopped")
finally:
    producer.flush()
    logger.info("all pending messages flushed")