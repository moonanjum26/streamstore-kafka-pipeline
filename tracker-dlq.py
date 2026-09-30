import json
from confluent_kafka import Consumer
import boto3
from datetime import datetime
from config import  KAFKA_BOOTSTRAP_SERVERS, S3_BUCKET_NAME

consumer_dlq_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "order-dlq-tracker",
    "auto.offset.reset": "earliest"
}
consumer_dlq = Consumer(consumer_dlq_config)
consumer_dlq.subscribe(["orders-dlq"])

s3_client = boto3.client("s3")
# s3_client.create_bucket(Bucket="dlq-tracker")

print("DLQ consumer running...")

try:
    while True:
        msg = consumer_dlq.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        value = msg.value().decode("utf-8")
        dlq_value = json.loads(value)
        order_id = dlq_value["original_message"].get("order_id", "unknown")
        file_key = f"dlq/{datetime.now().strftime('%Y/%m/%d')}/{dlq_value['failure_stage']}_{order_id}_{datetime.now().strftime('%H%M%S%f')}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(dlq_value).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"written to s3: {file_key}")
except KeyboardInterrupt:
    print("Closing s3")
finally:
    consumer_dlq.close()
