import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
import pandas as pd
from io import BytesIO
from google.cloud import storage
from prefect import flow, task
import os 

load_dotenv()

@task
def consume_messages():
    try:
        consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id=os.getenv("KAFKA_GROUP_ID"),
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        data = []
        for message in consumer:
            data.append(message.value)
        return data
    except Exception as e:
        print(f"Error consuming messages from Kafka: {e}")

@task
def convert_to_parquet(data):
    try:
        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        return buffer
    except Exception as e:
        print(f"Error converting to Parquet: {e}")

@task
def load_to_gcs(buffer):
    try:
        client = storage.Client()
        bucket = client.get_bucket(os.getenv("GCS_BUCKET_NAME"))
        blob = bucket.blob("data.parquet")
        blob.upload_from_file(buffer, content_type="application/octet-stream")
    except Exception as e:
        print(f"Error loading to GCS: {e}")


@flow(name="kafka_GCS")
def main():
    kafka_messages = consume_messages()
    parquet_buffer = convert_to_parquet(kafka_messages)
    load_to_gcs(parquet_buffer)

if __name__ == "__main__":
    main()