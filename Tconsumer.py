from kafka import KafkaConsumer
import json
import os

def extract_after_payload(json_data):
    # Parse JSON
    data = json.loads(json_data)
    
    # Access the "after" field within the payload
    after_payload = data["payload"]["after"]
    
    return after_payload

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    group_id=os.getenv("KAFKA_GROUP_ID"),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

message_count = 0

try:
    # Consume messages from the Kafka topic
    for message in consumer:
        after_payload = extract_after_payload(message.value)
        print(after_payload)
        message_count += 1
        if message_count >= 1:
            break
except KeyboardInterrupt:
    # Handle keyboard interrupt to gracefully close the consumer
    consumer.close()
