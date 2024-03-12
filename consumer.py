from confluent_kafka import Consumer, KafkaError

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'energy-consumer'  # Consumer group ID
topic = 'postgres.public.emission'  # Kafka topic to consume from

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest available offset
}

# Create Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe([topic])

try:
    while True:
        # Poll for messages
        message = consumer.poll(timeout=1.0)

        if message is None:
            continue
        elif message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, the consumer reached the end of the topic
                print(f'[{message.topic()}] Reached end of partition [{message.partition()}]')
            elif message.error():
                # Error occurred while consuming message
                print(f'Error: {message.error()}')
        else:
            # Message consumed successfully
            print(f'Received message: {message.value().decode("utf-8")}')

except KeyboardInterrupt:
    # Terminate consumer on keyboard interrupt
    consumer.close()
