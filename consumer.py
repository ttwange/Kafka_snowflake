from confluent_kafka import Consumer, KafkaError
import snowflake.connector
import os
import json
# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
group_id = 'snowflake-consumer'  # Consumer group ID
topic = 'postgres.public.emission'  # Kafka topic to consume from

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("snowflake_role"),
    password=os.getenv("snowflake_password"),
    account=os.getenv("snowflake_account"),
    database=os.getenv("snowflake_database"),
    schema=os.getenv("snowflake_schema")
)

# Create Kafka consumer instance
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest available offset
}
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

            # Prepare data (assuming JSON format for simplicity)
            data = json.loads(message.value().decode("utf-8"))

            # Execute SQL statements to insert data into Snowflake
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO your_table (column1, column2, ...)
                    VALUES (?, ?, ...)
                """, (data['column1'], data['column2'], ...))

                conn.commit()
                print("Data inserted successfully!")
            except snowflake.connector.errors.ProgrammingError as e:
                print(f"Error: {e}")
            finally:
                cursor.close()

except KeyboardInterrupt:
    # Terminate consumer on keyboard interrupt
    consumer.close()
    conn.close()
