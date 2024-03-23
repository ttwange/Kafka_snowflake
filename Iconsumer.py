from kafka import Consumer, KafkaException
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'broker:29092',
    'group.id': 'influx',
    'auto.offset.reset': 'earliest'
}

# InfluxDB configuration
influxdb_client = InfluxDBClient(url="http://localhost:8086", token="my_token", org="my_org")
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

def process_message(msg):
    try:
        value = msg.value().decode('utf-8')  # Assuming data is in UTF-8 format
        # Parse your message and extract relevant fields
        # Assuming the message format is <timestamp>,<field1>,<field2>,...
        parts = value.split(',')
        timestamp = int(parts[0])
        field1 = float(parts[1])
        field2 = float(parts[2])

        # Create an InfluxDB Point and write it to the database
        point = Point("measurement_name").time(timestamp).field("field1", field1).field("field2", field2)
        write_api.write(bucket="my_bucket", record=point)

    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    consumer = Consumer(conf)
    consumer.subscribe(['my_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, ignore
                    continue
                else:
                    raise KafkaException(msg.error())
            process_message(msg)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
