import os
import json
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prefect import flow, task

load_dotenv()

@task
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("snowflake_user"),
            password=os.getenv("snowflake_password"),
            account=os.getenv("snowflake_account"),
            warehouse=os.getenv("snowflake_warehouse"),
            database=os.getenv("snowflake_database"),
            schema=os.getenv("snowflake_schema")
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

@task
def consume_messages():
    try:
        consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id=os.getenv("KAFKA_GROUP_ID"),
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            yield message.value
    except Exception as e:
        print(f"Error consuming messages from Kafka: {e}")

@task
def insert_data_to_snowflake(con, data):
    try:
        snowflake_insert_sql = """
            INSERT INTO emission (
            Minutes1UTC, Minutes1DK, CO2Emission, ProductionGe100MW,
            ProductionLt100MW, SolarPower, OffshoreWindPower,
            OnshoreWindPower, Exchange_Sum, Exchange_DK1_DE,
            Exchange_DK1_NL, Exchange_DK1_GB, Exchange_DK1_NO,
            Exchange_DK1_SE, Exchange_DK1_DK2, Exchange_DK2_DE,
            Exchange_DK2_SE, Exchange_Bornholm_SE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Create a cursor from the Snowflake connection
        cursor = con.cursor()
        cursor.executemany(snowflake_insert_sql, data)
        cursor.close()
        con.commit()
    except Exception as e:
        print(f"Error inserting data to Snowflake: {e}")

# Define the Prefect flow
@flow(name="Kafka_snowflake")
def main():
    snowflake_conn = connect_to_snowflake()
    kafka_messages = consume_messages()
    insert_data_to_snowflake(snowflake_conn, kafka_messages)

if __name__ == "__main__":
    main()
