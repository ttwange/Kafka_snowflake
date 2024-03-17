import os
import json
import logging
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prefect import flow, task
from datetime import datetime
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
        logging.error(f"Error connecting to Snowflake: {e}")
        raise e
@task
def extract_from_payload(json_data):
    data = json.loads(json_data)
    after_payload = data["payload"]["after"]
    return after_payload

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
        cursor.execute(snowflake_insert_sql, data)
        cursor.close()
        con.commit()
    except Exception as e:
        print(f"Error inserting data to Snowflake: {e}")

@flow(name="ETL_snowflake")
def snow_main():
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id=os.getenv("KAFKA_GROUP_ID"),
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    snowflake_conn = connect_to_snowflake()
    for message in consumer:
        try:
            payload = extract_from_payload(message.value)
            insert_data_to_snowflake(snowflake_conn, payload)

            consumer.commit()

        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    snow_main()
