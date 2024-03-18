import os
import json
import logging
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
        logging.error(f"Error connecting to Snowflake: {e}")
        raise e

@task
def consume_kafka():
    data = []
    consumer = KafkaConsumer(
        "postgres.public.emission",
        bootstrap_servers="localhost:9092",
        group_id="snowflake",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        try:
            payload = message.value
            after_payload = payload.get("payload", {}).get("after")
            if after_payload:
                data.append(after_payload)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
    return data

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

        # Preprocess data to match Snowflake insert format
        prepared_data = [(item['Minutes1UTC'], item['Minutes1DK'], item['CO2Emission'], 
                          item['ProductionGe100MW'], item['ProductionLt100MW'], item['SolarPower'], 
                          item['OffshoreWindPower'], item['OnshoreWindPower'], item['Exchange_Sum'], 
                          item['Exchange_DK1_DE'], item['Exchange_DK1_NL'], item['Exchange_DK1_GB'], 
                          item['Exchange_DK1_NO'], item['Exchange_DK1_SE'], item['Exchange_DK1_DK2'], 
                          item['Exchange_DK2_DE'], item['Exchange_DK2_SE'], item['Exchange_Bornholm_SE']) 
                         for item in data]

        # Create a cursor from the Snowflake connection
        cursor = con.cursor()
        cursor.executemany(snowflake_insert_sql, prepared_data)
        cursor.close()
        con.commit()
    except Exception as e:
        logging.error(f"Error inserting data to Snowflake: {e}")

@flow(name="ETL_snowflake")
def main():
    snowflake_conn = connect_to_snowflake()
    payload = consume_kafka()
    logging.info(f"Received {len(payload)} records from Kafka.")
    insert_data_to_snowflake(snowflake_conn, payload)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main().run()
