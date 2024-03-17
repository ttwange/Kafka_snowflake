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
def extract_from_payload(json_data):
    data = json.loads(json_data)
    after_payload = data["payload"]["after"]
    return after_payload

@task
def insert_data_to_snowflake(con, message_value):
    try:
        cursor = con.cursor()
        data = json.loads(message_value)
        after_data = data.get('after', {})
        if not after_data:
            logging.warning("No 'after' data found in the message.")
            return

        snowflake_insert_sql = """
            INSERT INTO emission (
            Minutes1UTC, Minutes1DK, CO2Emission, ProductionGe100MW,
            ProductionLt100MW, SolarPower, OffshoreWindPower,
            OnshoreWindPower, Exchange_Sum, Exchange_DK1_DE,
            Exchange_DK1_NL, Exchange_DK1_GB, Exchange_DK1_NO,
            Exchange_DK1_SE, Exchange_DK1_DK2, Exchange_DK2_DE,
            Exchange_DK2_SE, Exchange_Bornholm_SE
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(snowflake_insert_sql, (
            after_data.get('minutes1utc'),
            after_data.get('minutes1dk'),
            after_data.get('co2emission'),
            after_data.get('productionge100mw'),
            after_data.get('productionlt100mw'),
            after_data.get('solarpower'),
            after_data.get('offshorewindpower'),
            after_data.get('onshorewindpower'),
            after_data.get('exchange_sum'),
            after_data.get('exchange_dk1_de'),
            after_data.get('exchange_dk1_nl'),
            after_data.get('exchange_dk1_gb'),
            after_data.get('exchange_dk1_no'),
            after_data.get('exchange_dk1_se'),
            after_data.get('exchange_dk1_dk2'),
            after_data.get('exchange_dk2_de'),
            after_data.get('exchange_dk2_se'),
            after_data.get('exchange_bornholm_se')
        ))
        con.commit()
        logging.info("Data inserted into Snowflake successfully!!")
    except json.JSONDecodeError as je:
        logging.error(f"Error parsing JSON: {je}")
    except Exception as e:
        logging.error(f"Error inserting data to Snowflake: {e}")

@flow(name="ETL_snowflake")
def main():
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
            after_payload = extract_from_payload(message.value)
            insert_data_to_snowflake(snowflake_conn, after_payload)

            consumer.commit()

        except Exception as e:
            logging.error(f"Error processing message: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
