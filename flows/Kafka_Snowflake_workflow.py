import os
import json
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prefect import flow, task

load_dotenv()

@flow(name="Kafka_Snowflake")
def snow_main():
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=os.getenv("snowflake_user"),
            password=os.getenv("snowflake_password"),
            account=os.getenv("snowflake_account"),
            warehouse=os.getenv("snowflake_warehouse"),
            database=os.getenv("snowflake_database"),
            schema=os.getenv("snowflake_schema")
        )
        cursor = conn.cursor()

        # Kafka Consumer Configuration
        consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id=os.getenv("KAFKA_GROUP_ID"),
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Insert data into Snowflake
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

        for message in consumer:
            try:
                cursor.execute(snowflake_insert_sql, message.value)
                conn.commit()
                print("Data inserted into Snowflake")
            except Exception as e:
                print(f"Error inserting data to Snowflake: {e}")
                conn.rollback()
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")



if __name__ == "__main__":
    snow_main()
