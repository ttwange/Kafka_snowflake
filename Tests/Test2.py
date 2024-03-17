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
        raise e
    
@task
def insert_data_to_snowflake(con):
    consumer = KafkaConsumer(
            os.getenv("KAFKA_TOPIC"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            group_id=os.getenv("KAFKA_GROUP_ID"),
            auto_offset_reset='earliest'
        )
    
    try:
        cursor = con.cursor()
        for message in consumer:
            message_value = json.loads(message.value.decode('utf-8'))
            data = message_value.get('payload', {}).get('after', None)
            
            if data:
                snowflake_insert_sql = """
                    INSERT INTO emission (
                    Minutes1UTC, Minutes1DK, CO2Emission, ProductionGe100MW,
                    ProductionLt100MW, SolarPower, OffshoreWindPower,
                    OnshoreWindPower, Exchange_Sum, Exchange_DK1_DE,
                    Exchange_DK1_NL, Exchange_DK1_GB, Exchange_DK1_NO,
                    Exchange_DK1_SE, Exchange_DK1_DK2, Exchange_DK2_SE,
                    Exchange_Bornholm_SE
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(snowflake_insert_sql, (
                    data['Minutes1UTC'],
                    data['Minutes1DK'], 
                    data['CO2Emission'], 
                    data['ProductionGe100MW'],
                    data['ProductionLt100MW'],
                    data['SolarPower'],
                    data['OffshoreWindPower'],
                    data['OnshoreWindPower'],
                    data['Exchange_Sum'],
                    data['Exchange_DK1_DE'],
                    data['Exchange_DK1_NL'],
                    data['Exchange_DK1_GB'],
                    data['Exchange_DK1_NO'],
                    data['Exchange_DK1_SE'],
                    data['Exchange_DK1_DK2'],
                    data['Exchange_DK2_SE'],
                    data['Exchange_Bornholm_SE']
                ))
                con.commit()
        print("Data inserted into Snowflake successfully!!")
    except Exception as e:
        print(f"Error inserting data to Snowflake: {e}")

@flow(name="ETL snowflake")
def main():
    snowflake_conn = connect_to_snowflake()
    insert_data_to_snowflake(snowflake_conn)

if __name__ == "__main__":
    main()
