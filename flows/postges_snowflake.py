import os
import psycopg2
import snowflake.connector
from dotenv import load_dotenv
from prefect import flow, task

#load enviroment variables 
load_dotenv

@task
def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER")
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        raise RuntimeError("Error connecting to snowflake") from e

@task
def connect_to_postgres():
    try:
        engine = psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB")
        )
        return engine
    except Exception as e:
        raise RuntimeError("Error connecting to PostgreSQL") from e
    
@task
def fetch_data_from_postgres(engine):
    try:
        cursor = engine.cursor()
        cursor.execute(f"SELECT * FROM {os.getenv('POSTGRES_TABLE')}")
        data = cursor.fetchall()
        cursor.close()
        return data
    except Exception as e:
        raise RuntimeError("Error fetching data from PostgresQL") from e
    