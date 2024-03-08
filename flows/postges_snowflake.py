import snowflake.connector
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

try:
    # Connect to Snowflake using environment variables
    snowflake_conn = snowflake.connector.connect(
        user=os.getenv("snowflake_user"),
        password=os.getenv("snowflake_password"),
        account=os.getenv("snowflake_account"),
        warehouse=os.getenv("snowflake_warehouse"),
        database=os.getenv("snowflake_database"),
        schema=os.getenv("snowflake_schema")
    )

    # Connect to PostgreSQL using environment variables
    postgres_conn = psycopg2.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB")
    )

    # Fetch data from PostgreSQL
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute(f"SELECT * FROM {os.getenv('POSTGRES_TABLE')}")
    data = postgres_cursor.fetchall()

   # Snowflake insert statement
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
    snowflake_cursor = snowflake_conn.cursor()
    snowflake_cursor.executemany(snowflake_insert_sql, data)

    # Commit the transaction
    snowflake_conn.commit()

except Exception as e:
    print("Error:", e)

finally:
    # Close the cursors and connections
    if 'snowflake_cursor' in locals():
        snowflake_cursor.close()
    if 'snowflake_conn' in locals():
        snowflake_conn.close()
    if 'postgres_cursor' in locals():
        postgres_cursor.close()
    if 'postgres_conn' in locals():
        postgres_conn.close()
