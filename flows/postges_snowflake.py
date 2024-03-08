import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv
load_dotenv()
import os

# Connect to Snowflake using the connection options from environment variables
snowflake_conn = snowflake.connector.connect(
    user=os.getenv("snowflake_user"),
    password=os.getenv("snowflake_password"),
    account=os.getenv("snowflake_account"),
    warehouse=os.getenv("snowflake_warehouse"),
    database=os.getenv("snowflake_database"),
    schema=os.getenv("snowflake_schema")
)
# connection to postgres
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("postgres_PORT")
postgres_table = os.getenv("postgres_TABLE")

postgres_conn = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

# Fetch data from PostgreSQL
postgres_cursor = postgres_conn.cursor()
postgres_cursor.execute(f"SELECT * FROM {postgres_table}")
data = postgres_cursor.fetchall()


# Create a cursor from the connection
cursor = snowflake_conn.cursor(DictCursor)

# Execute a simple SQL query
try:
    cursor.execute("SELECT current_version()")
    # Fetch the result
    result = cursor.fetchone()
    print("Connection is working!")
except Exception as e:
    print( e)

# Close the cursor and connection
cursor.close()
snowflake_conn.close()