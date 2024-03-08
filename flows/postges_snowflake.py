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

# Create a cursor from the connection
cursor = snowflake_conn.cursor(DictCursor)

# Execute a simple SQL query
try:
    cursor.execute("SELECT current_version()")
    # Fetch the result
    result = cursor.fetchone()
    print("Snowflake version: ", result["current_version()"])
    print("Connection is working!")
except Exception as e:
    print("Failed to connect to Snowflake: ", e)

# Close the cursor and connection
cursor.close()
snowflake_conn.close()