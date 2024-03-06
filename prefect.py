import prefect
from prefect import task, Flow
from sqlalchemy import create_engine
import snowflake.connector

# Step 3: Set up PostgreSQL Connection
POSTGRES_CONN_STR = "postgresql://username:password@host:port/database"

# Step 4: Set up Snowflake Connection
SNOWFLAKE_CONN_PARAMS = {
    "user": "username",
    "password": "password",
    "account": "account_name",
    "warehouse": "warehouse_name",
    "database": "database_name",
    "schema": "schema_name"
}

# Step 5: Define Tasks
@task
def extract_from_postgres():
    engine = create_engine(POSTGRES_CONN_STR)
    # Example SQL query to extract data from PostgreSQL
    query = "SELECT * FROM table_name"
    data = engine.execute(query)
    return data.fetchall()

@task
def load_to_snowflake(data):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN_PARAMS)
    cursor = conn.cursor()
    # Example Snowflake table to load data into
    snowflake_table = "TARGET_TABLE"
    for row in data:
        cursor.execute(f"INSERT INTO {snowflake_table} VALUES (%s, %s, %s)", row)
    conn.commit()
    cursor.close()
    conn.close()

# Step 6: Define Dependencies
with Flow("Postgres_to_Snowflake") as flow:
    data = extract_from_postgres()
    load_to_snowflake(data)

# Step 7: Run the Flow
flow.run()
