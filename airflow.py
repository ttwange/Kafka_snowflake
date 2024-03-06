from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# Step 4: Set up PostgreSQL Connection
POSTGRES_CONN_STR = "postgresql://username:password@host:port/database"

# Step 6: Define Tasks
def extract_from_postgres():
    engine = create_engine(POSTGRES_CONN_STR)
    # Example SQL query to extract data from PostgreSQL
    query = "SELECT * FROM table_name"
    with engine.connect() as conn:
        data = conn.execute(query)
        return data.fetchall()
# Step 7: Define Dependencies
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 6),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    description='A DAG to move data from PostgreSQL to Snowflake',
    schedule_interval='@daily',  # Adjust as needed
)

extract_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task