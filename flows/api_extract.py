import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine


# Load environment variables from .env file
load_dotenv()

# Get PostgreSQL connection parameters from environment variables
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("postgres_PORT")


def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

def transformation(json_data):
    df = pd.DataFrame(json_data)
    df = df.drop(columns=["aFRR_ActivatedDK1","aFRR_ActivatedDK2","mFRR_ActivatedDK1","mFRR_ActivatedDK2","ImbalanceDK1","ImbalanceDK2"])
    return df

def load(clean_data):
    postgres_connection_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

    conn = psycopg2.connect(postgres_connection_string)
    cursor = conn.cursor()

    # Insert dataframe into PostgreSQL table
    clean_data_columns = clean_data.columns.tolist()
    for _, row in clean_data.iterrows():
        cursor.execute(
            f"INSERT INTO emission ({','.join(clean_data_columns)}) VALUES ({','.join(['%s']*len(clean_data_columns))})",
            tuple(row)
        )

    # Commit the transaction
    conn.commit()
    print("Data loaded successfully.")

json_data = fetch_data()
clean_data = transformation(json_data)
load(clean_data)


