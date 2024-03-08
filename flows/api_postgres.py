import os
import psycopg2
import requests
import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

@task(log_prints=True, retries=3)
def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

@task(log_prints=True,retries=3)
def transformation(json_data):
    df = pd.DataFrame(json_data)
    df = df.drop(columns=["aFRR_ActivatedDK1","aFRR_ActivatedDK2","mFRR_ActivatedDK1","mFRR_ActivatedDK2","ImbalanceDK1","ImbalanceDK2"])
    print(df.columns)
    return df

@task(log_prints=True,retries=3)
def load(clean_data,postgres_user,postgres_password,postgres_host,postgres_port,postgres_db):
    postgres_connection_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

    conn = psycopg2.connect(postgres_connection_string)
    cursor = conn.cursor()

    clean_data_columns = clean_data.columns.tolist()
    for _, row in clean_data.iterrows():
        cursor.execute(
            f"INSERT INTO emission ({','.join(clean_data_columns)}) VALUES ({','.join(['%s']*len(clean_data_columns))})",
            tuple(row)
        )

    conn.commit()
    print("Data loaded successfully.")

@flow(name="Energy ingest")
def main():
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_port = os.getenv("postgres_PORT")

    json_data = fetch_data()
    clean_data = transformation(json_data)
    load(clean_data,postgres_user,postgres_password,postgres_host,postgres_port,postgres_db)

if __name__ == "__main__":
    main()


