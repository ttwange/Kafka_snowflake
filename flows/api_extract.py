import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

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
    engine = create_engine(f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}")
    engine.connect()
    print(pd.io.sql.get_schema(clean_data, name='emission', con=engine))
    #clean_data.to_sql(name='emis', con=engine, if_exists="append")
    # print("Data loaded successfully")

json_data = fetch_data()
clean_data = transformation(json_data)
load(clean_data)


