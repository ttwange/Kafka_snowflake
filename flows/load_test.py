import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text, Float
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get PostgreSQL connection parameters from environment variables
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_db = os.getenv("POSTGRES_DB")
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")

# Construct PostgreSQL connection string
postgres_connection_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

# Define the table schema
metadata = MetaData()
emission_table = Table(
    'n_emission',
    metadata,
    Column('Minutes1UTC', Text),
    Column('Minutes1DK', Text),
    Column('CO2Emission', Float),
    Column('ProductionGe100MW', Float),
    Column('ProductionLt100MW', Float),
    Column('SolarPower', Float),
    Column('OffshoreWindPower', Float),
    Column('OnshoreWindPower', Float),
    Column('Exchange_Sum', Float),
    Column('Exchange_DK1_DE', Float),
    Column('Exchange_DK1_NL', Float),
    Column('Exchange_DK1_GB', Float),
    Column('Exchange_DK1_NO', Float),
    Column('Exchange_DK1_SE', Float),
    Column('Exchange_DK1_DK2', Float),
    Column('Exchange_DK2_DE', Float),
    Column('Exchange_DK2_SE', Float),
    Column('Exchange_Bornholm_SE', Float)
)

# Create the engine
engine = create_engine(postgres_connection_string)

try:
    # Create the table
    metadata.create_all(engine)
    print("Table 'emission' created successfully.")

except Exception as e:
    print(f"Error creating table: {e}")
