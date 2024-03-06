import requests
import pandas as pd

def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

# Call the function to fetch data
json_data = fetch_data()
if json_data:
    # Convert JSON data to DataFrame
    df = pd.DataFrame(json_data)
    #print(df.head())  # Display the first few rows of the DataFrame
else:
    print("No data fetched.")

print(df.isna().sum())

def transformation(df: int) -> int:
    df.drop(columns=["aFRR_ActivatedDK1","aFRR_ActivatedDK2"])
    return df
