import requests
import pandas as pd

def fetch_data():
    url = "https://api.energidataservice.dk/dataset/PowerSystemRightNow/"
    data = requests.get(url).json()
    return data['records']

# Call the function to fetch data

def transformation(json_data):
    df = pd.DataFrame(json_data)
    df = df.drop(columns=["aFRR_ActivatedDK1","aFRR_ActivatedDK2","mFRR_ActivatedDK1","mFRR_ActivatedDK2","ImbalanceDK1","ImbalanceDK2"])
    return df

json_data = fetch_data()
clean_data = transformation(json_data)

print(clean_data.head())

