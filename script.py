import requests
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

NETWORK_ID = "capital-bikeshare"
STATION_ID = "7a03f9cbe938f3be78aa94c699049942"

# --- Fetch ---
def fetch_station():
    url = f"http://api.citybik.es/v2/networks/{NETWORK_ID}"
    data = requests.get(url, timeout=30).json()["network"]["stations"]

    df = pd.DataFrame(data)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"].str.replace("Z", "", regex=False), utc=True
    )

    df = df.query("id == @STATION_ID").copy()

    df["capacity"] = df["free_bikes"] + df["empty_slots"]

    return df[[
        "id", "name", "timestamp",
        "free_bikes", "empty_slots", "capacity"
    ]]

# --- Google Sheets auth ---
def connect_sheets():
    creds = Credentials.from_service_account_info(
        eval(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

# --- Append ---
def append_data(df, sheet):
    df["timestamp"] = df["timestamp"].astype(str)
    sheet.append_rows(df.values.tolist())

# --- ENV VARS ---
import os
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]
SHEET_NAME = os.environ["SHEET_NAME"]

# --- Run ---
if __name__ == "__main__":
    df = fetch_station()
    sheet = connect_sheets()
    append_data(df, sheet)
