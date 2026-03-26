import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

NETWORK_ID = "capital-bikeshare"
STATION_ID = "7a03f9cbe938f3be78aa94c699049942"

SHEET_ID = "1SPAN_YK6V8vxS9BjZxiD9-8ryGIEpyAXbrkjB6N_bQc"


# --- Fetch ---
def fetch_citybikes_network(network_id: str) -> pd.DataFrame:
    endpoint = f"http://api.citybik.es/v2/networks/{network_id}"
    
    response = requests.get(endpoint, timeout=30)
    response.raise_for_status()
    
    payload = response.json()
    station_records = payload["network"]["stations"]

    if not station_records:
        return pd.DataFrame()

    stations_table = pd.DataFrame(station_records)

    # timestamp
    stations_table["timestamp"] = pd.to_datetime(
        stations_table["timestamp"].str.replace("Z", "", regex=False),
        utc=True
    )

    # expand extra
    if "extra" in stations_table.columns:
        extra_expanded = pd.json_normalize(stations_table["extra"])
        stations_table = pd.concat(
            [stations_table.drop(columns=["extra"]), extra_expanded],
            axis=1
        )

    # flatten rental_uris
    if "rental_uris.android" in stations_table.columns:
        stations_table.rename(columns={
            "rental_uris.android": "rental_android",
            "rental_uris.ios": "rental_ios"
        }, inplace=True)

    # network id
    stations_table["network_id"] = network_id

    # capacity
    if {"free_bikes", "empty_slots"}.issubset(stations_table.columns):
        stations_table["capacity"] = (
            stations_table["free_bikes"] + stations_table["empty_slots"]
        )

    # remove last_updated (you chose timestamp instead)
    if "last_updated" in stations_table.columns:
        stations_table.drop(columns=["last_updated"], inplace=True)

    # add collected_at (your true time index)
    stations_table["collected_at"] = pd.Timestamp.utcnow()

    return stations_table


# --- Google Sheets ---
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


# --- Append ---
def append_data(df, sheet):
    if df.empty:
        return

    df = df.query("id == @STATION_ID").copy()
    if df.empty:
        return

    df = df.astype(str)

    existing_header = sheet.row_values(1)
    new_header = df.columns.tolist()

    # first run → set header
    if not existing_header:
        sheet.append_row(new_header)

    # prevent silent corruption
    elif existing_header != new_header:
        raise ValueError("Schema mismatch detected — stopping to prevent data loss")

    sheet.append_rows(df.values.tolist())


# --- Run ---
if __name__ == "__main__":
    df = fetch_citybikes_network(NETWORK_ID)
    sheet = connect_sheets()
    append_data(df, sheet)
