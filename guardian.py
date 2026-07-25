import time
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime, timedelta

TOWNS = ["Yala", "Kisumu", "Kakamega", "Busia"]
SHEET_ID = "1JCePIF30s2MxeUDDvgyK4aSGerGarKE4AuG7cnn1MS8"

ROUTE_PAIRS = []
for t in TOWNS:
    ROUTE_PAIRS.append(("Nairobi", t))
    ROUTE_PAIRS.append((t, "Nairobi"))

# --- Fetch ---
def fetch_guardian_routes(route_pairs: list, travel_date: str) -> pd.DataFrame:
    endpoint = "https://theguardian.co.ke/api/search-buses"
    rows = []

    for origin, dest in route_pairs:
        payload = {"fromCity": origin, "toCity": dest, "travelDate": travel_date}
        try:
            response = requests.post(
                endpoint, json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                for trip in data:
                    trip["fromCity"], trip["toCity"], trip["queryDate"] = origin, dest, travel_date
                    rows.append(trip)
        except Exception as error:
            print(f"Error fetching {origin} -> {dest}: {error}")
        time.sleep(1)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # stringify nested fields back into the same column, no new variables
    for col in ["fareInfo", "template", "countries"]:
        if col in df.columns:
            df[col] = df[col].apply(json.dumps)

    df["collected_at"] = pd.Timestamp.now("UTC")
    return df

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
    df = df.drop_duplicates(subset=["bookingRef", "toCity", "collected_at"])
    df = df.astype(str)

    existing_header = sheet.row_values(1)
    new_header = df.columns.tolist()

    if not existing_header:
        sheet.append_row(new_header)
    elif existing_header != new_header:
        raise ValueError("Schema mismatch detected — stopping to prevent data loss")

    sheet.append_rows(df.values.tolist())

# --- Run ---
if __name__ == "__main__":
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT08:00:00.000Z")

    df = fetch_guardian_routes(ROUTE_PAIRS, tomorrow)

    if df.empty:
        print("No data fetched")
        exit()

    sheet = connect_sheets()
    append_data(df, sheet)
    print(f"{len(df)} rows appended")
