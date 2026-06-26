import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone

POSTS = [
    {
        "post_id": "1ug5jw6",
        "title_type": "descriptive",  # change to "storytelling" in week 2
    }
]

SHEET_ID = "12qqOxKf8286M7HkZw6k7754HMROUpyP9qcfYyn0sxH4"

# --- Fetch ---
def fetch_post_metrics(post: dict) -> pd.DataFrame:
    url = f"https://www.reddit.com/r/dataisbeautiful/comments/{post['post_id']}/.json"
    response = requests.get(
        url,
        headers={"User-Agent": "reddit-ab-tracker/1.0 by datanerdke"},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    post_data = data[0]["data"]["children"][0]["data"]
    record = {
        "collected_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "post_id": post["post_id"],
        "title_type": post["title_type"],
        "upvotes": post_data["ups"],
        "upvote_ratio": post_data["upvote_ratio"],
        "comments": post_data["num_comments"],
        "score": post_data["score"]
    }
    return pd.DataFrame([record])

# --- Google Sheets ---
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

# --- Append ---
def append_data(df: pd.DataFrame, sheet):
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
    all_rows = []
    for post in POSTS:
        try:
            df = fetch_post_metrics(post)
            all_rows.append(df)
            print(f"Fetched {post['title_type']} post {post['post_id']}: {df.to_dict('records')}")
        except Exception as error:
            print(f"Error fetching {post['post_id']}: {error}")
    if not all_rows:
        print("No data fetched")
        exit()
    df = pd.concat(all_rows, ignore_index=True)
    sheet = connect_sheets()
    append_data(df, sheet)
