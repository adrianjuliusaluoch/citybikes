import httpx
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import time
import random
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
POSTS = [
    {
        "post_id": "1ug5jw6",
        "title_type": "descriptive",  # change to "storytelling" in week 2
    }
]

SHEET_ID = "12qqOxKf8286M7HkZw6k7754HMROUpyP9qcfYyn0sxH4"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────
# FETCH via old.reddit.com (no credentials needed)
# ─────────────────────────────────────────────
def fetch_post_metrics(post: dict) -> pd.DataFrame:
    url = f"https://old.reddit.com/r/dataisbeautiful/comments/{post['post_id']}/"

    response = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # old.reddit.com embeds score, ratio, and comment count as data attributes
    # on the main post's thing div
    thing = soup.find("div", attrs={"data-fullname": lambda v: v and v.startswith("t3_")})

    if not thing:
        raise ValueError(f"Could not find post data for {post['post_id']} — Reddit may have changed its markup")

    record = {
        "collected_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "post_id": post["post_id"],
        "title_type": post["title_type"],
        "upvotes": int(thing.get("data-score", 0)),
        "upvote_ratio": float(thing.get("data-upvote-ratio", 0)),
        "comments": int(thing.get("data-comments-count", 0)),
        "score": int(thing.get("data-score", 0)),
    }
    return pd.DataFrame([record])


# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


def append_data(df: pd.DataFrame, sheet):
    if df.empty:
        return
    df = df.astype(str)
    existing_header = sheet.row_values(1)
    new_header = df.columns.tolist()
    if not existing_header:
        sheet.append_row(new_header)
    elif existing_header != new_header:
        raise ValueError("Schema mismatch detected — stopping to prevent data loss")
    sheet.append_rows(df.values.tolist())


# ─────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    all_rows = []
    for i, post in enumerate(POSTS):
        # be polite — small random delay between requests when tracking multiple posts
        if i > 0:
            time.sleep(random.uniform(2, 5))
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
