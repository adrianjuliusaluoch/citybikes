import time
import json
import sys
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

BASE = "https://missingvoices.or.ke"
SHEET_ID = "1VNm6eeQLhxNEa337PiDlvSRHCYGCf5lYHvnzHv2MQoI"
HEADERS = {"User-Agent": "Mozilla/5.0 (research scrape; personal analysis)"}


# --- Fetch ---
def fetch_with_retry(url, retries=4, base_backoff=5, timeout=30):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as error:
            last_error = error
            if attempt < retries:
                wait = base_backoff * (2 ** (attempt - 1))
                time.sleep(wait)
    raise last_error


def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        name_cell = cells[1]
        name_link = name_cell.find("a")
        name = name_link.get_text(strip=True) if name_link else name_cell.get_text(strip=True)
        href = name_link["href"] if name_link and name_link.get("href") else None
        profile_url = (BASE + href) if href and href.startswith("/") else href

        rows.append({
            "no": cells[0].get_text(strip=True),
            "name": name,
            "age": cells[2].get_text(strip=True),
            "sex": cells[3].get_text(strip=True),
            "location": cells[4].get_text(strip=True),
            "manner_of_death": cells[5].get_text(strip=True),
            "date_of_incident": cells[6].get_text(strip=True),
            "profile_url": profile_url,
        })
    return rows


def scrape_all(max_pages=200, delay=1.5):
    all_rows = []
    failed_pages = []
    for page in range(max_pages):
        url = f"{BASE}/voices?page={page}"
        try:
            html = fetch_with_retry(url)
        except Exception as error:
            failed_pages.append((page, str(error)))
            print(f"Error fetching page {page}: {error}")
            continue

        rows = parse_page(html)
        if not rows:
            print(f"No rows found on page {page} — assuming end of table.")
            break

        all_rows.extend(rows)
        print(f"Page {page}: {len(rows)} rows (total so far: {len(all_rows)})")
        time.sleep(delay)

    return all_rows, failed_pages


# --- Google Sheets ---
def connect_sheets():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


def append_new_only(df, sheet):
    if df.empty:
        return df

    df = df.astype(str)
    existing_header = sheet.row_values(1)
    new_header = df.columns.tolist()

    if not existing_header:
        sheet.append_row(new_header)
        existing_urls = set()
    elif existing_header != new_header:
        raise ValueError("Schema mismatch detected — stopping to prevent data loss")
    else:
        existing_urls = set(sheet.col_values(new_header.index("profile_url") + 1)[1:])

    new_rows = df[~df["profile_url"].isin(existing_urls)]
    if new_rows.empty:
        print("No new cases since last run.")
        return new_rows

    sheet.append_rows(new_rows.values.tolist())
    return new_rows


# --- Run ---
if __name__ == "__main__":
    rows, failed_pages = scrape_all()
    if not rows:
        print("No data fetched")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["collected_at"] = pd.Timestamp.now("Africa/Nairobi").isoformat()

    sheet = connect_sheets()
    appended_df = append_new_only(df, sheet)
    print(f"{len(appended_df)} new rows appended (scraped {len(df)} total)")

    if failed_pages:
        print(f"WARNING: {len(failed_pages)} page(s) failed after retries: {failed_pages}")
        sys.exit(1)
