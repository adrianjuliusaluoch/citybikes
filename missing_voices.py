import asyncio
import time
import json
import sys
import os
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from tqdm.asyncio import tqdm

BASE = "https://missingvoices.or.ke"
SHEET_ID = "1VNm6eeQLhxNEa337PiDlvSRHCYGCf5lYHvnzHv2MQoI"
HEADERS = {"User-Agent": "Mozilla/5.0 (research scrape; personal analysis)"}
CONCURRENT_REQUESTS = 5
MAX_PAGE_SEARCH = 200  # upper bound for binary search


# --- Fetch ---
async def safe_get(session, url, retries=4, base_backoff=5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=HEADERS) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as error:
            last_error = error
            if attempt < retries:
                await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
    print(f"Giving up on {url}: {last_error}")
    return None


def parse_page(html):
    if not html:
        return []
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


async def page_has_rows(session, page):
    html = await safe_get(session, f"{BASE}/voices?page={page}")
    return len(parse_page(html)) > 0


async def detect_last_page(session, max_pages=MAX_PAGE_SEARCH):
    # page 0 is guaranteed to have rows; binary search for the last page that does
    low, high = 0, max_pages
    while low < high:
        mid = (low + high + 1) // 2
        print(f"checking page {mid} (range {low}-{high})")
        if await page_has_rows(session, mid):
            low = mid
        else:
            high = mid - 1
    print(f"Detected last page: {low}")
    return low


async def scrape_page_with_progress(session, page, semaphore):
    async with semaphore:
        html = await safe_get(session, f"{BASE}/voices?page={page}")
        return parse_page(html)


async def scrape_all():
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        last_page = await detect_last_page(session)

        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [
            asyncio.create_task(scrape_page_with_progress(session, page, semaphore))
            for page in range(0, last_page + 1)
        ]

        all_rows = []
        for coro in tqdm.as_completed(tasks, total=len(tasks), desc="Scraping pages"):
            rows = await coro
            all_rows.extend(rows)

        return all_rows


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
    rows = asyncio.run(scrape_all())
    if not rows:
        print("No data fetched")
        sys.exit(1)

    df = pd.DataFrame(rows)
    # dedupe in case two concurrent pages returned overlapping rows during a race
    df = df.drop_duplicates()
    df["collected_at"] = pd.Timestamp.now("Africa/Nairobi").isoformat()

    sheet = connect_sheets()
    appended_df = append_new_only(df, sheet)
    print(f"{len(appended_df)} new rows appended (scraped {len(df)} total)")
