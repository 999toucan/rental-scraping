#!/usr/bin/env python3
import os
import csv
import time
import sys
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
import craigslistscraper as cs
from datetime import datetime
from helpers.gcp_utils import GCSHelper
from helpers.craigslist_helpers import (
    is_page_removed, 
    is_page_throttled,
    save_scraped_url,
    extract_posting_info,
    post_id_from_url,
    get_row_from_soup,
    add_date_to_filename,
    save_local_csv,
    upload_to_gcs
)



CITY = "vancouver"
CATEGORIES = ["apa", "roo", "sub"]
QUERY = ""  # or "rental"

BASE_SLEEP = 1.2
JITTER = 0.9

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

FIELDNAMES = ["category", "id", "title", "url"]
cols = ["post_id", "posted", "updated", "price", "rent_period", "title", "description", "attribute", "region", "region_code", "GPS_coordinate", "url", "pictures"]

def safe_get(d, *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def load_seen(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def append_seen(path: str, url: str):
    with open(path, "a", encoding="utf-8") as f:
        f.write(url + "\n")
        f.flush()


def ensure_csv_header(path: str, fieldnames: list[str]):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def fetch_ad_with_retries(ad, headers, max_tries=6, timeout=25):
    for attempt in range(1, max_tries + 1):
        try:
            status = ad.fetch(headers=headers, timeout=timeout)
            return status
        except requests.exceptions.RequestException as e:
            sleep = min(30, (2 ** (attempt - 1)) + random.random() * 2)
            print(f"ad.fetch error attempt {attempt}/{max_tries}: {e} (sleep {sleep:.1f}s)")
            time.sleep(sleep)
    return None


def main(OUT_CSV, SEEN_FILE, PREVIOUSLY_SCRAPED):
    ensure_csv_header(OUT_CSV, FIELDNAMES)
    seen = load_seen(SEEN_FILE)
    print("Loaded seen urls:", len(seen))
    
    try:
        saved_csv = pd.read_csv(PREVIOUSLY_SCRAPED, on_bad_lines="skip", index_col=None)
        print("Previous web data found")
    except Exception as e:
        saved_csv = pd.DataFrame()  
        print("No previous web data found, continuing...")
        pass
        
    scraped_urls = set(saved_csv["url"]) if "url" in saved_csv.columns else set()

    # Open CSV once; append rows as we go
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=FIELDNAMES)

        for cat in CATEGORIES:
            print(f"\n=== Category {cat} ===")

            search = cs.Search(query=QUERY, city=CITY, category=cat)
            status = search.fetch(headers=HEADERS, timeout=25)
            if status != 200:
                print(f"Search fetch failed for {cat}: status={status}")
                continue

            for ad in search.ads:
                url = getattr(ad, "url", None) or getattr(ad, "link", None)
                if not url:
                    continue
                if url in seen and url in scraped_urls:
                    post_id = post_id_from_url(url)
                    matching_rows = saved_csv[saved_csv["url"] == url]
                    if not matching_rows.empty:
                        # find latest update date
                        updated_series = pd.to_datetime(matching_rows["updated"], errors="coerce")
                        posted_series = pd.to_datetime(matching_rows["posted"], errors="coerce")
                        combined_series = updated_series.combine_first(posted_series)
                        last_update = combined_series.max()
                        print(f"id:{post_id} is scraped ... last updated on {last_update}")
                        
                        # polite delay BEFORE fetching detail page
                        time.sleep(BASE_SLEEP + random.random() * JITTER)
                        
                        # scrape for update
                        response = requests.get(url)
                        soup = BeautifulSoup(response.text, "html.parser")
                        
                        if response.status_code == 200 and not is_page_throttled(soup) and not is_page_removed(soup):
                            info = extract_posting_info(soup)
                            post_id = info["post_id"]
                            posted = info["posted"]
                            updated = pd.to_datetime(info["updated"])
                            
                            if updated > last_update:
                                # create DataFrame and save
                                scrape = get_row_from_soup(soup, url)
                                data = pd.DataFrame([scrape], columns=cols)  # wrap in list for single row
                                data.to_csv(PREVIOUSLY_SCRAPED, mode='a', header=False, index=False)
                                
                                print(f"Post ID {post_id} updated on {updated.strftime('%Y-%m-%d %H:%M:%S')} added to {PREVIOUSLY_SCRAPED}")
                                continue
                            else:
                                print(url + " seen before. No updates found")
                                continue

                # polite delay BEFORE fetching detail page
                time.sleep(BASE_SLEEP + random.random() * JITTER)

                status = fetch_ad_with_retries(ad, headers=HEADERS, max_tries=6, timeout=25)
                if status != 200:
                    title = getattr(ad, "title", "UNKNOWN")
                    print(f"Skip '{title}' status={status}")
                    continue

                data = ad.to_dict()

                row = {
                    "category": cat,
                    "id": safe_get(data, "id", "post_id"),
                    "title": safe_get(data, "title"),
                    "url": safe_get(data, "url", "link") or url,
                }

                # append to CSV 
                writer.writerow(row)
                fcsv.flush()

                # record as seen 
                seen.add(url)
                append_seen(SEEN_FILE, url)

                if len(seen) % 50 == 0:
                    print("saved rows so far:", len(seen))

    print("Done. Total unique URLs saved:", len(seen))


if __name__ == "__main__":
    answer = sys.argv[1].lower() if len(sys.argv) > 1 else "n"
    print(f"Upload answer (y/n): {answer}")
    
    # search
    OUT_CSV = "web_scraping/craigslist/data/craigslist_rentals.csv"
    SEEN_FILE = "web_scraping/craigslist/data/seen_urls.txt"
    PREVIOUSLY_SCRAPED = "web_scraping/craigslist/data/detailed_craigslist_rentals.csv"

    OUT_CSV = add_date_to_filename(OUT_CSV)
    SEEN_FILE = add_date_to_filename(SEEN_FILE)
    PREVIOUSLY_SCRAPED = add_date_to_filename(PREVIOUSLY_SCRAPED)

    main(OUT_CSV, SEEN_FILE, PREVIOUSLY_SCRAPED)
    
    # upload 
    if answer == "y":
        gcs = GCSHelper(credentials_path="~/rental_project/key.json")
        BUCKET_NAME = "big-data-lab-2-project-data-store"
        BLOB_NAME = "raw/craigslist/all_rentals.csv"
        gcs.upload_local_file(BUCKET_NAME, PREVIOUSLY_SCRAPED, BLOB_NAME)
    else:
        print("Upload skipped.")