#!/usr/bin/env python3
import re
import os
import sys
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from helpers.gcp_utils import GCSHelper
from helpers.craigslist_helpers import (
    contains_keyword, 
    is_page_removed, 
    is_page_throttled,
    save_scraped_url,
    get_row_from_soup,
    add_date_to_filename,
    upload_to_gcs,
    save_local_csv
)

BASE = "https://vancouver.craigslist.org"
cols = ["post_id", "posted", "updated", "price", "rent_period", "title", "description", "attribute", "region", "region_code", "GPS_coordinate", "url", "pictures"]
    
def main(SAVE_PATH, PREVIOUSLY_REQUESTED, TO_BE_REQUESTED, max_requests=30, wait_time=15):
    column_data = set()
    try:
        saved_csv = pd.read_csv(SAVE_PATH, on_bad_lines="skip", index_col=None)
        column_data.update(saved_csv["url"])
        print("Data found in 'detailed_craigslist_rentals.csv' !")
    except Exception as e:
        print("No previous data found in 'detailed_craigslist_rentals.csv', continuing...")
        pass
    
    try:
        saved_csv = pd.read_csv(PREVIOUSLY_REQUESTED)
        column_data.update(saved_csv["url"])
        print("Data found in 'scraped_url.csv' !")
    except Exception as e:
        print("No previous data found in 'scraped_url', continuing...")
        pass    
        
    with open(TO_BE_REQUESTED, "r", encoding="utf-8", errors="ignore") as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print("Number of URLs searched:", len(urls))
    print("Number of URLs in column_data:", len(column_data))
    filtered_url = [url for url in urls if url not in column_data]
    print("List length after filter: ", len(filtered_url))
    
    throttle_counter = 0
    scrape = []
    scraped_urls = set()
    
    for url in filtered_url:
        throttle_counter += 1
        if throttle_counter > max_requests:
            break
           
        # system messages
        sys.stdout.write(f"\rProcessed {throttle_counter} URLs...")
        wait_time = random.uniform(1, 3)
        sys.stdout.write(f"Waiting {wait_time:.2f} seconds...")
        sys.stdout.flush()
        
        time.sleep(wait_time)
        
        # try to scrape
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            sys.stdout.write(f"Status: {response.status_code:.2f} ")
            
            if is_page_removed(soup) or response.status_code == 404 or response.status_code == 410:
                sys.stdout.write("Page is removed.          ")
                sys.stdout.flush()
                save_scraped_url(url, PREVIOUSLY_REQUESTED)
                scraped_urls.add(url)
            if is_page_throttled(soup):
                sys.exit("Request has been throttled {url}. Please try again later.") 
            if response.status_code != 200:
                continue 
                
        except requests.exceptions.RequestException as e:
            continue
        
        sys.stdout.write(" Request Successful!")
        sys.stdout.flush()

            
        if url not in scraped_urls:
            scraped_urls.add(url)
            
        extracted_row = get_row_from_soup(soup, url)
        
        # Add the scraped data to the list
        scrape.append(extracted_row)
    
    # if file exists, append to .csv
    print(f"\nTotal URLs scraped: {len(scraped_urls)}")
    print(f"Total valid rows: {len(scrape)}")
    save_local_csv(scrape, SAVE_PATH, cols)
    for url in scraped_urls:
        save_scraped_url(url, PREVIOUSLY_REQUESTED)
   


if __name__ == "__main__":
    answer = sys.argv[1].lower() if len(sys.argv) > 1 else "n"
    print(f"Upload answer (y/n): {answer}")
    
    # request
    SAVE_PATH = "web_scraping/craigslist/data/detailed_craigslist_rentals.csv"
    PREVIOUSLY_REQUESTED = "web_scraping/craigslist/data/scraped_url.csv"
    TO_BE_REQUESTED = "web_scraping/craigslist/data/seen_urls.txt"
    
    SAVE_PATH = add_date_to_filename(SAVE_PATH)
    PREVIOUSLY_REQUESTED = add_date_to_filename(PREVIOUSLY_REQUESTED)
    TO_BE_REQUESTED = add_date_to_filename(TO_BE_REQUESTED)
    
    main(SAVE_PATH, PREVIOUSLY_REQUESTED, TO_BE_REQUESTED, max_requests=100)

    # upload 
    # upload 
    if answer == "y":
        gcs = GCSHelper(credentials_path="~/rental_project/key.json")
        BUCKET_NAME = "big-data-lab-2-project-data-store"
        BLOB_NAME = "raw/craigslist/all_rentals.csv"
        gcs.upload_local_file(BUCKET_NAME, SAVE_PATH, BLOB_NAME)
    else:
        print("Upload skipped.")
        
