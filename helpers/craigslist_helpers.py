#!/usr/bin/env python3
import re
import os
import sys
import json
import time
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from google.cloud import storage
from datetime import datetime

BASE = "https://vancouver.craigslist.org"

def contains_keyword(str, keyword="burnaby"):
    normalized = re.sub(r'[^a-z0-9]+', '', str.lower())
    key = re.sub(r'[^a-z0-9]+', '', keyword.lower())
    return key in normalized

def is_page_removed(bs):
    removed_div = bs.find("div", class_="removed")
    if removed_div and "deleted by its author" in removed_div.get_text():
        return True
    else:
        return False
    
def is_page_throttled(bs):
    is_blocked = (
        (bs.title and "blocked" in bs.title.string.lower()) or 
        ("request has been blocked" in bs.get_text().lower())
    )
    return is_blocked
    
def clean_price(string):
    return string.replace("$", "").replace(",", "")
    
def save_scraped_url(url, savepath):
    file_exists = os.path.isfile(savepath)
    with open(savepath, "a") as f:
        if not file_exists:
            f.write("url\n")
        f.write(url + "\n")
        
def extract_attr(soup):
    # extracts attributs
    # returns: list of strings
    attrgroups = soup.find_all('div', class_='attrgroup')
    texts = []
    for attrgroup in attrgroups:
        attr_divs = attrgroup.find_all('div', class_='attr')
        attr_texts = [div.find('span', class_='valu').get_text(strip=True) 
                      for div in attr_divs if div.find('span', class_='valu')]
        texts.extend(attr_texts)
    return texts

def extract_pictures(soup):
    # extracts urls links to pictures
    # returns: list of strings 
    script_tag = soup.find('script', string=re.compile(r'var imgList'))
    
    if script_tag:
        script_content = script_tag.string
        json_match = re.search(r'var imgList = (\[.*?\]);', script_content)
        
        if json_match:
            img_list_json = json_match.group(1)
            img_data = json.loads(img_list_json)
            url_list = [item.get('url') for item in img_data if 'url' in item]
            return url_list 
    return []
    
import re

def extract_posting_info(soup):
    # extracts the 'id" and 'posted', 'updated' dates
    # returns: dictionary with both dates in YYYY-MM-DD format.
    
    results = {
        'post_id': "",
        'posted': "",
        'updated': ""
    }
   
    container = soup.find("div", class_="postinginfos")
    
    if not container:
        return results
    
    id_tag = container.find("p", string=re.compile(r"post id:", re.I))
    if id_tag:
        results["post_id"] = re.sub(r"\D", "", id_tag.text)
            
    time_tags = container.find_all("time")
    results["posted"] = time_tags[0]["datetime"][:10] if len(time_tags) > 0 else ""
    results["updated"] = time_tags[1]["datetime"][:10] if len(time_tags) > 1 else ""

    return results


def post_id_from_url(url):
    parts = url.rstrip("/").split("/")
    last_part = parts[-1]
    post_id = last_part.split(".")[0]
    return int(post_id)
    
        
def get_row_from_soup(soup, url):
    # extract data from soup
    # posting info 
    info = extract_posting_info(soup)
    post_id = info["post_id"]
    posted = info["posted"]
    updated = info["updated"]
    
    # price
    price = clean_price(price_element.get_text(strip=True)) \
        if (price_element := soup.find("span", class_="price")) \
        else None
    
    # rental period
    rent_period_div = soup.find('div', class_='attr rent_period')
    if rent_period_div:
        rent_period = rent_period_div.find('a').get_text(strip=True) \
        if rent_period_div.find('a') else None
    else:
        rent_period = None
    
    # title 
    title_meta = soup.find('title')
    title = title_meta.get_text() if title_meta else None
    
    # description
    posting_body = soup.find('section', {'id': 'postingbody'})
    if posting_body:
        text = posting_body.get_text(separator=" ", strip=True)
        description = " ".join(text.split())
    else:
        description = None 
        print("Section with id 'postingbody' not found.")
        
    # attribute 
    attr_texts = extract_attr(soup)
    
    # region
    region_meta = soup.find('meta', {'name': 'geo.placename'})
    region = region_meta['content'] if region_meta else None
    # region code 
    region_code_meta = soup.find('meta', {'name': 'geo.region'})
    region_code = region_code_meta['content'] if region_code_meta else None
                  
    # coordinates lat lon
    icbm_meta = soup.find('meta', {'name': 'ICBM'})
    if icbm_meta:
        lat_lon = icbm_meta['content']
        lat, lon = lat_lon.split(', ')
    else:
        lat, lon = None, None
    coord = (lat, lon)
    
    # pictures 
    pictures = extract_pictures(soup) 
    
    return [post_id, posted, updated, price, rent_period, title, description,
                                      attr_texts, region, region_code, coord, url, pictures]
                                 
def save_local_csv(ls, save_path, headers, retries=3):
    """
    saves csv to save_path with headers
    if file is not created, create file
    otherwise append to file 
    """
    for attempt in range(retries):
        try:
            data = pd.DataFrame(ls, columns=headers)
            if os.path.exists(save_path):
                data.to_csv(save_path, mode='a', header=False, index=False)
                print(f"{len(data)} lines added to")
                print(save_path)
                return 
            else:
                data.to_csv(save_path, mode='w', header=True, index=False)
                print(f"File created with {len(data)} lines.")
                print(save_path)
                return
        except PermissionError as e:
            print(f"Permission error on attempt {attempt + 1} of {retries}: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                print("Maximum retry attempts reached. Could not save the file.")
                raise 

        except Exception as e:
            print(f"Error saving data to CSV: {e}")
            raise
            
def add_date_to_filename(filename):
    # takes string and adds date YYYY-MM-DD before file extension
    date_str = datetime.now().strftime("%Y-%m-%d")
    name, extension = filename.rsplit('.', 1)
    return f"{name}_{date_str}.{extension}"

def upload_to_gcs(local_path, bucket_name, bucket_path, destination_name):    
    """
    local_path: Path to the file on your machine (e.g., OUT_CSV)
    bucket_name: Your bucket ID (e.g., 'big-data-lab-2-project-data-store')
    bucket_path: The folder inside the bucket (e.g., 'data')
    blob_name: The desired filename (e.g., 'all_rentals.csv')
    """
    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        print(f"Skipping: {local_path} is missing or empty.")
        return
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        full_blob_path = f"{bucket_path}/{destination_name}"
        blob = bucket.blob(full_blob_path)
        
        print(f"Uploading {local_path} to gs://{bucket_name}/{full_blob_path}...")
        blob.upload_from_filename(local_path)
        print("Upload complete and file overwritten.")
    except Exception as e:
        print(f"GCS Upload Error: {e}")
        