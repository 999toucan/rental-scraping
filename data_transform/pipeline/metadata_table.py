import pandas as pd
import ast

TODAY = pd.Timestamp.now().normalize()


def generate_master_metadata(row):

    # hash, source and url
    row_hash = row.get("hash")
    source_key = row.get("source", "")

    url = (
        row.get("url")
        or row.get("source_url")
        or row.get("listing_url")
    )

    # last_updated
    last_updated_val = None

    if source_key == "craigslist":
        raw_updated = row.get("updated")
        if pd.notnull(raw_updated) and str(raw_updated).strip() != "":
            last_updated_val = str(raw_updated).strip()

    elif source_key == "airbnb":
        last_updated_val = row.get("end_date")

    # date posted and days on market
    if source_key == "craigslist":
        raw_date = (
            row.get("updated")
            if pd.notnull(row.get("updated")) and str(row.get("updated")).strip() != ""
            else row.get("posted")
        )

    elif source_key == "airbnb":
        raw_date = row.get("start_date")

    else:
        raw_date = (
            row.get("date_posted")
            or row.get("posted")
        )

    date_dt = pd.to_datetime(raw_date, errors="coerce")

    days_on_market = (
        (TODAY - date_dt.normalize()).days
        if pd.notnull(date_dt)
        else None
    )

    # image extraction
    img_val = (
        row.get("pictures")
        or row.get("image_url")
        or row.get("image")
        or row.get("picture_url")
    )

    primary_image = None

    if isinstance(img_val, str) and img_val.strip():

        if img_val.startswith("["):

            try:
                img_list = ast.literal_eval(img_val)
                primary_image = img_list[0] if img_list else None

            except:
                primary_image = img_val.split(",")[0].strip("[]'\" ")

        else:
            primary_image = img_val.split(",")[0].strip()

    return pd.Series({
        "hash": row_hash,
        "source": source_key,
        "date_posted": date_dt.date() if pd.notnull(date_dt) else None,
        "last_updated": last_updated_val,
        "days_on_market": days_on_market,
        "primary_image": primary_image,
        "url": url,
        "manual_review": False
    })


def build_metadata_table(dfs):

    processed = []

    for df in dfs:

        if "hash" not in df.columns:
            continue

        processed.append(
            df.apply(generate_master_metadata, axis=1)
        )

    df_master_metadata = pd.concat(processed, ignore_index=True)

    # remove duplicate hashes
    df_master_metadata = df_master_metadata.drop_duplicates(subset="hash")

    df_master_metadata.set_index("hash", inplace=True)

    return df_master_metadata