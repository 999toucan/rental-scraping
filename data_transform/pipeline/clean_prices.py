import pandas as pd
import re

def clean_craigslist_price(df):

    mask = df["rent_period"] == "monthly"

    return df.loc[mask].copy()


def clean_standard_price(df):

    df["price"] = df["price"].astype(str).str.extract(r"([\d,.]+)")
    df["price"] = df["price"].str.replace(",", "", regex=False)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0).astype(int)

    return df


def clean_multi_price(df):

    df["price"] = df["price"].astype(str)
    df["price"] = df["price"].str.extract(r"([\d,.]+)")
    df["price"] = df["price"].str.replace(",", "", regex=False)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df