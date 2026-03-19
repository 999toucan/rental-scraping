import pandas as pd

def load_raw_sources():

    cl_raw = pd.read_csv("./data/raw_craigslist_all_rentals.csv")
    kj_raw = pd.read_csv("./data/raw_kijiji_all_rentals.csv")
    tr_raw = pd.read_csv("./data/raw_trovit_all_rentals.csv")
    rew_raw = pd.read_csv("./data/raw_rew_all_rentals.csv")
    pm_raw = pd.read_csv("./data/raw_PadMapper_all_rentals.csv")
    bnb_raw = pd.read_csv("./data/raw_airbnb_airbnb_all_rentals.csv")

    dfs = [
        ("craigslist", cl_raw),
        ("kijiji", kj_raw),
        ("trovit", tr_raw),
        ("rew", rew_raw),
        ("padmapper", pm_raw),
        ("airbnb", bnb_raw)
    ]

    cleaned = []

    for source, df in dfs:
        
        # normalize columns
        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        # add source column
        df["source"] = source

        cleaned.append(df)

    return cleaned