import pandas as pd


def build_raw_text_table(dfs):

    combined = pd.concat(
        [df[["hash", "source", "description"]] for df in dfs],
        ignore_index=True
    )

    # Remove Craigslist QR prefix
    combined.loc[combined["source"] == "craigslist", "description"] = (
        combined.loc[combined["source"] == "craigslist", "description"]
        .str.replace(r"(?i)^qr code link to this post\s*", "", regex=True)
    )

    combined.set_index("hash", inplace=True)

    return combined