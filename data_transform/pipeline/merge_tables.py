import pandas as pd
from functools import reduce


def merge_all_tables(master_df, tables):

    dfs = [master_df] + tables

    cleaned_dfs = [dfs[0]]

    for df in dfs[1:]:
        df = df.drop(columns=[c for c in ["url", "source"] if c in df.columns], errors="ignore")
        cleaned_dfs.append(df)

    merge_keys = ["hash"]

    df_master_unified = reduce(
        lambda left, right: pd.merge(left, right, on=merge_keys, how="left"),
        cleaned_dfs
    )

    return df_master_unified