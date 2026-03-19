import pandas as pd


def check_df(df, name="df"):
    """Basic dataframe diagnostics"""

    print(f"\n--- {name} ---")
    print("shape:", df.shape)
    print("columns:", list(df.columns))
    print("null counts:")
    print(df.isna().sum().sort_values(ascending=False).head(10))
    print()


def check_duplicates(df, key):
    """Check duplicates for primary key"""

    dupes = df.duplicated(key).sum()

    print(f"duplicates on {key}: {dupes}")


def preview(df, n=5):
    """Print first rows"""

    print(df.head(n))