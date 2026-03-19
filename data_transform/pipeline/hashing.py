import hashlib

def generate_hash(df, col):

    df = df.copy()

    if col not in df.columns:
        raise ValueError(f"Missing column '{col}' for hashing")

    df["hash"] = df[col].astype(str).apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )

    return df