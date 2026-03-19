import pandas as pd

def build_airbnb_extra_table(df):

    def extract_metadata(row):

        verified_raw = row.get('host_identity_verified', 'f')
        is_verified = True if verified_raw == 't' else False

        superhost_raw = row.get('host_is_superhost', 'f')
        is_superhost = True if superhost_raw == 't' else False

        return pd.Series({
            'hash': row.get('hash'),
            'url': row.get('listing_url'),
            'is_verified': is_verified,
            'host_is_superhost': is_superhost,
            'review_score': row.get('review_scores_value'),
            'num_reviews': row.get('number_of_reviews')
        })

    abnb_only = df[df['source'] == 'airbnb'].copy()

    if abnb_only.empty:
        return pd.DataFrame(columns=[
            'hash',
            'url',
            'is_verified',
            'host_is_superhost',
            'review_score',
            'num_reviews'
        ])

    result = abnb_only.apply(extract_metadata, axis=1)

    return result.set_index('hash')