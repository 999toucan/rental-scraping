import pandas as pd
import re

# PRICE HELPERS
def clean_craigslist_price(df):
    mask = df['rent_period'] == 'monthly'
    return df.loc[mask].copy()


def clean_standard_price(df):
    if 'price' not in df.columns:
        return df

    df['price'] = df['price'].astype(str).str.extract(r'([\d,.]+)')
    df['price'] = df['price'].str.replace(',', '', regex=False)
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)
    return df

def clean_multi_price(df):

    def get_min_price(val):
        nums = re.findall(r'([\d,.]+)', str(val))
        if not nums:
            return 0

        clean_nums = [float(n.replace(',', '')) for n in nums]

        return int(min(clean_nums))

    df['price'] = df['price'].apply(get_min_price)

    return df


# TYPE HELPERS
def set_craigslist_type(df):

    property_types = {
        'apartment','condo','house','townhouse','duplex','loft','suite'
    }

    df['property_type'] = df['attribute'].apply(
        lambda x: next(
            iter(set(eval(x) if isinstance(x,str) else x) & property_types),
            'apartment'
        )
    )

    return df


def set_padmapper_type(df):

    def extract_pm_type(desc):

        desc = str(desc).lower()

        if 'house' in desc:
            return 'house'

        if 'room' in desc:
            return 'room'

        if 'suite' in desc:
            return 'suite'

        return 'apartment'

    df['property_type'] = df['description'].apply(extract_pm_type)

    return df


def set_airbnb_type(df):

    def extract_type(desc):

        if desc is None or pd.isna(desc):
            return 'unknown'

        desc = str(desc).lower()

        if 'room' in desc or 'shared' in desc:
            return 'room'

        if any(k in desc for k in ['apartment','condo','suite','loft']):
            return 'apartment'

        if any(k in desc for k in ['house','villa','townhouse','home']):
            return 'house'

        return 'other'

    df['property_type'] = df['property_type'].apply(extract_type)

    return df

def standardize_and_clean(df, url_col):

    df = df.copy()

    df['url'] = df[url_col]

    type_map = {
        'apartments condos':'apartment',
        'apt/condo':'apartment',
        'condo':'apartment',
        'house ':'house',
        'townhouse':'house',
        'duplex':'house',
        'loft':'apartment'
    }

    df['property_type'] = df['property_type'].astype(str).str.lower().str.strip()
    df['property_type'] = df['property_type'].replace(type_map)

    return df[['hash','price','property_type','source','url']]

def build_master_core(cl,kj,rew,tr,pm,abnb):

    cl = clean_craigslist_price(cl)
    kj = clean_standard_price(kj)
    rew = clean_standard_price(rew)
    tr = clean_multi_price(tr)
    pm = clean_multi_price(pm)
    abnb = clean_standard_price(abnb)

    cl = set_craigslist_type(cl)

    kj['property_type'] = kj.get('type')

    rew['property_type'] = rew.get('type')

    tr['property_type'] = tr['location'].str.split('in').str[0]

    pm = set_padmapper_type(pm)

    abnb = set_airbnb_type(abnb)

    cl_final = standardize_and_clean(cl,'url')
    kj_final = standardize_and_clean(kj,'source_url')
    rew_final = standardize_and_clean(rew,'url')
    tr_final = standardize_and_clean(tr,'listing_url')
    pm_final = standardize_and_clean(pm,'listing_url')
    abnb_final = standardize_and_clean(abnb,'listing_url')

    master_df = pd.concat(
        [cl_final,kj_final,rew_final,tr_final,pm_final,abnb_final],
        ignore_index=True
    )

    df_master_core = master_df[master_df['price'] > 0]

    return df_master_core