import pandas as pd
import numpy as np


def extract_regex_features(master_df, df_master_descriptions, rew_raw, bnb_raw):

    # merge descriptions
    master_df = master_df.merge(
        df_master_descriptions[["description"]],
        left_on="hash",
        right_index=True,
        how="left"
    )

    # structured features from source data
    rew_feat = pd.DataFrame({
        "hash": rew_raw["hash"],
        "beds": pd.to_numeric(rew_raw["beds"], errors="coerce"),
        "sqft": pd.to_numeric(rew_raw["size_sqft"], errors="coerce")
    })

    bnb_feat = pd.DataFrame({
        "hash": bnb_raw["hash"],
        "beds": pd.to_numeric(bnb_raw["bedrooms"], errors="coerce"),
        "sqft": None
    })

    structured_features = pd.concat([rew_feat, bnb_feat], ignore_index=True)
    structured_features = structured_features.drop_duplicates("hash")

    master_df = master_df.merge(structured_features, on="hash", how="left")

    blob = master_df["description"].fillna("").astype(str).str.lower()

    ################################################
    # BEDS
    ################################################

    beds_regex = r'(\d+(?:\.5)?)\s*(?:bed(?:room)?s?|bdrm|bd|br)\b'

    master_df["beds"] = master_df["beds"].fillna(
        blob.str.extract(beds_regex)[0].astype(float)
    )

    # studio detection
    studio_mask = blob.str.contains(
        r'\bstudio\b|\bbachelor\b|\b0\s*(?:bed|br)',
        na=False
    )

    master_df.loc[studio_mask, "beds"] = 0

    # word numbers
    word_beds = {
        "one":1,"two":2,"three":3,"four":4,"five":5
    }

    for word, val in word_beds.items():

        master_df.loc[
            master_df["beds"].isna() &
            blob.str.contains(fr'\b{word}\s+bed(?:room)?', na=False),
            "beds"
        ] = val


    ################################################
    # SQFT
    ################################################

    sqft_regex = r'(\d{3,5})\s*(?:sq\.?\s*ft|sqft|sf|square\s*feet)'

    master_df["sqft"] = master_df["sqft"].fillna(
        blob.str.extract(sqft_regex)[0].astype(float)
    )


    ################################################
    # FURNISHED
    ################################################

    master_df["is_furnished"] = (
        blob.str.contains(
            r'\bfurnished\b|\bfully furnished\b|\bturnkey\b|\bmove[- ]?in ready\b',
            na=False
        )
        &
        ~blob.str.contains(
            r'\bunfurnished\b|\bnot furnished\b',
            na=False
        )
    )


    ################################################
    # UTILITIES INCLUDED
    ################################################

    master_df["utilities_inc"] = blob.str.contains(
        r'utilities included|all utilities included|all[- ]inclusive|hydro included|heat included|water included|electricity included',
        na=False
    )


    ################################################
    # LAUNDRY
    ################################################

    master_df["has_laundry"] = np.select(

        [
            blob.str.contains(
                r'in[- ]suite laundry|in[- ]unit laundry|ensuite laundry|washer\s*/?\s*dryer|private laundry',
                na=False
            ),

            blob.str.contains(
                r'shared laundry|coin laundry|building laundry|common laundry|laundry room',
                na=False
            )
        ],

        ["In-Suite","In-Building"],

        default="Unknown"
    )


    ################################################
    # PET POLICY (IMPROVED)
    ################################################

    def get_pet_label(text):

        t = str(text).lower()

        if any(x in t for x in [
            "no pets",
            "no animals",
            "pet free",
            "pets not allowed",
            "strictly no pets"
        ]):
            return "No Pets"

        cat = any(x in t for x in [
            "cats ok",
            "cats allowed",
            "cat friendly",
            "cats welcome"
        ])

        dog = any(x in t for x in [
            "dogs ok",
            "dogs allowed",
            "dog friendly",
            "dogs welcome"
        ])

        if cat and dog:
            return "Cats & Dogs OK"

        if cat:
            return "Cats Only"

        if dog:
            return "Dogs Only"

        if any(x in t for x in [
            "pets allowed",
            "pet friendly",
            "pet negotiable",
            "pets negotiable"
        ]):
            return "Pets Negotiable"

        if any(x in t for x in [
            "small pets",
            "small dog",
            "small pets ok"
        ]):
            return "Small Pets Only"

        return "Unknown"


    master_df["pets_policy"] = master_df["description"].apply(get_pet_label)

    return master_df