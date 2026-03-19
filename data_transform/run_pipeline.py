# Run instructions
# =============================================
# Raw data is stored in ./data/
# Results saved in ./data/filtered/

# ---------------------------------------------

# Run without AI enrichment:
# python3 run_pipeline.py


# ---------------------------------------------

# Run with ChatGPT:
# python3 run_pipeline.py --ai_provider chatgpt --api_key YOUR_OPENAI_API_KEY

# Run with Gemini:
# python3 run_pipeline.py --ai_provider gemini --api_key YOUR_GEMINI_API_KEY

# ---------------------------------------------
# Optional parameters:
# --model MODEL_NAME   (override default model)
# --delay SECONDS      (delay between API calls to avoid rate limits)

import argparse
import pandas as pd

from pipeline.load_data import load_raw_sources
from pipeline.hashing import generate_hash
from pipeline.core_table import build_master_core
from pipeline.metadata_table import build_metadata_table
from pipeline.location_table import build_location_table
from pipeline.raw_text_table import build_raw_text_table
from pipeline.airbnb_table import build_airbnb_extra_table
from pipeline.regex_features import extract_regex_features
from pipeline.merge_tables import merge_all_tables
from pipeline.ai_enrichment import create_enricher


def parse_args():

    parser = argparse.ArgumentParser(
        description="Rental listing ETL pipeline"
    )

    parser.add_argument(
        "--ai_provider",
        choices=["chatgpt", "gemini"],
        default=None
    )

    parser.add_argument(
        "--api_key",
        default=None
    )

    parser.add_argument(
        "--model",
        default=None
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0
    )

    return parser.parse_args()


def main():

    args = parse_args()

    print("Loading raw sources...")
    cl_raw, kj_raw, tr_raw, rew_raw, pm_raw, bnb_raw = load_raw_sources()

    print("Generating hashes...")
    cl_raw = generate_hash(cl_raw, "url")
    kj_raw = generate_hash(kj_raw, "source_url")
    rew_raw = generate_hash(rew_raw, "url")
    tr_raw = generate_hash(tr_raw, "listing_url")
    pm_raw = generate_hash(pm_raw, "listing_url")
    bnb_raw = generate_hash(bnb_raw, "listing_url")

    print("Building master core table...")
    df_master_core = build_master_core(
        cl_raw,
        kj_raw,
        rew_raw,
        tr_raw,
        pm_raw,
        bnb_raw
    )

    df_master_core = df_master_core.drop_duplicates("hash")

    dfs = [cl_raw, kj_raw, rew_raw, tr_raw, pm_raw, bnb_raw]

    print("Building metadata table...")
    df_master_metadata = build_metadata_table(dfs)

    print("Building location table...")
    df_master_location = build_location_table(dfs)

    print("Building raw description table...")
    df_master_raw = build_raw_text_table(dfs)

    print("Building Airbnb metadata...")
    df_airbnb_meta = build_airbnb_extra_table(bnb_raw)

    print("Running regex feature extraction...")
    master_df = extract_regex_features(
        df_master_core,
        df_master_raw,
        rew_raw,
        bnb_raw
    )

    if args.ai_provider and args.api_key:

        print("Running AI enrichment for missing values...")

        ai_rows = master_df[
            master_df["beds"].isna() |
            master_df["sqft"].isna() |
            (master_df["has_laundry"] == "Unknown") |
            (master_df["pets_policy"] == "Unknown")
        ].copy()

        if not ai_rows.empty:

            enricher = create_enricher(
                provider=args.ai_provider,
                api_key=args.api_key,
                model=args.model,
                delay=args.delay
            )

            def build_prompt(row):
                return f"""
                Extract structured rental features from this listing description.

                Return JSON with keys:
                beds, sqft, laundry, pets_policy

                Description:
                {row['description']}
                """

            ai_rows["prompt"] = ai_rows.apply(build_prompt, axis=1)

            ai_results = enricher.enrich_dataframe(
                df=ai_rows,
                text_column="prompt",
                output_column="ai_json"
            )

            for idx, row in ai_results.iterrows():

                try:
                    parsed = pd.json_normalize(eval(row["ai_json"])).iloc[0]
                except:
                    continue

                if pd.isna(master_df.loc[idx, "beds"]) and "beds" in parsed:
                    master_df.loc[idx, "beds"] = parsed["beds"]

                if pd.isna(master_df.loc[idx, "sqft"]) and "sqft" in parsed:
                    master_df.loc[idx, "sqft"] = parsed["sqft"]

                if master_df.loc[idx, "has_laundry"] == "Unknown" and "laundry" in parsed:
                    master_df.loc[idx, "has_laundry"] = parsed["laundry"]

                if master_df.loc[idx, "pets_policy"] == "Unknown" and "pets_policy" in parsed:
                    master_df.loc[idx, "pets_policy"] = parsed["pets_policy"]

        else:
            print("No rows require AI enrichment.")

    else:
        print("Skipping AI enrichment")

    print("Merging final dataset...")
    df_master_unified = merge_all_tables(
        master_df,
        [
            df_master_metadata,
            df_airbnb_meta,
            df_master_location
        ]
    )
    
    print("Removing duplicate listings...")
    df_master_unified = df_master_unified.drop_duplicates(subset="hash")
    output_path = "./data/filtered/df_master_unified.csv"

    df_master_unified.to_csv(output_path, index=False)

    print("Pipeline complete")
    print("Saved:", output_path)
    print("Final shape:", df_master_unified.shape)
    print("Final shape [source != 'airbnb']:", df_master_unified[df_master_unified["source"] != "airbnb"].shape)


if __name__ == "__main__":
    main()