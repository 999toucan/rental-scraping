
# Rental Data Pipeline

This project scrapes rental listings, cleans and merges the datasets, extracts structured features, and optionally uses AI to fill missing information.

---

## Craigslist Web Scraper

```bash
python3 -m web_scraping.craigslist.craigslist_search_url
python3 -m web_scraping.craigslist.request_to_csv
```

These scripts generate raw CSV files inside the `data/` directory.

---

# Run the Data Transformation Pipeline

## Data Locations

Raw scraped data should be stored in:

```

.data_transform/data/

```

The pipeline cleans and merges the scraped datasets, extracts structured features, and optionally runs AI enrichment.

### Run Without AI Enrichment

```bash
python3 run_pipeline.py
```

This will:

* clean and normalize the raw data
* merge all sources
* extract structured features using regex
* save the final dataset to:

```
./data/filtered/df_master_unified.csv
```

---

# Run With AI Enrichment

AI can fill missing structured features (beds, sqft, laundry, pets policy) when regex extraction fails.

### ChatGPT

```bash
python3 run_pipeline.py --ai_provider chatgpt --api_key YOUR_OPENAI_API_KEY
```

### Gemini

```bash
python3 run_pipeline.py --ai_provider gemini --api_key YOUR_GEMINI_API_KEY
```

---

# Optional Parameters

| Parameter            | Description                                  |
| -------------------- | -------------------------------------------- |
| `--model MODEL_NAME` | Override the default AI model                |
| `--delay SECONDS`    | Delay between API calls to avoid rate limits |

Example:

```bash
python3 run_pipeline.py \
--ai_provider chatgpt \
--api_key YOUR_API_KEY \
--model gpt-4o-mini \
--delay 0.5
```

---

# Pipeline Overview

The pipeline performs the following steps:

1. Load raw scraped data
2. Generate unique listing hashes
3. Clean and standardize listing features
4. Extract structured information using regex
5. Optionally enrich missing data using AI
6. Merge metadata, location, and description features
7. Save the final unified dataset

Final output:

```
./data/filtered/df_master_unified.csv
```
