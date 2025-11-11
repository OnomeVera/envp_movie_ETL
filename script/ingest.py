# scr/etl_pipeline.py
import requests
import pandas as pd
import os
import logging
from datetime import datetime


#  CONFIGURATION

API_KEY = "28e9bb452ac5e18dc2469d7eac3a852c"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Ensure all folders exist
for path in [RAW_DIR, PROCESSED_DIR, LOG_DIR]:
    os.makedirs(path, exist_ok=True)

# timestamp for each run
today = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOG_DIR, f"etl_log_{today}.log")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

logging.info(f"TMDB ETL Pipeline started for run: {today}")



#  Ingest

def fetch_movies(api_key, start_year=2023, end_year=2025):
    all_movies = []
    for year in range(start_year, end_year + 1):
        logging.info(f"Fetching movies for {year}...")
        for page in range(1, 6):
            url = "https://api.themoviedb.org/3/discover/movie"
            params = {
                "api_key": api_key,
                "language": "en-US",
                "sort_by": "vote_average.desc",
                "primary_release_year": year,
                "vote_average.gte": 3.5,
                "page": page
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                if "results" in data:
                    all_movies.extend(data["results"])
            except Exception as e:
                logging.error(f" Failed for year {year}, page {page}: {e}")

    raw_path = os.path.join(RAW_DIR, f"movies_raw_{today}.csv")
    pd.DataFrame(all_movies).to_csv(raw_path, index=False)
    logging.info(f" Raw data saved → {raw_path}")
    return raw_path



# Transform

def transform_movies(raw_path):
    df = pd.read_csv(raw_path)
    logging.info(f"Transforming {len(df)} records...")

    columns = [
        "title", "release_date", "vote_average", "vote_count",
        "overview", "original_language", "popularity"
    ]
    df = df[columns].dropna(subset=["title", "vote_average"])
    df = df[df["vote_average"] >= 3.5]

    processed_path = os.path.join(PROCESSED_DIR, f"movies_cleaned_{today}.csv")
    df.to_csv(processed_path, index=False)
    logging.info(f" Transformed data saved → {processed_path}")
    return processed_path


# Load

def load_movies(processed_path):
    df = pd.read_csv(processed_path)
    logging.info(f"Loaded {len(df)} cleaned records.")
    print("\n Top 5 Movies from this run:")
    print(df.head(5))



# Run Pipeline

if __name__ == "__main__":
    try:
        raw_path = fetch_movies(API_KEY)
        processed_path = transform_movies(raw_path)
        load_movies(processed_path)
        logging.info(" ETL pipeline completed successfully!")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

