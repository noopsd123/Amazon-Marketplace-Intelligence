import os
import glob
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_TABLE  = f"{PROJECT_ID}.raw.product_snapshots"
RAW_DIR    = "data/raw"

# Only keep the columns we actually need
KEEP_COLUMNS = [
    "sku",
    "name",
    "brandName",
    "nodeName",
    "listedPrice",
    "salePrice",
    "currency",
    "rating",
    "reviewCount",
    "inStock",
    "scrapedDate",
    "url",
]

def clean_dataframe(df, source_file):
    """Keep only useful columns, add pipeline metadata"""

    # Keep only columns that exist in this file
    existing_cols = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[existing_cols].copy()

    # Add pipeline metadata
    df["_source_file"] = os.path.basename(source_file)
    df["_loaded_at"]   = datetime.now(timezone.utc).isoformat()

    # Convert everything to string first to avoid type issues
    # BigQuery will cast properly from staging layer
    for col in df.columns:
        if col not in ["_loaded_at"]:
            df[col] = df[col].astype(str)

    # Replace 'nan' strings with None (proper nulls)
    df = df.replace("nan", None)
    df = df.replace("None", None)

    return df


def is_already_loaded(client, source_file):
    """Check if this file was already loaded to avoid duplicates"""
    filename = os.path.basename(source_file)
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{RAW_TABLE}`
        WHERE _source_file = '{filename}'
    """
    try:
        result = client.query(query).result()
        for row in result:
            return row.cnt > 0
    except Exception:
        # Table doesn't exist yet — first load
        return False


def load_csv_to_bigquery(csv_path):
    client = bigquery.Client()

    # Skip if already loaded
    if is_already_loaded(client, csv_path):
        print(f"Already loaded {os.path.basename(csv_path)} — skipping")
        return 0

    print(f"Loading {os.path.basename(csv_path)}...")

    # Read CSV (large file — read in chunks)
    chunks = pd.read_csv(csv_path, chunksize=10000, low_memory=False)

    total_rows = 0
    for i, chunk in enumerate(chunks):
        df = clean_dataframe(chunk, csv_path)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )

        job = client.load_table_from_dataframe(
            df, RAW_TABLE, job_config=job_config
        )
        job.result()
        total_rows += len(df)
        print(f"  Chunk {i+1}: {len(df)} rows loaded")

    print(f"  Done: {total_rows} total rows → {RAW_TABLE}\n")
    return total_rows


def run_load():
    csv_files = glob.glob(f"{RAW_DIR}/*.csv")

    if not csv_files:
        print("No CSV files found in data/raw/ — run extract first")
        return

    print(f"Found {len(csv_files)} CSV files to process\n")

    grand_total = 0
    for csv_file in sorted(csv_files):
        rows = load_csv_to_bigquery(csv_file)
        grand_total += rows

    print(f"All done. {grand_total} total rows loaded to BigQuery.")


if __name__ == "__main__":
    run_load()
