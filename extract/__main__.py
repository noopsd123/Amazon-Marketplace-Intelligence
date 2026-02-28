import requests
import os
import zipfile
import io
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Top-level data folder in the repo
REPO_BASE_URL = "https://api.github.com/repos/octaprice/ecommerce-product-dataset/contents/data"
RAW_DATA_DIR  = "data/raw"


def get_all_zip_files(api_url, found_zips=None):
    """Recursively walk all subfolders and collect every .zip file"""
    if found_zips is None:
        found_zips = []

    response = requests.get(api_url)
    if response.status_code != 200:
        print(f"Could not access: {api_url}")
        return found_zips

    items = response.json()

    for item in items:
        if item["type"] == "dir":
            # Go deeper into subfolder
            get_all_zip_files(item["url"], found_zips)
        elif item["type"] == "file" and item["name"].endswith(".zip"):
            found_zips.append(item)

    return found_zips


def download_and_extract_zip(zip_info):
    """Download a zip, extract the CSV inside, save to data/raw/"""
    zip_name     = zip_info["name"]                        # e.g. amazon_com_best_sellers_2025_01_27.zip
    csv_name     = zip_name.replace(".zip", ".csv")        # e.g. amazon_com_best_sellers_2025_01_27.csv
    local_csv    = os.path.join(RAW_DATA_DIR, csv_name)

    # Skip if we already have this CSV
    if os.path.exists(local_csv):
        print(f"Already have {csv_name} — skipping")
        return None

    print(f"Downloading {zip_name} ({zip_info['size'] // 1024} KB compressed)...")

    # Download zip into memory (no temp file needed)
    download_url = zip_info["download_url"]
    response     = requests.get(download_url, stream=True)

    # Extract CSV from the zip directly into data/raw/
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        # Find the CSV file inside the zip
        csv_files_in_zip = [f for f in zf.namelist() if f.endswith(".csv")]

        if not csv_files_in_zip:
            print(f"  No CSV found inside {zip_name} — skipping")
            return None

        # Extract just the CSV (ignore metadata.json)
        csv_file_in_zip = csv_files_in_zip[0]
        with zf.open(csv_file_in_zip) as src, open(local_csv, "wb") as dst:
            dst.write(src.read())

    print(f"  Saved → {local_csv}")
    return local_csv


def run_extract():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    print("Scanning repo for zip files...")
    all_zips = get_all_zip_files(REPO_BASE_URL)
    print(f"Found {len(all_zips)} zip files in repo\n")

    new_files = []
    for zip_info in all_zips:
        result = download_and_extract_zip(zip_info)
        if result:
            new_files.append(result)

    print(f"\nDone. Downloaded and extracted {len(new_files)} new CSV files.")
    return new_files


if __name__ == "__main__":
    run_extract()