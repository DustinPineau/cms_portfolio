from datetime import datetime
import requests
import zipfile
import csv
import psycopg2
import json
import os

def get_connection():
    conn = psycopg2.connect(
        host="192.168.122.10",
        port=5432,
        dbname="portfolio",
        user="dustin",
        password=os.environ.get("DB_PASSWORD")
    )
    return conn

def get_current_month_year():
    now = datetime.now()
    return now.strftime("%B"), now.strftime("%Y")

def download_file(month, year):
    url = f"https://download.cms.gov/nppes/NPPES_Data_Dissemination_{month}_{year}_V2.zip"
    dest = f"/mnt/storage2/Projects/tmp/NPPES_Data_Dissemination_{month}_{year}_V2.zip"
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved to {dest}")
    return dest

def extract_file(filepath):
    print(f"Extracting {filepath}...")
    with zipfile.ZipFile(filepath, 'r') as z:
        z.extractall("/mnt/storage2/Projects/tmp/")
        csv_file = [f for f in z.namelist() if f.startswith("npidata_pfile")][0]
    print("Extraction complete")
    return f"/mnt/storage2/Projects/tmp/{csv_file}"

def load_file(conn, extracted_path):
    print(f"Loading {extracted_path}...")
    cursor = conn.cursor()
    count = 0
    with open(extracted_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute(
                "INSERT INTO raw.raw_nppes (data) VALUES (%s)",
                [json.dumps(row)]
            )    
            count += 1
            if count % 10000 == 0:
                conn.commit()
                print(f"Inserted {count} rows")
    conn.commit()
    cursor.close()
    print(f"Load complete. Total rows: {count}")

def cleanup(filepath, extracted_path):    
    print("Cleaning up temporary files...")
    if os.path.exists(filepath):
        os.remove(filepath)
        print(f"Deleted {filepath}")
    if os.path.exists(extracted_path):
        os.remove(extracted_path)
        print(f"Deleted {extracted_path}")
    print("Cleanup complete")

def main():
    try:
        conn = get_connection()
        month, year = get_current_month_year()
        filepath = download_file(month, year)
        extracted_path = extract_file(filepath)
        load_file(conn, extracted_path)
        cleanup(filepath, extracted_path)
        conn.close()
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
