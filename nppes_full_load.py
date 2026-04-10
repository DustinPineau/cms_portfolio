from datetime import datetime
from dateutil.relativedelta import relativedelta
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

def get_download_url():
    now = datetime.now()
    for i in range(2):
        date = now - relativedelta(months=i)
        month = date.strftime("%B")
        year = date.strftime("%Y")
        url = f"https://download.cms.gov/nppes/NPPES_Data_Dissemination_{month}_{year}_V2.zip"
        response = requests.head(url)
        if response.status_code == 200:
            print(f"Found file: {url}")
            return url, month, year
    raise Exception("Could not find a valid NPPES download URL for current or previous month") 

def download_file(url, month, year):
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
        url, month, year = get_download_url()
        filepath = download_file(url, month, year)
        extracted_path = extract_file(filepath)
        load_file(conn, extracted_path)
        cleanup(filepath, extracted_path)
        conn.close()
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
