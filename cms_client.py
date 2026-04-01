import requests
import os
import psycopg2
import json
import time

def get_connection():
    conn = psycopg2.connect(
        host="192.168.122.10",
        port=5432,
        dbname="portfolio",
        user="dustin",
        password=os.environ.get("DB_PASSWORD")
    )
    return conn

def fetch_with_retry(url, params=None, max_retries=5, wait=10):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Waiting {wait} seconds...")
            time.sleep(wait)
    raise Exception(f"Failed after {max_retries} attempts")

def get_total_count(dataset_id):
    url=f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data/stats"
    response = fetch_with_retry(url)
    total = response.json()["found_rows"]
    print(f"Dataset total row count: {total}")
    return total

def get_current_offset(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_part_d;")
    count = cursor.fetchone()[0]
    cursor.close()
    print(f"Resuming from offset: {count}")
    return count

def insert_rows(conn, rows):
    if not rows:
        return
    cursor = conn.cursor()
    try:
        for row in rows:
            cursor.execute(
                "INSERT INTO raw_part_d (data) VALUES (%s)",
                [json.dumps(row)]
            )
        conn.commit()
        print(f"Inserted {len(rows)} rows")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting rows: {e}")
        raise
    finally:
        cursor.close()

def fetch_all_pages(conn, dataset_id, total_count,  size=5000):
    url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"
    offset = get_current_offset(conn)

    while offset < total_count:
        params = {
            "size": size,
            "offset": offset,
        }

        response = fetch_with_retry(url, params)
        data = response.json()

        if not data:
            print("No more data received, breaking")
            break
        
        insert_rows(conn, data)
        offset += len(data)
        print(f"Progress: {offset}/{total_count}")

def main():
    dataset_id = "9552739e-3d05-4c1b-8eff-ecabf391e2e5"
    conn = get_connection()
    total_count = get_total_count(dataset_id)
    fetch_all_pages(conn, dataset_id, total_count)
    conn.close()
    print("done")

if __name__ == "__main__":
    main()
