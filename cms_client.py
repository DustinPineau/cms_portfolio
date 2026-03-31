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

def fetch_with_retry(url, params, max_retries=5, wait=10):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Waiting {wait} seconds...")
            time.sleep(wait)
    raise Exception(f"Failed after {max_retries} attempts")

def get_last_position(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT data->>'Prscrbr_NPI', data->>'Brnd_Name'
        FROM raw_part_d
        ORDER BY (data->>'Prscrbr_NPI')::bigint DESC, data->>'Brnd_Name' DESC
        LIMIT 1;
        """)
    rows = cursor.fetchone()
    cursor.close()
    if rows is None:
        return None, None
    return rows[0], rows[1]

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

def fetch_all_pages(conn, dataset_id, limit=1000):
    url = f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"
    last_npi, last_brnd = get_last_position(conn)
    order = "Prscrbr_NPI,Brnd_Name"
    total_rows = 0    

    while True:
        if last_npi is None:
            params = {
                "$limit": limit,
                "$order": order,
            }
        else:
            params = {
                "$limit": limit,
                "$order": order,
                "$where": f"Prscrbr_NPI::number > '{last_npi}' OR (Prscrbr_NPI::number = '{last_npi}' AND Brnd_Name > '{last_brnd}')"
            }   

        response = fetch_with_retry(url, params)    
        data = response.json()
        
        if not data:
            print("No more data received, breaking")
            break
        
        insert_rows(conn,data)
        total_rows += len(data)
        last_npi = data[-1]["Prscrbr_NPI"]
        last_brnd = data[-1]["Brnd_Name"]                

        print(f"Fetched {len(data)} rows, total={total_rows}, last_npi={last_npi}, last_brnd={last_brnd}")        
        if len(data) < limit:
            print(f"Reached end of dataset (received {len(data)} rows, limit={limit})")
            break

def main():
    conn = get_connection()
    fetch_all_pages(conn, "9552739e-3d05-4c1b-8eff-ecabf391e2e5")
    conn.close()
    print("done")

if __name__ == "__main__":
    main()
