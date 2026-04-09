import httpx
import asyncio
import os
import psycopg2
import json
import time
from datetime import datetime, timezone

def get_connection():
    conn = psycopg2.connect(
        host="192.168.122.10",
        port=5432,
        dbname="portfolio",
        user="dustin",
        password=os.environ.get("DB_PASSWORD")
    )
    return conn

def get_pipeline_last_run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT last_run
        FROM raw.pipeline_metadata
        WHERE pipeline_name = 'nppes_client'
    """)
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        return None
    return row[0]

def get_npis(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT npi
        FROM analytics.dim_provider
        WHERE is_current = true
    """)
    npis = [row[0] for row in cursor.fetchall()]
    cursor.close()
    print(f"Found {len(npis)} NPIs to process")
    return npis

async def fetch_npi(client, npi):
    url = f"https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1"
    for attempt in range(max_retries):
        try:
            response = await client.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                print(f"Failed to fetch NPI {npi}: {e}")
                return None

async def check_and_insert(client, conn, npi, last_run):
    data = await fetch_npi(client, npi)

    if not data.get("results"):
        return

    result = data["results"][0]
    last_updated_epoch = int(result.get("last_updated_epoch", 0)) / 1000

    if last_run is None or last_updated_epoch > last_run.timestamp():
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO raw.raw_nppes (data) VALUES (%s)",
            [json.dumps(result)]
        )
        conn.commit()
        cursor.close()

async def fetch_all_npis(conn, npis, last_run, batch_size=50):
    async with httpx.AsyncClient() as client:
        for i in range(0, len(npis), batch_size):
            batch = npis[i:i + batch_size]
            tasks = [check_and_insert(client, conn, npi, last_run) for npi in batch]
            await asyncio.gather(*tasks)
            print(f"Progress: {min(i + batch_size, len(npis))}/{len(npis)}")

def update_pipeline_last_run(conn):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO raw.pipeline_metadata (pipeline_name, last_run)
        VALUES ('nppes_client', NOW())
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_run = NOW()
    """)
    conn.commit()
    cursor.close()
    print("Updated pipeline_metadata last_run")

def main():
    conn = get_connection()
    last_run = get_pipeline_last_run(conn)
    npis = get_npis(conn)
    asyncio.run(fetch_all_npis(conn, npis, last_run))
    update_pipeline_last_run(conn)
    conn.close()

if __name__ == "__main__":
    main()
