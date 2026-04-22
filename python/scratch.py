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
            return response
        except Exception as e:
            print(f"Request failed: {e}. Waiting {wait} seconds...")
            time.sleep(wait)
    raise Exception(f"Failed after {max_retries} attempts")
