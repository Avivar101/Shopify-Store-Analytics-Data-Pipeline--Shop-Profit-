import json
import duckdb
from shopify_client import get_paginated
from datetime import datetime

def ingest_shopify_data(source, table_name):
    print(f"Pulling {source}s...")

    data = get_paginated(f"{source}s.json", params={"limit":250})

    print(f"Fetched {len(data)} {source}s")

    con = duckdb.connect("shopify.duckdb")

    con.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.{table_name} (
            shop_domain TEXT,
            {source}_id BIGINT,
            updated_at TIMESTAMP,
            ingested_at TIMESTAMP,
            payload_json TEXT
        )
    """)

    shop_domain = "okezie-ben-john.myshopify.com"
    now = datetime.now()

    for item in data:
        con.execute(f"""
            INSERT INTO raw.{table_name}
            VALUES (?, ?, ?, ?, ?)
    """, (
        shop_domain,
        item.get("id"),
        item.get("updated_at"),
        now,
        json.dumps(item)
    ))
        
    con.close()
    print(f"Ingested {len(data)} {source}s into raw.{table_name}")