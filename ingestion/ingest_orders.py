import json
import duckdb
from shopify_ingestion.shopify_client import get_paginated
from datetime import datetime

def main():
    print("Pulling orders...")

    orders = get_paginated(
        "orders.json",
        params={
            "status":"any",
            "limit": 250
        }
    )

    print(f"Fetched {len(orders)} orders")

    con = duckdb.connect("shopify.duckdb")

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_shopify_orders (
                shop_domain TEXT,
                order_id BIGINT,
                updated_at TIMESTAMP,
                ingested_at TIMESTAMP,
                payload_json TEXT
            )
    """)

    shop_domain = "okezie-ben-john.myshopify.com"
    now = datetime.now()

    for order in orders:
        con.execute("""
            INSERT INTO raw_shopify_orders
                    VALUES (?, ?, ?, ?, ?)
        """, (
            shop_domain,
            order["id"],
            order["updated_at"],
            now,
            json.dumps(order)
        ))
    con.close()
    print("Orders loaded into DUCKDB.")

if __name__ == "__main__":
    main()