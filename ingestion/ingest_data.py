import json
import duckdb
from shopify_client import get_paginated, get_valid_access_token
from datetime import datetime, timedelta, timezone

DB_PATH = "shopify.duckdb"
SHOP_DOMAIN = "okezie-ben-john.myshopify.com"

batch_no = int(datetime.now(timezone.utc).timestamp())

def ensure_metadata_objects(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (
            source TEXT PRIMARY KEY,
            last_updated_at TIMESTAMP        
        );            
    """)

def ensure_raw_table(
        con: duckdb.DuckDBPyConnection,
        source: str,
        table_name: str,
) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.{table_name} (
            shop_domain TEXT,
            {source}_id BIGINT,
            updated_at TIMESTAMP,
            ingested_at TIMESTAMP,
            payload_json TEXT,
            batch_no BIGINT
        );                
    """)

def get_last_updated_at(
        con: duckdb.DuckDBPyConnection,
        source: str,
):
    row = con.execute("""
                SELECT last_updated_at
                FROM pipeline_state
                WHERE source = ?
            """, [source]).fetchone()
    return row[0] if row else None

def update_pipeline_state(
        con: duckdb.DuckDBPyConnection,
        source: str,
        last_updated_at,
) -> None:
    con.execute("""
        INSERT INTO pipeline_state (source, last_updated_at)
        VALUES (?, ?)
        ON CONFLICT(source) DO UPDATE
        SET last_updated_at = excluded.last_updated_at
    """, [source, last_updated_at])


def ingest_shopify_data(source: str, table_name: str) -> None:
    print(f"Pulling {source}s...")

    con = duckdb.connect(DB_PATH)

    ensure_metadata_objects(con)
    ensure_raw_table(con, source, table_name)

    last_updated_at = get_last_updated_at(con, source)
    params={"limit":250}
    if last_updated_at:
        params["updated_at_min"] = last_updated_at.isoformat()
    token = get_valid_access_token()
    data = get_paginated(f"{source}s.json", params=params, access_token=token)

    print(f"Fetched {len(data)} {source}s")

    max_updated_at_in_batch = None
    print("where max_updated_at_in_batch is None: ", max_updated_at_in_batch)
    now = datetime.now()

    for item in data:
        item_updated_at = item.get("updated_at")
        con.execute(f"""
            INSERT INTO raw.{table_name}
            (shop_domain, {source}_id, updated_at, ingested_at, payload_json, batch_no)
            VALUES (?, ?, ?, ?, ?, ?)
    """, (
        SHOP_DOMAIN,
        item.get("id"),
        item_updated_at,
        now,
        json.dumps(item),
        batch_no
    ))
    
        if item_updated_at:
            item_updated_at_dt = datetime.fromisoformat(
                item_updated_at.replace("Z", "+00:00")
            ).astimezone(timezone.utc).replace(tzinfo=None)

            if (
                max_updated_at_in_batch is None
                or item_updated_at_dt > max_updated_at_in_batch
            ):
                max_updated_at_in_batch = item_updated_at_dt

    if max_updated_at_in_batch is not None:
        max_updated_at_in_batch += timedelta(seconds=1)
        print(f"Updating pipeline state for {source} with last_updated_at: {max_updated_at_in_batch}")
        update_pipeline_state(con, source, max_updated_at_in_batch)
            
    con.close()

    # print(f"Ingested {len(data)} {source}s into raw.{table_name}")
    # print(f"Batch no: {batch_no}")
    # print(f"Latest updated_at saved: {max_updated_at_in_batch}")