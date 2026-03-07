import os
import requests
from dotenv import load_dotenv

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION")

BASE_URL = f"https://{STORE}/admin/api/{API_VERSION}"

HEADERS = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json"
}

def get_paginated(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    results = []

    while url:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()

        data = response.json()
        key = list(data.keys())[0]  # Get the first key which contains the data
        results.extend(data[key])

        # Pagination handling
        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            url = link_header.split(";")[0].strip("<>")
            params = None  # Clear params for subsequent requests
        else:
            url = None
    return results