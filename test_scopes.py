import os, requests
from dotenv import load_dotenv

load_dotenv()
STORE = os.getenv("SHOPIFY_STORE")
TOKEN = os.getenv("SHOPIFY_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION")

url = f"https://{STORE}/admin/api/{VERSION}/oauth/access_scopes.json"
r = requests.get(url, headers={"X-Shopify-Access-Token": TOKEN})
print(r.status_code)
print(r.text)