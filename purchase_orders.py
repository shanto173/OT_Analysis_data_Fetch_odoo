import os
import json
import requests
import pandas as pd
import glob
import time
from datetime import datetime
import pytz
import logging as log

import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account

# ----------------------------
# Logging
# ----------------------------
log.basicConfig(level=log.INFO)

# ----------------------------
# Odoo credentials (from GitHub secrets or env)
# ----------------------------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("USERNAME")
ODOO_PASSWORD = os.getenv("PASSWORD")

# ----------------------------
# Field mapping
# ----------------------------
FIELDS = {
    "company_id": "Company",
    "create_uid": "Created by",
    "create_date": "Created on",
    "x_studio_currency": "Currency.",
    "x_studio_gate_entry": "Gate Entry",
    "incoterm_id": "Incoterm",
    "next_approver": "Next Approver",
    "name": "Order Reference",
    "x_studio_order_status": "Order Status",
    "x_studio_pi_no": "PI No.",
    "priority": "Priority",
    "origin": "Source Document",
    "state": "Status",
    "amount_total": "Total",
    "partner_id": "Vendor",
    "shipment_mode": "Shipment Mode",
    "payment_term_id": "Payment Terms"
}

# ----------------------------
# Step 1: Authenticate via Odoo session
# ----------------------------
auth_url = f"{ODOO_URL}/web/session/authenticate"
headers = {"Content-Type": "application/json"}

auth_payload = {
    "jsonrpc": "2.0",
    "params": {
        "db": ODOO_DB,
        "login": ODOO_USERNAME,
        "password": ODOO_PASSWORD
    }
}

session = requests.Session()
resp = session.post(auth_url, headers=headers, data=json.dumps(auth_payload))
resp.raise_for_status()
auth_result = resp.json()
if not auth_result.get("result") or not auth_result["result"].get("uid"):
    raise Exception("Login failed. Check credentials or access rights.")

uid = auth_result["result"]["uid"]
log.info(f"✅ Logged in UID: {uid}")

# ----------------------------
# Step 2: Fetch purchase.order data (paginated)
# ----------------------------
fields_list = list(FIELDS.keys())
limit = 1000
offset = 0
all_records = []

while True:
    data_url = f"{ODOO_URL}/web/dataset/call_kw/purchase.order/search_read"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "purchase.order",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "fields": fields_list,
                "limit": limit,
                "offset": offset,
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid}
            }
        },
        "id": 2
    }

    resp = session.post(data_url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    resp_json = resp.json()

    if "result" not in resp_json:
        log.error(f"Error fetching purchase orders: {resp_json.get('error')}")
        break

    records = resp_json["result"]
    if not records:
        break

    all_records.extend(records)
    offset += limit
    log.info(f"Fetched {len(all_records)} records so far...")

log.info(f"Total records fetched: {len(all_records)}")

# ----------------------------
# Step 3: Clean many2one fields & nulls
# ----------------------------
def clean_value(val):
    if isinstance(val, list) and len(val) == 2:
        return val[1]
    elif val is None or val is False:
        return ""
    else:
        return val

for rec in all_records:
    for key in rec.keys():
        rec[key] = clean_value(rec[key])

# ----------------------------
# Step 4: Convert to DataFrame
# ----------------------------
df = pd.DataFrame(all_records)
df.rename(columns=FIELDS, inplace=True)

# ----------------------------
# Step 5: Save to Excel
# ----------------------------
os.makedirs("downloads", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"downloads/purchase_orders_{timestamp}.xlsx"
df.to_excel(filename, index=False)
log.info(f"✅ Downloaded & cleaned file saved: {filename}")

# ----------------------------
# Step 6: Find latest file matching pattern
# ----------------------------
list_of_files = glob.glob("downloads/purchase_orders_*.xlsx")
latest_file = max(list_of_files, key=os.path.getctime)
log.info(f"✅ Latest file selected: {latest_file}")

# ----------------------------
# Step 7: Paste into Google Sheet
# ----------------------------
# Wait a bit to ensure file is ready
time.sleep(5)
df = pd.read_excel(latest_file)
log.info("✅ File loaded into DataFrame.")

# Load Google service account credentials (gcreds.json stored in GitHub Secrets)
scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

creds = service_account.Credentials.from_service_account_file('gcreds.json', scopes=scope)
client = gspread.authorize(creds)

sheet = client.open_by_key("19FTCzNt8cWhy9CXFXM0NmIotlrkiKhIVMtH6MfFNOEM")
worksheet = sheet.worksheet("PO_Status_Data")

if df.empty:
    log.info("Skip: DataFrame is empty, not pasting to sheet.")
else:
    worksheet.clear()
    time.sleep(2)
    set_with_dataframe(worksheet, df)
    log.info("✅ Data pasted to Google Sheet (PO_Status_Data).")

    # Add timestamp
    local_tz = pytz.timezone('Asia/Dhaka')
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AC1", [[f"{local_time}"]])
    log.info(f"✅ Timestamp updated: {local_time}")
