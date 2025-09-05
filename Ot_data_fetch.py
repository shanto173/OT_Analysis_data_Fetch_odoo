import requests
import json
import re
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pytz
import time
from datetime import datetime
import os

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("ODOO_DB")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

# -------- Dates (from GitHub Action inputs or default) --------
DATE_FROM = os.getenv("FROM_DATE", "2025-08-26")
DATE_TO = os.getenv("TO_DATE", "2025-09-25")

COMPANY_IDS = [1, 3]  # 1 = Zipper, 3 = Metal Trims

# ========= GOOGLE SHEET CONFIG ==========
SERVICE_ACCOUNT_FILE = "gcread.json"   # GitHub Action will create this from secret
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# ========= START SESSION ==========
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# ---------------------- Step 1: Login
login_url = f"{ODOO_URL}/web/session/authenticate"
login_payload = {
    "jsonrpc": "2.0",
    "params": {"db": DB, "login": USERNAME, "password": PASSWORD}
}
resp = session.post(login_url, json=login_payload)
login_result = resp.json()
uid = login_result.get("result", {}).get("uid")
print("✅ Logged in, UID =", uid)

# ---------------------- Step 2: Get CSRF token
resp = session.get(f"{ODOO_URL}/web")
match = re.search(r'var odoo = {\s*csrf_token: "([A-Za-z0-9]+)"', resp.text)
csrf_token = match.group(1) if match else None
print("✅ CSRF token =", csrf_token)

# ---------------------- Iterate over companies
for company_id in COMPANY_IDS:
    print(f"\n--- Processing company_id {company_id} ---")

    # ---------------------- Step 3: Onchange to get defaults
    onchange_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/onchange"
    onchange_payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "onchange",
            "args": [[], {}, [], {
                "report_type": {}, "date_from": {}, "date_to": {},
                "is_company": {}, "atten_type": {}, "types": {}, "mode_type": {},
                "employee_id": {"fields": {"display_name": {}}},
                "mode_company_id": {"fields": {"display_name": {}}},
                "category_id": {"fields": {"display_name": {}}},
                "department_id": {"fields": {"display_name": {}}},
                "company_all": {}
            }],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                                   "allowed_company_ids": [company_id], "default_is_company": False}}
        }
    }
    resp = session.post(onchange_url, json=onchange_payload)
    wizard_defaults = resp.json().get("result", {}).get("value", {})
    print("✅ Onchange defaults:", wizard_defaults)

    # ---------------------- Step 4: Save wizard
    web_save_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    web_save_payload = {
        "id": 3,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "web_save",
            "args": [[], {
                "report_type": "ot_analysis",
                "date_from": DATE_FROM,
                "date_to": DATE_TO,
                "is_company": False,
                "atten_type": False,
                "types": False,
                "mode_type": "company",
                "employee_id": False,
                "mode_company_id": company_id,
                "category_id": False,
                "department_id": False,
                "company_all": "allcompany"
            }],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                            "allowed_company_ids": [company_id], "default_is_company": False},
                "specification": {
                    "report_type": {}, "date_from": {}, "date_to": {}, "is_company": {},
                    "atten_type": {}, "types": {}, "mode_type": {},
                    "employee_id": {"fields": {"display_name": {}}},
                    "mode_company_id": {"fields": {"display_name": {}}},
                    "category_id": {"fields": {"display_name": {}}},
                    "department_id": {"fields": {"display_name": {}}},
                    "company_all": {}
                }
            }
        }
    }
    resp = session.post(web_save_url, json=web_save_payload)
    wizard_id = resp.json().get("result", [{}])[0].get("id")
    print("✅ Wizard saved, ID =", wizard_id)

    # ---------------------- Step 5: Call report button
    call_button_url = f"{ODOO_URL}/web/dataset/call_button"
    call_button_payload = {
        "id": 4,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": REPORT_BUTTON_METHOD,
            "args": [[wizard_id]],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka",
                                   "uid": uid, "allowed_company_ids": [company_id], "default_is_company": False}}
        }
    }
    resp = session.post(call_button_url, json=call_button_payload)
    report_info = resp.json().get("result", {})
    report_name = report_info.get("report_name")
    print("✅ Report generated:", report_name)

    # ---------------------- Step 6: Download report
    download_url = f"{ODOO_URL}/report/download"
    options = {
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "mode_company_id": company_id,
        "department_id": False,
        "category_id": False,
        "employee_id": False,
        "report_type": "ot_analysis",
        "atten_type": False,
        "types": False,
        "is_company": False
    }
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [company_id],
        "active_model": MODEL,
        "active_id": wizard_id,
        "active_ids": [wizard_id],
        "default_is_company": False
    }
    report_path = f"/report/xlsx/{report_name}?options={json.dumps(options)}&context={json.dumps(context)}"
    download_payload = {
        "data": json.dumps([report_path, "xlsx"]),
        "context": json.dumps(context),
        "token": "dummy-because-api-expects-one",
        "csrf_token": csrf_token
    }
    headers = {"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}

    resp = session.post(download_url, data=download_payload, headers=headers, timeout=60)
    company_label = "Zipper" if company_id == 1 else "Metal_Trims"

    if resp.status_code == 200 and "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers.get("content-type", ""):
        filename = f"ot_analysis_{company_label}_{DATE_FROM}_to_{DATE_TO}.xlsx"
        with open(filename, "wb") as f:
            f.write(resp.content)
        print(f"✅ Report downloaded as {filename}")

        # ---------------------- Step 7: Push to Google Sheets ----------------------
        df_cost = pd.read_excel(filename)

        sheet_new = client.open_by_key("1-kBuln5CnKucuHqYG4vvgttJ8DqeJALvr4TjAYuVkXs")

        if company_id == 1:  # Zipper
            worksheet_new = sheet_new.worksheet("ZIP_OT_DATA")
            clear_range = "B1:IA1000"
        else:  # Metal Trims
            worksheet_new = sheet_new.worksheet("MT_OT_DATA")
            clear_range = "B1:HI1000"

        if df_cost.empty:
            print(f"Skip: DataFrame is empty for {company_label}, not pasting.")
        else:
            worksheet_new.batch_clear([clear_range])
            time.sleep(4)
            set_with_dataframe(worksheet_new, df_cost, row=1, col=2)
            print(f"✅ Data pasted to Google Sheet ({company_label}).")

            local_tz = pytz.timezone("Asia/Dhaka")
            local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
            worksheet_new.update("E1", [[f"{local_time}"]])
            print(f"✅ Timestamp updated for {company_label}: {local_time}")
    else:
        print("❌ Download failed", resp.status_code, resp.text[:500])
