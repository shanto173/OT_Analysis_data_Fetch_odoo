import os
import requests, json, re
import pandas as pd
from datetime import datetime
import pytz
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
import logging as log
import time

# -------------------- Setup Logging --------------------
log.basicConfig(level=log.INFO)

# -------------------- Read Environment --------------------
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

FROM_DATE = os.environ.get("FROM_DATE", "2025-08-26")
TO_DATE = os.environ.get("TO_DATE")
if not TO_DATE:
    TO_DATE = datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"
REPORT_TYPE = "ot_analysis"
COMPANY_IDS = [1, 3]  # 1 = Zipper, 3 = Metal Trims

# -------------------- Start Session --------------------
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# -------------------- Login --------------------
log.info("Logging in...")
resp = session.post(f"{ODOO_URL}/web/session/authenticate", json={
    "jsonrpc": "2.0",
    "params": {"db": DB, "login": USERNAME, "password": PASSWORD}
})
login_resp = resp.json()
if login_resp.get("error"):
    log.error("❌ Login error: %s", login_resp["error"])
    raise Exception("Login failed. Check credentials or access rights.")
uid = login_resp.get("result", {}).get("uid")
if not uid:
    log.error("❌ UID is None. Login failed.")
    raise Exception("Login failed, UID is None.")
log.info("✅ Logged in, UID = %s", uid)

# -------------------- Get CSRF Token --------------------
resp = session.get(f"{ODOO_URL}/web")
match = re.search(r'var odoo = {\s*csrf_token: "([A-Za-z0-9]+)"', resp.text)
csrf_token = match.group(1) if match else None
if not csrf_token:
    raise Exception("❌ CSRF token not found.")
log.info("✅ CSRF token = %s", csrf_token)

# -------------------- Google Sheet Auth --------------------
creds = service_account.Credentials.from_service_account_file("gcreds.json", scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
])
client = gspread.authorize(creds)

# -------------------- Process Each Company --------------------
for company_id in COMPANY_IDS:
    company_label = "Zipper" if company_id == 1 else "Metal_Trims"
    log.info("\n--- Processing %s ---", company_label)

    # -------------------- Onchange Wizard --------------------
    onchange_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/onchange"
    session.post(onchange_url, json={
        "id":1,"jsonrpc":"2.0","method":"call","params":{
            "model":MODEL,"method":"onchange","args":[[], {}, [], {}],
            "kwargs":{"context":{"lang":"en_US","tz":"Asia/Dhaka","uid":uid,"allowed_company_ids":[company_id],"default_is_company":False}}
        }
    })

    # -------------------- Save Wizard --------------------
    web_save_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    resp = session.post(web_save_url, json={
        "id":3,"jsonrpc":"2.0","method":"call","params":{
            "model":MODEL,"method":"web_save","args":[[], {
                "report_type": REPORT_TYPE,
                "date_from": FROM_DATE,
                "date_to": TO_DATE,
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
            "kwargs":{"context":{"lang":"en_US","tz":"Asia/Dhaka","uid":uid,"allowed_company_ids":[company_id],"default_is_company":False}}
        }
    })
    wizard_id = resp.json().get("result", [{}])[0].get("id")
    if not wizard_id:
        log.error("❌ Wizard ID is None. Skipping %s", company_label)
        continue
    log.info("✅ Wizard saved, ID = %s", wizard_id)

    # -------------------- Generate Report --------------------
    call_button_url = f"{ODOO_URL}/web/dataset/call_button"
    resp = session.post(call_button_url, json={
        "id":4,"jsonrpc":"2.0","method":"call","params":{
            "model":MODEL,"method":REPORT_BUTTON_METHOD,"args":[[wizard_id]],
            "kwargs":{"context":{"lang":"en_US","tz":"Asia/Dhaka","uid":uid,"allowed_company_ids":[company_id],"default_is_company":False}}
        }
    })
    report_name = resp.json().get("result", {}).get("report_name")
    if not report_name:
        log.error("❌ Report name is None. Skipping %s", company_label)
        continue
    log.info("✅ Report generated: %s", report_name)

    # -------------------- Download Report --------------------
    download_url = f"{ODOO_URL}/report/download"
    options = {
        "date_from": FROM_DATE, "date_to": TO_DATE,
        "mode_company_id": company_id,
        "department_id": False,
        "category_id": False,
        "employee_id": False,
        "report_type": REPORT_TYPE,
        "atten_type": False, "types": False, "is_company": False
    }
    context = {"lang":"en_US","tz":"Asia/Dhaka","uid":uid,"allowed_company_ids":[company_id],
               "active_model":MODEL,"active_id":wizard_id,"active_ids":[wizard_id],"default_is_company":False}
    report_path = f"/report/xlsx/{report_name}?options={json.dumps(options)}&context={json.dumps(context)}"
    download_payload = {"data": json.dumps([report_path,"xlsx"]), "context": json.dumps(context), "token":"dummy-because-api-expects-one", "csrf_token": csrf_token}
    resp = session.post(download_url, data=download_payload, headers={"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}, timeout=60)

    if resp.status_code == 200 and "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers.get("content-type",""):
        filename = f"{REPORT_TYPE}_{company_label}_{FROM_DATE}_to_{TO_DATE}.xlsx"
        with open(filename,"wb") as f: f.write(resp.content)
        log.info("✅ Report downloaded as %s", filename)
    else:
        log.error("❌ Download failed %s %s", resp.status_code, resp.text[:500])
        continue

    # -------------------- Load & Paste to Google Sheet --------------------
    df = pd.read_excel(filename)
    if df.empty: 
        log.warning("Skip: DataFrame is empty for %s", company_label)
        continue

    sheet_key = "1-kBuln5CnKucuHqYG4vvgttJ8DqeJALvr4TjAYuVkXs"
    sheet = client.open_by_key(sheet_key)
    if company_label == "Zipper":
        worksheet = sheet.worksheet("ZIP_OT_DATA")
        worksheet.batch_clear(["B1:IA1000"])
    else:
        worksheet = sheet.worksheet("MT_OT_DATA")
        worksheet.batch_clear(["B1:HI1000"])

    time.sleep(2)
    set_with_dataframe(worksheet, df, row=1, col=2, include_index=False)
    local_time = datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("E1", [[f"{local_time}"]])
    log.info("✅ Data pasted & timestamp updated for %s.", company_label)
