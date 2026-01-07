import requests
import json
import re
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pytz
import time
from datetime import datetime, date, timedelta
import os
import argparse

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DB = os.getenv("ODOO_DB")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

# -------- Dates (from GitHub Action inputs or default) --------
local_tz = pytz.timezone("Asia/Dhaka")
DATE_FROM_DEFAULT = "2025-07-26"
DATE_TO_DEFAULT = (datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d")

parser = argparse.ArgumentParser()
parser.add_argument("--from_date", type=str, default=DATE_FROM_DEFAULT)
parser.add_argument("--to_date", type=str, default=DATE_TO_DEFAULT)
args = parser.parse_args()

DATE_FROM = args.from_date
DATE_TO = args.to_date

COMPANY_IDS = [1, 3]  # 1 = Zipper, 3 = Metal Trims

# ========= GOOGLE SHEET CONFIG ==========
SERVICE_ACCOUNT_FILE = "gcreds.json"   # GitHub Action will create this from secret
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# ========= START SESSION ==========
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# --------- Helper functions ----------

def safe_post_json(session, url, payload=None, headers=None, retries=3, timeout=60):
    """
    POST json payload and return parsed JSON dict.
    Retries on network/5xx or invalid JSON up to retries times.
    Returns parsed json dict on success, or None on final failure.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, json=payload, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            print(f"RequestException on attempt {attempt} for {url}: {e}")
            if attempt < retries:
                sleep_t = min(60, 2 ** attempt)
                print(f" retrying in {sleep_t}s ...")
                time.sleep(sleep_t)
                continue
            else:
                print(" final failure (network).")
                return None

        if resp.status_code >= 500:
            print(f"Server error {resp.status_code} on attempt {attempt} for {url}: {resp.text[:300]}")
            if attempt < retries:
                sleep_t = min(60, 2 ** attempt)
                print(f" retrying in {sleep_t}s ...")
                time.sleep(sleep_t)
                continue
            else:
                print(" final failure (server error).")
                return None

        # try parse JSON
        try:
            data = resp.json()
            return data
        except ValueError:  # JSONDecodeError
            print(f"Invalid JSON on attempt {attempt} for {url}. Status: {resp.status_code}. Response start:\n{resp.text[:500]}")
            if attempt < retries:
                sleep_t = min(60, 2 ** attempt)
                print(f" retrying in {sleep_t}s ...")
                time.sleep(sleep_t)
                continue
            else:
                print(" final failure (invalid JSON).")
                return None


def download_report_with_retries(session, url, data, headers=None, max_attempts=5, timeout=60):
    """
    POST form/data to download endpoint. If returned content is XLSX (or ZIP/PK signature),
    return resp. Otherwise retry up to max_attempts when status is 5xx or invalid content.
    Returns resp on final attempt even if not valid (caller decides).
    """
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.post(url, data=data, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            print(f"Download RequestException on attempt {attempt}: {e}")
            if attempt < max_attempts:
                sleep_t = min(60, 2 ** attempt)
                print(f" retrying download in {sleep_t}s ...")
                time.sleep(sleep_t)
                continue
            else:
                print(" final download failure (network).")
                return None

        content_type = resp.headers.get("content-type", "")
        # heuristics: check content type or file bytes (xlsx files start with PK because they are ZIP)
        is_xlsx_by_type = "openxmlformats-officedocument.spreadsheetml.sheet" in content_type.lower()
        is_zip_header = isinstance(resp.content, (bytes, bytearray)) and resp.content.startswith(b"PK")

        if resp.status_code == 200 and (is_xlsx_by_type or is_zip_header):
            return resp

        # If 5xx or Bad Gateway, retry
        print(f"Attempt {attempt} - download returned status {resp.status_code}, content-type: {content_type}")
        # show a snippet safely (text may be HTML)
        try:
            snippet = resp.text[:500]
        except Exception:
            snippet = repr(resp.content[:200])
        print(" Response snippet:", snippet)

        if attempt < max_attempts:
            sleep_t = min(60, 2 ** attempt)
            print(f" retrying download in {sleep_t}s ...")
            time.sleep(sleep_t)
            continue
        else:
            print(" final download attempt failed.")
            return resp


def smart_fix_dates_in_dataframe(df, date_from_str, date_to_str):
    """
    Intelligently fix dates in the dataframe based on the date range.
    - Converts date columns to proper datetime format with correct years
    - Uses the date_from and date_to to determine the correct year for each month
    """
    from_date = pd.to_datetime(date_from_str)
    to_date = pd.to_datetime(date_to_str)
    
    print(f"🗓️ Date range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
    
    total_fixes = 0
    
    # Month name mapping
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    # Iterate through all columns
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            # Check if this column contains date-like strings
            sample = df[col].dropna().astype(str).head(10)
            
            # Look for patterns like "26 Jul Fri" or "05 Jan Mon"
            date_pattern = r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\w{3}'
            
            has_dates = any(re.search(date_pattern, str(val), re.IGNORECASE) for val in sample)
            
            if has_dates:
                print(f"🔍 Found date column: '{col}'")
                fixed_values = []
                
                for idx, val in df[col].items():
                    if pd.isna(val):
                        fixed_values.append(val)
                        continue
                    
                    val_str = str(val)
                    match = re.search(date_pattern, val_str, re.IGNORECASE)
                    
                    if match:
                        # Extract day and month
                        parts = val_str.split()
                        day = parts[0]
                        month = parts[1]
                        
                        # Determine correct year based on month
                        month_num = month_names.index(month) + 1 if month in month_names else 1
                        
                        # If month is >= from_date.month, use from_date.year
                        # Otherwise use to_date.year (for wrap-around like Jul-Dec 2025, Jan 2026)
                        if from_date.year == to_date.year:
                            year = from_date.year
                        elif month_num >= from_date.month:
                            year = from_date.year
                        else:
                            year = to_date.year
                        
                        # Reconstruct the date string with correct year
                        new_val = f"{day} {month} {year} {parts[2] if len(parts) > 2 else ''}"
                        fixed_values.append(new_val.strip())
                        total_fixes += 1
                    else:
                        fixed_values.append(val)
                
                df[col] = fixed_values
                print(f"  ✅ Fixed {total_fixes} date values in column '{col}'")
    
    print(f"📊 Total date fixes applied: {total_fixes}")
    return df


# ---------------------- Step 1: Login (with safe JSON handling)
login_url = f"{ODOO_URL}/web/session/authenticate"
login_payload = {
    "jsonrpc": "2.0",
    "params": {"db": DB, "login": USERNAME, "password": PASSWORD}
}
login_result = safe_post_json(session, login_url, payload=login_payload, retries=3, timeout=30)
if not login_result:
    print("❌ Login failed (no JSON response). Exiting.")
    raise SystemExit(1)

uid = login_result.get("result", {}).get("uid")
print("✅ Logged in, UID =", uid)

# ---------------------- Step 2: Get CSRF token (safe)
resp = session.get(f"{ODOO_URL}/web", timeout=30)
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
    onchange_data = safe_post_json(session, onchange_url, payload=onchange_payload, retries=3, timeout=30)
    if not onchange_data:
        print(f"❌ Failed to get onchange defaults for company {company_id}. Skipping this company.")
        continue
    wizard_defaults = onchange_data.get("result", {}).get("value", {})
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
    web_save_data = safe_post_json(session, web_save_url, payload=web_save_payload, retries=3, timeout=30)
    if not web_save_data:
        print(f"❌ Failed to save wizard for company {company_id}. Skipping this company.")
        continue

    # extract wizard id robustly
    wizard_id = None
    result_obj = web_save_data.get("result")
    if isinstance(result_obj, list) and len(result_obj) > 0 and isinstance(result_obj[0], dict):
        wizard_id = result_obj[0].get("id")
    elif isinstance(result_obj, dict):
        wizard_id = result_obj.get("id")
    print("✅ Wizard saved, ID =", wizard_id)
    if not wizard_id:
        print(f"❌ No wizard_id returned for company {company_id}. Skipping.")
        continue

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
    call_button_data = safe_post_json(session, call_button_url, payload=call_button_payload, retries=3, timeout=60)
    if not call_button_data:
        print(f"❌ Call button failed for company {company_id}. Skipping.")
        continue
    report_info = call_button_data.get("result", {})
    report_name = report_info.get("report_name") or report_info.get("report")
    print("✅ Report generated:", report_name)

    # ---------------------- Step 6: Download report (with retry up to 5 attempts)
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

    print(f"Attempting download for company {company_id} (up to 5 attempts)...")
    resp = download_report_with_retries(session, download_url, data=download_payload, headers=headers, max_attempts=5, timeout=120)

    if resp and resp.status_code == 200 and ("openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers.get("content-type", "").lower() or resp.content.startswith(b"PK")):
        company_label = "Zipper" if company_id == 1 else "Metal_Trims"
        filename = f"ot_analysis_{company_label}_{DATE_FROM}_to_{DATE_TO}.xlsx"
        with open(filename, "wb") as f:
            f.write(resp.content)
        print(f"✅ Report downloaded as {filename}")

        # ---------------------- Step 7: Push to Google Sheets ----------------------
        try:
            # Read the Excel file
            df_cost = pd.read_excel(filename, sheet_name=1)
            
            print(f"\n📊 DataFrame shape: {df_cost.shape}")
            print(f"📋 First few columns: {df_cost.columns.tolist()[:10]}")
            print(f"\n🔍 Sample of row 2 (date header row):")
            print(df_cost.iloc[2, :20].to_string())
            
            # Smart fix dates based on the date range
            print(f"\n🔧 Applying smart date fixing...")
            df_cost = smart_fix_dates_in_dataframe(df_cost, DATE_FROM, DATE_TO)
            
            print(f"\n✅ Sample after fixing:")
            print(df_cost.iloc[2, :20].to_string())
            
            # Open Google Sheet
            sheet_new = client.open_by_key("1-kBuln5CnKucuHqYG4vvgttJ8DqeJALvr4TjAYuVkXs")

            if company_id == 1:  # Zipper
                worksheet_new = sheet_new.worksheet("ZIP_OT_DATA")
                clear_range = "B1:IA1000"
            else:  # Metal Trims
                worksheet_new = sheet_new.worksheet("MT_OT_DATA")
                clear_range = "B1:IA1000"
            
            # Clear existing data
            worksheet_new.batch_clear([clear_range])
            print(f"✅ Cleared range {clear_range}")
            
            # Write dataframe to Google Sheets
            set_with_dataframe(worksheet_new, df_cost, row=1, col=2, include_index=False, include_column_header=True)
            print(f"✅ Data pushed to Google Sheets for company {company_id}")
            
        except Exception as e:
            print(f"❌ Failed to process/upload data for company {company_id}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    else:
        # If download failed after retries
        status = resp.status_code if resp is not None else "No response"
        snippet = ""
        try:
            snippet = resp.text[:500] if resp else "<no response>"
        except Exception:
            snippet = "<binary content or no response>"
        print(f"❌ Download failed after retries for company {company_id}. Status: {status}. Snippet: {snippet}")
        print(" moving to next company...\n")
        continue

print("\n✅ All companies processed.")