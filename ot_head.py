import os
import json
import requests
import pandas as pd
import time
from datetime import datetime
import pytz
import logging as log
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from dotenv import load_dotenv
load_dotenv()

# ----------------------------
# Logging
# ----------------------------
log.basicConfig(level=log.INFO)

# ----------------------------
# Odoo credentials (from GitHub secrets or env)
# ----------------------------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# ----------------------------
# Field mapping for attendance
# ----------------------------
FIELDS = {
    "attDate": "Date",
    "employee_id": "Employee",
    "department_id": "Department",
    "com_otHours": "OT Hours ",
    "worked_hours": "Worked Hours",
    "x_studio_category": "Category"
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
print("Auth response:", auth_result)
if not auth_result.get("result") or not auth_result["result"].get("uid"):
    raise Exception("Login failed. Check credentials or access rights.")
uid = auth_result["result"]["uid"]
log.info(f"✅ Logged in UID: {uid}")

# ----------------------------
# Step 2: Fetch all employees with active status (with retry logic)
# ----------------------------
def fetch_all_employees(context, max_retries=10):
    """Fetch all employees and their active status with retry logic"""
    data_url = f"{ODOO_URL}/web/dataset/call_kw/hr.employee/search_read"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "hr.employee",
            "method": "search_read",
            "args": [[]],  # Empty domain to get all employees
            "kwargs": {
                "fields": ["id", "name", "active"],
                "context": context
            }
        },
        "id": 1
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(data_url, headers=headers, data=json.dumps(payload), timeout=60)
            resp.raise_for_status()
            resp_json = resp.json()
            
            if "result" not in resp_json:
                log.error(f"Error fetching employees: {resp_json.get('error')}")
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 60)
                    log.info(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return {}
            
            # Create a dictionary mapping employee_id to active status
            employee_dict = {emp['id']: emp['active'] for emp in resp_json['result']}
            log.info(f"✅ Fetched {len(employee_dict)} employees with active status")
            return employee_dict
            
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            log.error(f"❌ Attempt {attempt}/{max_retries} failed fetching employees: {str(e)}")
            
            if attempt < max_retries:
                wait_time = min(2 ** attempt, 60)
                log.info(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                log.error(f"❌ All {max_retries} attempts failed for fetching employees")
                return {}
    
    return {}

# ----------------------------
# Common settings
# ----------------------------
fields_list = list(FIELDS.keys())
limit = 1000
local_tz = pytz.timezone('Asia/Dhaka')
from_date = "2024-04-01"
to_date = datetime.now(local_tz).strftime("%Y-%m-%d")
domain = ["&", ["attDate", ">=", from_date], ["attDate", "<=", to_date]]

# Function to fetch attendance records for a given context with retry logic
def fetch_attendance(context, employee_dict, max_retries=10):
    offset = 0
    all_records = []
    
    while True:
        data_url = f"{ODOO_URL}/web/dataset/call_kw/hr.attendance/search_read"
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "hr.attendance",
                "method": "search_read",
                "args": [domain],
                "kwargs": {
                    "fields": fields_list,
                    "limit": limit,
                    "offset": offset,
                    "context": context
                }
            },
            "id": 2
        }
        
        # Retry logic for each batch fetch
        records = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.post(data_url, headers=headers, data=json.dumps(payload), timeout=60)
                resp.raise_for_status()
                resp_json = resp.json()
                
                if "result" not in resp_json:
                    log.error(f"Error fetching attendance at offset {offset}: {resp_json.get('error')}")
                    if attempt < max_retries:
                        wait_time = min(2 ** attempt, 60)
                        log.info(f"⏳ Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        log.error(f"❌ Failed to fetch batch at offset {offset} after {max_retries} attempts")
                        return all_records  # Return what we have so far
                
                records = resp_json["result"]
                break  # Success, exit retry loop
                
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                log.error(f"❌ Attempt {attempt}/{max_retries} failed at offset {offset}: {str(e)}")
                
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    wait_time = min(2 ** attempt, 60)
                    log.info(f"⏳ Waiting {wait_time} seconds before retry (current offset: {offset})...")
                    time.sleep(wait_time)
                else:
                    log.error(f"❌ All {max_retries} attempts failed at offset {offset}")
                    log.info(f"💾 Returning {len(all_records)} records fetched before error")
                    return all_records  # Return partial results
        
        # Check if we got records
        if records is None or not records:
            log.info(f"✅ No more records to fetch. Total fetched: {len(all_records)}")
            break
        
        # Add employee active status to each record
        for record in records:
            emp_id = record.get('employee_id')
            if isinstance(emp_id, list) and len(emp_id) >= 1:
                emp_id = emp_id[0]  # Get the ID from [id, name] format
            record['employee_active'] = employee_dict.get(emp_id, True)  # Default to True if not found
        
        all_records.extend(records)
        offset += limit
        log.info(f"📊 Fetched {len(all_records)} records so far (current offset: {offset})...")
        
        # Small delay to avoid overwhelming the server
        time.sleep(0.5)
    
    return all_records

# ----------------------------
# Fetch employees first (for both company contexts)
# ----------------------------
context_14 = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid, "allowed_company_ids": [1, 4], "current_company_id": 1}
employee_dict_14 = fetch_all_employees(context_14)

context_34 = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid, "allowed_company_ids": [3, 4], "current_company_id": 3}
employee_dict_34 = fetch_all_employees(context_34)

# ----------------------------
# Fetch attendance for company ids 1 and 4
# ----------------------------
records_14 = fetch_attendance(context_14, employee_dict_14)
log.info(f"Total records fetched for companies 1 & 4: {len(records_14)}")

# ----------------------------
# Fetch attendance for company ids 3 and 4
# ----------------------------
records_34 = fetch_attendance(context_34, employee_dict_34)
log.info(f"Total records fetched for companies 3 & 4: {len(records_34)}")

# ----------------------------
# Clean many2one fields & nulls
# ----------------------------
def clean_value(val):
    if isinstance(val, list) and len(val) == 2:
        return val[1]
    elif val is None or val is False:
        return ""
    else:
        return val

for rec in records_14:
    for key in rec.keys():
        if key != 'employee_active':  # Don't clean the active field
            rec[key] = clean_value(rec[key])

for rec in records_34:
    for key in rec.keys():
        if key != 'employee_active':  # Don't clean the active field
            rec[key] = clean_value(rec[key])

# ----------------------------
# Convert to DataFrames and rename columns
# ----------------------------
# Update field mapping to include employee active
FIELDS['employee_active'] = 'Employee/Active'

df_14 = pd.DataFrame(records_14)
df_34 = pd.DataFrame(records_34)

# Check if DataFrames are empty
if df_14.empty:
    log.warning("⚠️ No records fetched for companies 1 & 4. Skipping processing.")
    grouped_14 = pd.DataFrame()
else:
    df_14.rename(columns=FIELDS, inplace=True)
    
    # ----------------------------
    # Standardize Date to First Day of Month
    # ----------------------------
    df_14['Date'] = pd.to_datetime(df_14['Date'])
    df_14['Date'] = df_14['Date'].dt.to_period('M').dt.to_timestamp()
    log.info(f"✅ Standardized dates to first day of month for companies 1 & 4")
    
    # ----------------------------
    # Group by specified columns and sum OT and Worked hours
    # ----------------------------
    group_cols = ['Date', 'Employee', 'Department', 'Category', 'Employee/Active']
    grouped_14 = df_14.groupby(group_cols, as_index=False).agg({'OT Hours ': 'sum', 'Worked Hours': 'sum'})
    log.info(f"Grouped data for companies 1 & 4: {len(grouped_14)} rows")

if df_34.empty:
    log.warning("⚠️ No records fetched for companies 3 & 4. Skipping processing.")
    grouped_34 = pd.DataFrame()
else:
    df_34.rename(columns=FIELDS, inplace=True)
    
    # ----------------------------
    # Standardize Date to First Day of Month
    # ----------------------------
    df_34['Date'] = pd.to_datetime(df_34['Date'])
    df_34['Date'] = df_34['Date'].dt.to_period('M').dt.to_timestamp()
    log.info(f"✅ Standardized dates to first day of month for companies 3 & 4")
    
    # ----------------------------
    # Group by specified columns and sum OT and Worked hours
    # ----------------------------
    group_cols = ['Date', 'Employee', 'Department', 'Category', 'Employee/Active']
    grouped_34 = df_34.groupby(group_cols, as_index=False).agg({'OT Hours ': 'sum', 'Worked Hours': 'sum'})
    log.info(f"Grouped data for companies 3 & 4: {len(grouped_34)} rows")

# ----------------------------
# Paste into Google Sheets with Retry Logic
# ----------------------------
# Load Google service account credentials (gcreds.json stored in GitHub Secrets)
scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file('gcreds.json', scopes=scope)
client = gspread.authorize(creds)

# Get the service account email for sharing instructions
service_account_email = creds.service_account_email
log.info(f"📧 Service Account Email: {service_account_email}")
log.info("⚠️  Make sure this email has Editor access to the Google Sheet!")

try:
    sheet = client.open_by_key("1OOwRMvGMgZ0lLsq3VLWmqGWF9WsqLj6N72Bdn-0-PNw")
except PermissionError as e:
    log.error(f"❌ Permission Error: The service account ({service_account_email}) doesn't have access to the spreadsheet.")
    log.error("📝 To fix: Open the Google Sheet and share it with the service account email as an Editor.")
    raise

# ----------------------------
# Function to paste data with retry logic
# ----------------------------
def paste_to_sheet_with_retry(worksheet, dataframe, worksheet_name, max_retries=10):
    """
    Paste dataframe to Google Sheet with retry logic
    
    Args:
        worksheet: gspread worksheet object
        dataframe: pandas DataFrame to paste
        worksheet_name: name of the worksheet (for logging)
        max_retries: maximum number of retry attempts (default: 10)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if dataframe.empty:
        log.info(f"Skip: Grouped DataFrame for {worksheet_name} is empty, not pasting to sheet.")
        return True
    
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"📝 Attempt {attempt}/{max_retries}: Pasting data to {worksheet_name}...")
            
            # Clear the worksheet
            worksheet.batch_clear(["A:G"])
            time.sleep(2)
            
            # Paste the dataframe
            set_with_dataframe(worksheet, dataframe, row=1, col=1)
            log.info(f"✅ Grouped data pasted to Google Sheet ({worksheet_name}).")
            
            # Add timestamp
            local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
            worksheet.update("AC1", [[f"{local_time}"]])
            log.info(f"✅ Timestamp updated for {worksheet_name}: {local_time}")
            
            return True
            
        except Exception as e:
            log.error(f"❌ Attempt {attempt}/{max_retries} failed for {worksheet_name}: {str(e)}")
            
            if attempt < max_retries:
                # Exponential backoff: wait longer between each retry
                wait_time = min(2 ** attempt, 60)  # Cap at 60 seconds
                log.info(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                log.error(f"❌ All {max_retries} attempts failed for {worksheet_name}")
                return False
    
    return False

# ----------------------------
# Paste data for companies 1 & 4
# ----------------------------
worksheet_14 = sheet.worksheet("Z_raw_df")
success_14 = paste_to_sheet_with_retry(worksheet_14, grouped_14, "Z_raw_df (Companies 1 & 4)")

if not success_14:
    log.error("❌ Failed to paste data for Companies 1 & 4 after all retries")

# ----------------------------
# Paste data for companies 3 & 4
# ----------------------------
worksheet_34 = sheet.worksheet("M_raw_df")
success_34 = paste_to_sheet_with_retry(worksheet_34, grouped_34, "M_raw_df (Companies 3 & 4)")

if not success_34:
    log.error("❌ Failed to paste data for Companies 3 & 4 after all retries")

# ----------------------------
# Final Summary
# ----------------------------
if success_14 and success_34:
    log.info("🎉 All data successfully pasted to Google Sheets!")
elif success_14 or success_34:
    log.warning("⚠️ Partial success: Some data was pasted, but some operations failed")
else:
    log.error("❌ Failed to paste any data to Google Sheets")
    raise Exception("All Google Sheets paste operations failed")