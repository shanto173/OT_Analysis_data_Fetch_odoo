# OT Analysis Data Fetch — Odoo → Google Sheets

An automated data pipeline that extracts **Overtime (OT) analysis**, **HR attendance**, and **Purchase Order** data from an [Odoo](https://www.odoo.com/) ERP system and synchronises the results to Google Sheets.  
The pipeline runs on **GitHub Actions** and can also be executed locally.

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [Repository Structure](#repository-structure)
- [Scripts Overview](#scripts-overview)
  - [Ot_data_fetch.py — OT Analysis Report](#ot_data_fetchpy--ot-analysis-report)
  - [ot_head.py — HR Attendance Records](#ot_headpy--hr-attendance-records)
  - [purchase_orders.py — Purchase Orders](#purchase_orderspy--purchase-orders)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [Local Setup](#local-setup)
  - [GitHub Actions Setup](#github-actions-setup)
- [Running the Scripts](#running-the-scripts)
  - [Local Execution](#local-execution)
  - [GitHub Actions](#github-actions)
- [Configuration Reference](#configuration-reference)
- [Data Flow](#data-flow)
- [License](#license)

---

## What This Project Does

This project automates the extraction of operational data from an Odoo instance and pushes the cleaned results into Google Sheets for reporting and analysis:

| Data | Source Model | Destination Sheet (worksheet) |
|---|---|---|
| OT Analysis — Zipper | `attendance.pdf.report` | `ZIP_OT_DATA` |
| OT Analysis — Metal Trims | `attendance.pdf.report` | `MT_OT_DATA` |
| HR Attendance — Zipper | `hr.attendance` | `Z_raw_df` |
| HR Attendance — Metal Trims | `hr.attendance` | `M_raw_df` |
| Purchase Orders | `purchase.order` | `PO_Status_Data` |

---

## Repository Structure

```
OT_Analysis_data_Fetch_odoo/
├── .github/
│   └── workflows/
│       └── main.yml          # GitHub Actions workflow
├── Ot_data_fetch.py          # OT Analysis report fetcher
├── ot_head.py                # HR Attendance data fetcher
├── purchase_orders.py        # Purchase Orders data fetcher
├── requirements.txt          # Python dependencies
├── .gitignore
└── LICENSE
```

> **Note:** `gcreds.json` (Google service account key) and `.env` are excluded from version control via `.gitignore`.  
> They are generated at runtime — see [Setup](#setup).

---

## Scripts Overview

### `Ot_data_fetch.py` — OT Analysis Report

Generates and downloads an **OT (Overtime) Analysis XLSX report** from Odoo for two companies (Zipper and Metal Trims) and writes each company's data to a separate worksheet in Google Sheets.

**Steps performed:**
1. Authenticate with Odoo via JSON-RPC.
2. Trigger the `attendance.pdf.report` wizard for each company.
3. Call `action_generate_xlsx_report` to produce the file.
4. Download the generated XLSX (up to 5 retries with exponential back-off).
5. Parse and fix date formats in the resulting DataFrame.
6. Write the data to the target Google Sheet.

**Command-line arguments:**

| Argument | Default | Description |
|---|---|---|
| `--from_date` | `2025-07-26` | Report start date (`YYYY-MM-DD`) |
| `--to_date` | Yesterday | Report end date (`YYYY-MM-DD`) |

---

### `ot_head.py` — HR Attendance Records

Fetches raw **HR attendance records** (with overtime and worked hours) from Odoo, groups them by employee and date, and pushes the results to Google Sheets.

**Steps performed:**
1. Authenticate with Odoo.
2. Fetch all active employees per company context.
3. Retrieve `hr.attendance` records from `2024-04-01` to today.
4. Flatten many-to-one fields (e.g. `[id, "Name"]` → `"Name"`).
5. Group by Date / Employee / Department / Category and sum OT & Worked Hours.
6. Write two worksheets: one for Zipper (`Z_raw_df`) and one for Metal Trims (`M_raw_df`).

**Fetched fields:**

| Odoo field | Sheet column |
|---|---|
| `attDate` | Date |
| `employee_id` | Employee |
| `department_id` | Department |
| `com_otHours` | OT Hours |
| `worked_hours` | Worked Hours |
| `x_studio_category` | Category |

---

### `purchase_orders.py` — Purchase Orders

Exports all **Purchase Order** records from Odoo, saves them to a timestamped Excel file, and syncs the data to Google Sheets.

**Steps performed:**
1. Authenticate with Odoo.
2. Paginate through all `purchase.order` records (1 000 per batch).
3. Map internal field names to human-readable column headers.
4. Save to `downloads/PO_<timestamp>.xlsx`.
5. Read the latest file and write to the `PO_Status_Data` worksheet.

**Fetched fields:** Company, Status, Order Reference, Vendor, Total, Payment Terms, Priority, Source Document, Gate Entry, PI No., Currency, Incoterm, Shipment Mode, Created By, Created On, and more.

---

## Prerequisites

- Python 3.11+
- An Odoo instance with API access
- A Google Cloud **service account** with the Sheets API enabled and the target spreadsheets shared with the service account email

---

## Setup

### Local Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/shanto173/OT_Analysis_data_Fetch_odoo.git
   cd OT_Analysis_data_Fetch_odoo
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create a `.env` file** in the project root (never commit this file):
   ```dotenv
   ODOO_URL=https://your-odoo-instance.com
   ODOO_DB=your-database-name
   ODOO_USERNAME=your@email.com
   ODOO_PASSWORD=yourpassword
   ```

4. **Add your Google service account key** as `gcreds.json` in the project root.  
   The service account must have **Editor** access to the target Google Sheets.

---

### GitHub Actions Setup

Add the following [repository secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets):

| Secret name | Description |
|---|---|
| `ODOO_URL` | Full URL of your Odoo instance |
| `ODOO_DB` | Odoo database name |
| `ODOO_USERNAME` | Odoo login e-mail |
| `ODOO_PASSWORD` | Odoo login password |
| `GOOGLE_CREDENTIALS_B64` | Base64-encoded content of `gcreds.json` |

To encode your service account key:
```bash
base64 -w 0 gcreds.json
```
Copy the output as the value for `GOOGLE_CREDENTIALS_B64`.

---

## Running the Scripts

### Local Execution

```bash
# OT Analysis report (uses yesterday as end date by default)
python Ot_data_fetch.py --from_date 2025-07-26 --to_date 2025-08-15

# HR Attendance records
python ot_head.py

# Purchase Orders
python purchase_orders.py
```

### GitHub Actions

1. Go to your repository on GitHub → **Actions** → **Odoo OT & PO Reports**.
2. Click **Run workflow**.
3. Choose which script to run:
   - **All** — runs both `Ot_data_fetch.py` and `purchase_orders.py`
   - **Ot_data_fetch.py** — OT report only
   - **purchase_orders.py** — Purchase Orders only
4. Click **Run workflow** to start the job.

> The date inputs in the workflow UI are informational only; the workflow always uses `2025-07-26` as the start date and *yesterday* as the end date.

---

## Configuration Reference

### Google Sheets IDs

Update these constants inside each script if you use different spreadsheets:

| Script | Constant | Default Sheet ID |
|---|---|---|
| `Ot_data_fetch.py` | `SPREADSHEET_ID` | `1-kBuln5CnKucuHqYG4vvgttJ8DqeJALvr4TjAYuVkXs` |
| `ot_head.py` | `SPREADSHEET_ID` | `1OOwRMvGMgZ0lLsq3VLWmqGWF9WsqLj6N72Bdn-0-PNw` |
| `purchase_orders.py` | `SPREADSHEET_ID` | `19FTCzNt8cWhy9CXFXM0NmIotlrkiKhIVMtH6MfFNOEM` |

### Company IDs (Odoo)

| ID | Company |
|---|---|
| `1` | Zipper |
| `3` | Metal Trims |
| `4` | Shared / holding company |

---

## Data Flow

```
┌─────────────────────────────────┐
│         Odoo ERP Instance       │
│  (attendance.pdf.report,        │
│   hr.attendance, purchase.order)│
└────────────┬────────────────────┘
             │ JSON-RPC / HTTP
     ┌───────▼──────────────────────────────────┐
     │              Python Scripts               │
     │  Ot_data_fetch.py  ot_head.py  purchase_ │
     │                               orders.py  │
     └───────┬──────────────────────────────────┘
             │ gspread / pandas
     ┌───────▼──────────────────────────────────┐
     │           Google Sheets                  │
     │  ZIP_OT_DATA  │  Z_raw_df  │ PO_Status_  │
     │  MT_OT_DATA   │  M_raw_df  │ Data        │
     └──────────────────────────────────────────┘
```

---

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file included in this repository.
