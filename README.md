# OT Analysis & Data Fetch — Odoo to Google Sheets

Automated Python scripts that pull **Overtime (OT) analysis**, **HR attendance**, and **Purchase Order** data from an [Odoo](https://www.odoo.com/) ERP instance and push the results into Google Sheets. The scripts are orchestrated by a GitHub Actions workflow so they can be triggered manually or on a schedule.

---

## Repository Contents

| File | Purpose |
|---|---|
| `Ot_data_fetch.py` | Generates and downloads the **OT Analysis XLSX report** from Odoo (per company) and uploads it to a dedicated Google Sheet. |
| `ot_head.py` | Reads raw **HR attendance records** (date, employee, department, OT hours, worked hours, category) from Odoo, aggregates them by month, and pushes the result to a Google Sheet. |
| `purchase_orders.py` | Fetches all **Purchase Order** records from Odoo, cleans many2one fields, saves a local Excel file, and uploads it to a Google Sheet. |
| `requirements.txt` | Python package dependencies. |
| `.github/workflows/main.yml` | GitHub Actions workflow that installs dependencies, decodes Google credentials, and runs the chosen script(s). |
| `.env.example` | Template for the required environment variables (copy to `.env` and fill in your values — **never commit `.env`**). |

---

## How It Works

### `Ot_data_fetch.py` — OT Analysis Report
1. Authenticates with Odoo via JSON-RPC session.
2. Obtains a CSRF token.
3. For each configured company (`COMPANY_IDS`):
   - Calls the `attendance.pdf.report` wizard (`onchange` → `web_save` → `action_generate_xlsx_report`).
   - Downloads the generated XLSX report with retry / exponential back-off.
   - Reads the report into a DataFrame, applies smart date-year correction, and writes it to the matching Google Sheet tab (`ZIP_OT_DATA` or `MT_OT_DATA`).

### `ot_head.py` — Raw Attendance (Head-count / OT Hours)
1. Authenticates with Odoo.
2. Fetches all `hr.employee` records to obtain active/inactive status.
3. Fetches all `hr.attendance` records within the configured date range (paginated, 1,000 records per request, with retry).
4. Cleans the data, groups by month / employee / department / category, and uploads the result to Google Sheets (`Z_raw_df` and `M_raw_df` tabs).

### `purchase_orders.py` — Purchase Orders
1. Authenticates with Odoo.
2. Fetches all `purchase.order` records (paginated).
3. Cleans many2one fields, saves a timestamped local XLSX file, and pushes the data to the `PO_Status_Data` tab in Google Sheets.

---

## Setup

### Prerequisites
- Python 3.11+
- A Google Cloud service account with the **Google Sheets API** enabled and editor access to the target spreadsheets.
- An Odoo instance accessible over HTTPS.

### 1 — Clone and install dependencies

```bash
git clone https://github.com/shanto173/OT_Analysis_data_Fetch_odoo.git
cd OT_Analysis_data_Fetch_odoo
pip install -r requirements.txt
```

### 2 — Configure environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `ODOO_URL` | Base URL of your Odoo instance (e.g. `https://your-company.odoo.com`) |
| `ODOO_DB` | Odoo database name |
| `ODOO_USERNAME` | Odoo login email |
| `ODOO_PASSWORD` | Odoo login password |

> ⚠️ **Never commit `.env` to version control.** It is listed in `.gitignore`.

### 3 — Google credentials

Place your Google service account JSON file at the repository root as `gcreds.json`.  
`gcreds.json` is also listed in `.gitignore` and should **never** be committed.

In the GitHub Actions workflow the credentials are base64-encoded and stored as a repository secret (`GOOGLE_CREDENTIALS_B64`); the workflow decodes them at runtime.

### 4 — Run locally

```bash
# OT Analysis report (yesterday as end date by default)
python Ot_data_fetch.py

# OT Analysis with explicit date range
python Ot_data_fetch.py --from_date 2025-07-26 --to_date 2025-08-31

# Raw attendance aggregation
python ot_head.py

# Purchase orders
python purchase_orders.py
```

---

## GitHub Actions

The workflow (`.github/workflows/main.yml`) is triggered **manually** via `workflow_dispatch` with the following inputs:

| Input | Options | Description |
|---|---|---|
| `script_choice` | `All`, `Ot_data_fetch.py`, `purchase_orders.py` | Which script to run |
| `from_date` | `YYYY-MM-DD` | Start date (informational; currently fixed to `2025-07-26` internally) |
| `to_date` | `YYYY-MM-DD` | End date (informational; always set to yesterday internally) |

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `ODOO_URL` | Odoo base URL |
| `ODOO_USERNAME` | Odoo login |
| `ODOO_PASSWORD` | Odoo password |
| `ODOO_DB` | Odoo database name |
| `GOOGLE_CREDENTIALS_B64` | Base64-encoded Google service account JSON |

---

## Target Google Sheets

| Sheet | Tab | Script |
|---|---|---|
| OT Analysis sheet (`1-kBuln5CnKucuHqYG4vvgttJ8DqeJALvr4TjAYuVkXs`) | `ZIP_OT_DATA` | `Ot_data_fetch.py` (company 1 — Zipper) |
| OT Analysis sheet | `MT_OT_DATA` | `Ot_data_fetch.py` (company 3 — Metal Trims) |
| Attendance sheet (`1OOwRMvGMgZ0lLsq3VLWmqGWF9WsqLj6N72Bdn-0-PNw`) | `Z_raw_df` | `ot_head.py` (companies 1 & 4) |
| Attendance sheet | `M_raw_df` | `ot_head.py` (companies 3 & 4) |
| PO sheet (`19FTCzNt8cWhy9CXFXM0NmIotlrkiKhIVMtH6MfFNOEM`) | `PO_Status_Data` | `purchase_orders.py` |

---

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file in this repository.
