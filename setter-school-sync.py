#!/usr/bin/env python3
"""
Sync Airtable tables to Google Sheets (multiple tabs).

Secrets expected:
- AIRTABLE_API_KEY  (Airtable Personal Access Token)
- GOOGLE_CREDENTIALS (Service Account JSON, stored as a GitHub secret)

This script expects the workflow to write GOOGLE_CREDENTIALS into:
- google_credentials.json
"""

import os
import json
from datetime import datetime
from pyairtable import Api
import gspread
from google.oauth2.service_account import Credentials


# ===== Airtable config =====
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
BASE_ID = "appK9De6dRY4XLJA"

# Airtable table name OR table id -> Google Sheet tab name
TABLE_MAPPINGS = {
    "Closer SRF": "Closer SRF",
    "EOD": "EOD",
    "Payment Plan": "Payment Plan",
}

# ===== Google Sheets config =====
# MUST match your actual sheet URL exactly (from your screenshot)
GOOGLE_SHEET_ID = "1U7a6mJu2gX8oBQ_NEoEX4nN-p_uYJGNDwZqQC_Qzov0"
GOOGLE_CREDENTIALS_FILE = "google_credentials.json"


def normalize(value):
    """Convert Airtable values into strings safe for Sheets."""
    if value is None:
        return ""

    if isinstance(value, list):
        parts = []
        for v in value:
            if isinstance(v, dict):
                # attachments / collaborators / complex objects
                if "name" in v:
                    parts.append(str(v["name"]))
                elif "url" in v:
                    parts.append(str(v["url"]))
                elif "id" in v:
                    parts.append(str(v["id"]))
                else:
                    parts.append(json.dumps(v))
            else:
                parts.append(str(v))
        return ", ".join(parts)

    if isinstance(value, dict):
        if "name" in value:
            return str(value["name"])
        if "url" in value:
            return str(value["url"])
        return json.dumps(value)

    return str(value)


def get_headers_from_schema(api: Api, base_id: str, table_name_or_id: str):
    """Get stable header order from Airtable schema."""
    base = api.base(base_id)
    schema = base.schema()
    table_schema = next(
        (t for t in schema.tables if t.name == table_name_or_id or t.id == table_name_or_id),
        None,
    )
    if not table_schema:
        return None
    return [f.name for f in table_schema.fields]


def ensure_worksheet(spreadsheet, tab_name: str, rows: int, cols: int):
    try:
        return spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=tab_name,
            rows=max(rows, 100),
            cols=max(cols, 10),
        )


def main():
    print(f"Starting sync at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    if not AIRTABLE_API_KEY:
        raise RuntimeError("Missing AIRTABLE_API_KEY env var")

    if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
        raise RuntimeError(f"Missing {GOOGLE_CREDENTIALS_FILE}. Workflow must create it from secret.")

    # Airtable client
    api = Api(AIRTABLE_API_KEY)

    # Google Sheets client
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    print("Opened spreadsheet:", spreadsheet.title)

    for table_name_or_id, tab_name in TABLE_MAPPINGS.items():
        print(f"\nSyncing Airtable '{table_name_or_id}' -> Sheet tab '{tab_name}'")

        # Pull records
        table = api.table(BASE_ID, table_name_or_id)
        records = table.all()

        if not records:
            print("  No records found, skipping.")
            continue

        # Get headers from schema (preferred)
        headers = get_headers_from_schema(api, BASE_ID, table_name_or_id)

        # Fallback to union of record keys
        if not headers:
            all_fields = set()
            for r in records:
                all_fields.update(r.get("fields", {}).keys())
            headers = sorted(all_fields)

        data_rows = []
        for r in records:
            fields = r.get("fields", {})
            data_rows.append([normalize(fields.get(h)) for h in headers])

        all_data = [headers] + data_rows

        ws = ensure_worksheet(
            spreadsheet,
            tab_name=tab_name,
            rows=len(all_data) + 10,
            cols=len(headers) + 5,
        )

        ws.clear()
        ws.update("A1", all_data)

        # Format header row (cap at Z)
        last = min(len(headers), 26)
        end_col = chr(ord("A") + last - 1)
        ws.format(f"A1:{end_col}1", {"textFormat": {"bold": True}})

        print(f"  ✓ Synced {len(data_rows)} rows, {len(headers)} columns")

    print(f"\nSync completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()

