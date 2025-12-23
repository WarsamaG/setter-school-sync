import os
import json
from pyairtable import Api
import gspread
from google.oauth2.service_account import Credentials


# ===== Airtable setup =====
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")  # GitHub Secret
BASE_ID = "appK9De6dRY4XLJA"

# Airtable table name -> Google Sheet tab name
TABLES_TO_SYNC = {
    "Closer SRF": "Closer SRF",
    "EOD": "EOD",
    "Payment Plan": "Payment Plan",
}

# ===== Google Sheets setup =====
# IMPORTANT: This MUST match the ID from your screenshot exactly.
GOOGLE_SHEET_ID = "1U7a6mJu2gX8oBQ_N6eX4nN-p_uYJGNDwZqQC_Ozov0"

# GitHub Secret containing the entire service account JSON
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS")


def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing environment variable: {name}")
    return val


def get_gspread_client():
    creds_raw = require_env("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_raw)

    # Helpful debug (safe to print)
    print("Using service account:", creds_dict.get("client_email"))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def normalize_cell_value(value):
    """Convert Airtable field values to something Sheets can store."""
    if value is None:
        return ""

    # Linked records / multi-selects / attachments may appear as lists
    if isinstance(value, list):
        parts = []
        for v in value:
            if isinstance(v, dict):
                # attachments or rich objects
                if "url" in v:
                    parts.append(v["url"])
                elif "name" in v:
                    parts.append(str(v["name"]))
                elif "id" in v:
                    parts.append(str(v["id"]))
                else:
                    parts.append(json.dumps(v))
            else:
                parts.append(str(v))
        return ", ".join(parts)

    if isinstance(value, dict):
        # Sometimes Airtable returns dict objects (e.g., collaborators, etc.)
        if "name" in value:
            return str(value["name"])
        if "url" in value:
            return str(value["url"])
        return json.dumps(value)

    return str(value)


def ensure_worksheet(sheet, title: str, rows: int, cols: int):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        # minimum size constraints
        rows = max(rows, 100)
        cols = max(cols, 10)
        return sheet.add_worksheet(title=title, rows=rows, cols=cols)


def sync_table_to_sheet(api: Api, base_id: str, table_name: str, sheet, worksheet_name: str):
    print(f"\n--- Syncing Airtable table: {table_name} -> Sheet tab: {worksheet_name} ---")

    table = api.table(base_id, table_name)

    # Pull schema to get stable column ordering
    base = api.base(base_id)
    schema = base.schema()
    table_schema = next((t for t in schema.tables if t.name == table_name), None)
    if not table_schema:
        raise RuntimeError(f"Could not find table '{table_name}' in base schema")

    field_names = [f.name for f in table_schema.fields]
    print("Fields:", field_names)

    # Fetch all records
    records = table.all()
    print(f"Fetched {len(records)} records")

    # Build rows: header + data
    rows = [field_names]
    for record in records:
        fields = record.get("fields", {})
        row = [normalize_cell_value(fields.get(fn)) for fn in field_names]
        rows.append(row)

    worksheet = ensure_worksheet(sheet, worksheet_name, rows=len(rows) + 10, cols=len(field_names) + 5)

    # Clear and update
    worksheet.clear()
    worksheet.update("A1", rows)

    # Format header row (only as wide as needed)
    last_col = min(len(field_names), 26)  # A-Z formatting range limit
    end_letter = chr(ord("A") + last_col - 1)
    worksheet.format(f"A1:{end_letter}1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
    })

    print(f"✓ Done: {table_name}")


def main():
    # Validate env
    require_env("AIRTABLE_API_KEY")
    require_env("GOOGLE_CREDENTIALS")

    print("DEBUG spreadsheet id:", GOOGLE_SHEET_ID)

    # Airtable
    api = Api(AIRTABLE_API_KEY)

    # Google Sheets
    gc = get_gspread_client()
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)
    print("Opened spreadsheet:", sheet.title)

    # Sync
    for airtable_table, sheet_tab in TABLES_TO_SYNC.items():
        try:
            sync_table_to_sheet(api, BASE_ID, airtable_table, sheet, sheet_tab)
        except Exception as e:
            print(f"✗ Error syncing '{airtable_table}': {e}")
            raise  # fail the action so you see it

    print("\n✓ All tables synced successfully!")


if __name__ == "__main__":
    main()
