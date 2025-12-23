import os
import json
from pyairtable import Api
import gspread
from google.oauth2.service_account import Credentials

# Airtable setup
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
BASE_ID = 'appK9De6dRY4XLJA'

# Tables to sync (Airtable table name -> Google Sheet tab name)
TABLES_TO_SYNC = {
    'Closer SRF': 'Closer SRF',
    'EOD': 'EOD',
    'Payment Plan': 'Payment Plan'
}

# Google Sheets setup
GOOGLE_SHEET_ID = '1U7a6mJu2gX8oBQ_NEoEX4nN-p_uVJGNDwZgQC_Qzow0'
GOOGLE_CREDENTIALS_JSON = os.environ.get('GOOGLE_CREDENTIALS')

def sync_table_to_sheet(api, base_id, table_name, sheet, worksheet_name):
    """Sync a single Airtable table to a Google Sheet worksheet"""
    
    print(f"Syncing {table_name}...")
    
    # Get table
    table = api.table(base_id, table_name)
    
    # Get schema to extract field names
    base = api.base(base_id)
    schema = base.schema()
    
    # Find the table in schema
    table_schema = next((t for t in schema.tables if t.name == table_name), None)
    if not table_schema:
        print(f"Could not find table {table_name} in schema")
        return
    
    # Extract field names
    field_names = [field.name for field in table_schema.fields]
    
    # Fetch all records
    records = table.all()
    
    # Prepare data
    rows = [field_names]  # Header row
    
    for record in records:
        row = []
        for field_name in field_names:
            value = record['fields'].get(field_name, '')
            
            # Handle different field types
            if isinstance(value, list):
                value = ', '.join(str(v) for v in value)
            elif value is None:
                value = ''
            
            row.append(str(value))
        
        rows.append(row)
    
    # Get or create worksheet
    try:
        worksheet = sheet.worksheet(worksheet_name)
    except:
        worksheet = sheet.add_worksheet(title=worksheet_name, rows=len(rows)+10, cols=len(field_names))
    
    # Clear existing data
    worksheet.clear()
    
    # Update with new data
    worksheet.update(range_name='A1', values=rows)
    
    # Format header row
    worksheet.format('A1:Z1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })
    
    print(f"✓ Synced {len(records)} records from {table_name}")

def main():
    # Initialize Airtable API
    api = Api(AIRTABLE_API_KEY)
    
    # Initialize Google Sheets
    credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    gc = gspread.authorize(credentials)
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)
    
    # Sync each table
    for airtable_table, sheet_tab in TABLES_TO_SYNC.items():
        try:
            sync_table_to_sheet(api, BASE_ID, airtable_table, sheet, sheet_tab)
        except Exception as e:
            print(f"Error syncing {airtable_table}: {e}")
    
    print("\n✓ All tables synced successfully!")

if __name__ == '__main__':
    main()


