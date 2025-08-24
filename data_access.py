import gspread
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


import json
import gspread
import pandas as pd
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

def _read_service_account_email(creds_path: str) -> str:
    with open(creds_path, "r") as f:
        data = json.load(f)
    # Works for standard service account JSONs
    return data.get("client_email", "<unknown-service-account-email>")

def write_csv_to_google_sheet(
    csv_path: str,
    spreadsheet_id: str,
    sheet_name: str,
    creds_path: str,
    create_sheet_if_missing: bool = True,
    overwrite: bool = True,
):
    """
    Upload a local CSV into a specific worksheet of a Google Spreadsheet.

    - Ensures the service account can access the spreadsheet (share it if needed).
    - Creates the worksheet if it doesn't exist (optional).
    - Overwrites the worksheet content (headers + rows).

    Args:
        csv_path: Local CSV file path.
        spreadsheet_id: Google Spreadsheet ID (the /d/<THIS>/ part of the URL).
        sheet_name: Target worksheet/tab name.
        creds_path: Path to service account JSON.
        create_sheet_if_missing: Create the worksheet if absent.
        overwrite: If True, clears existing content before writing.

    Raises:
        SpreadsheetNotFound: If the spreadsheet is not found / no permission.
        WorksheetNotFound: If the worksheet doesn't exist and create_sheet_if_missing=False.
    """
    # Load CSV
    df = pd.read_csv(csv_path)

    # Authorize with the service account
    sa_email = _read_service_account_email(creds_path)
    client = gspread.service_account(filename=creds_path)

    # Open spreadsheet
    try:
        sh = client.open_by_key(spreadsheet_id)
    except SpreadsheetNotFound as e:
        raise SpreadsheetNotFound(
            f"Spreadsheet not found or no access (404). "
            f"Check SPREADSHEET_ID and share the sheet with this service account:\n  {sa_email}"
        ) from e

    # Get or create worksheet
    try:
        ws = sh.worksheet(sheet_name)
    except WorksheetNotFound:
        if not create_sheet_if_missing:
            raise
        # create with some headroom rows/cols
        rows = max(len(df) + 10, 100)
        cols = max(len(df.columns) + 5, 26)
        ws = sh.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

    # Overwrite content
    if overwrite:
        ws.clear()

    # Prepare data payload (headers + rows)
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()

    # Bulk update starting at A1
    ws.update("A1", values)

    # Optional: resize the sheet to the data size (keeps things tidy)
    try:
        ws.resize(rows=len(values), cols=len(values[0]))
    except Exception:
        # Resizing may fail if not enough privileges; ignore quietly
        pass

    print(f"✅ Uploaded '{csv_path}' to spreadsheet '{sh.title}' → worksheet '{sheet_name}' as {sa_email}")

def deduplicate_headers(headers):
    """
    Ensure header columns are unique by appending suffixes (_1, _2, ...) to duplicates.

    Args:
        headers (list of str): List of column names from the sheet.

    Returns:
        list of str: A modified list with unique column names.
    """
    seen = {}
    result = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            result.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            result.append(h)
    return result


def list_sheets_in_folder(folder_id, creds_path="service_account.json"):
    """
    List all Google Sheets files inside a specific Google Drive folder.

    Args:
        folder_id (str): Google Drive folder ID.
        creds_path (str): Path to the Google service account JSON credentials.

    Returns:
        List of tuples: [(sheet_name, sheet_id), ...]
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    results = service.files().list(
        q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'",
        pageSize=1000,
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])
    return [(f['name'], f['id']) for f in files]


def load_all_from_folder(folder_id, creds_path="service_account.json"):
    """
    Load and parse all Google Sheets in a folder assuming consistent format:
    Date, Description, Category, Withdrawals, Deposits, Balance, Source

    Args:
        folder_id (str): Google Drive folder ID.
        creds_path (str): Path to the Google service account JSON credentials.

    Returns:
        pd.DataFrame: Merged dataframe with standardized columns.
    """
    sheets = list_sheets_in_folder(folder_id, creds_path)
    client = gspread.service_account(filename=creds_path)

    dataframes = []
    for name, sheet_id in sheets:
        print(name)
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            rows = sheet.get_all_values()

            if len(rows) < 4:
                print(f"⚠️ Skipping {name}: not enough rows")
                continue

            headers = [h.strip() for h in rows[1]]
            data = rows[2:]  # Data starts from the 4th row (index 3)

            # Skip rows where the Date (first column) is empty
            filtered_data = [row for row in data if len(row) > 0 and row[0].strip()]

            if not filtered_data:
                print(f"⚠️ Skipping {name}: no rows with valid Date")
                continue

            df = pd.DataFrame(filtered_data, columns=headers)

            # Drop rows where 'Date' is empty or just whitespace
            df = df[df['Date'].str.strip().astype(bool)]

            # Ensure expected columns exist
            expected_cols = ['Date', 'Description', 'Category', 'Withdrawals', 'Deposits', 'Balance', 'Source']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = ""

            df = df[expected_cols]
            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
                print(df)
            dataframes.append(df)
            print(f"✅ Loaded: {name} ({len(df)} rows)")

        except Exception as e:
            print(f"❌ Failed to load {name}: {e}")

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()


def load_all_historical_sheets(sheet_names, creds_path="service_account.json"):
    """
    Load multiple named Google Sheets individually, dropping empty rows.

    Args:
        sheet_names (list of str): List of sheet names (must be shared with the service account).
        creds_path (str): Path to the service account JSON file.

    Returns:
        pd.DataFrame: Combined non-empty data from all listed sheets.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)

    dataframes = []
    for sheet_name in sheet_names:
        try:
            sheet = client.open(sheet_name).sheet1
            rows = sheet.get_all_values()

            if len(rows) < 4:
                print(f"⚠️ Not enough rows to parse {sheet_name}")
                continue

            headers = deduplicate_headers(rows[2])
            data = rows[3:]
            df = pd.DataFrame(data, columns=headers)

            # Ensure consistent date format
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df = df[df['Date'].notna()]  # Drop rows where date could not be parsed
                df['Date'] = df['Date'].dt.strftime("%-m/%-d/%Y")  # Format: 2/17/2025

            # Drop fully empty rows
            df = df.dropna(how="all")
            df = df[~(df == "").all(axis=1)]

            if df.empty:
                print(f"⚠️ All rows empty after filtering: {sheet_name}")
                continue

            df["source_sheet"] = sheet_name
            dataframes.append(df)

            print(f"✅ Loaded: {sheet_name} ({len(df)} rows)")

        except Exception as e:
            print(f"❌ Failed to load {sheet_name}: {e}")

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()