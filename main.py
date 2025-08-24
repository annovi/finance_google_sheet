import os
from data_access import load_all_from_folder, write_csv_to_google_sheet

FOLDER_ID_2021 = "1mju7aROMYrESvlRMrVpla0LBUXgW5umm" # Google Drive folder ID
FOLDER_ID_2022 = "19U_gjCZhIdF4Ti86gsC9EC5fVcBfnS5d" # Google Drive folder ID
FOLDER_ID_2023 = "19O9N7RqbS4rP51ynn3ghbmsfgG_lDXMU" # Google Drive folder ID
FOLDER_ID_2024 = "1gwQ8ymIBP3Kobwa0ArL4WmH1WnhFk5J2" # Google Drive folder ID
FOLDER_ID_2025 = "1DYJmBFq_OngBIoJCtMRD-6d44xfMFDiS" # Google Drive folder ID
CREDS_PATH = "google_sheet/financial-parser-468017-0514dc91ef05.json"
OUTPUT_CSV = "google_sheet/combined_historical_data_2021.csv"

SPREADSHEET_ID_MAY = "1FWdjIrCkUf58jXWe7QPceSB9pD49EtQn0ImDqOmHweo"
SPREADSHEET_ID_JUNE = "1q4sKKQVTYWu6aNkWscNO_qWfH4szWRDiA3ia86ukLDc"  
SPREADSHEET_ID_JULY = "1mioy-TjP7qQ6E_cQdwY03sPdi5qo-ahvOF6wHS0UQxg"
SPREADSHEET_ID_AUGUST = "1MPPZLf8Q0yU5bSeecRE3_G--VEVobBGH64_xxhfDq_0"  

# Plan: list of {csv -> target worksheet name}

UPLOAD_PLAN_MAY = [
    {"csv": "output/scotiaBank_2025-05.csv", "sheet": "ScotiaBank_2025-05"},
    {"csv": "output/scotiaVisa_2025-05.csv", "sheet": "ScotiaVisa_2025-05"},
    {"csv": "output/tdVisa_2025-05.csv", "sheet": "TDVisa_2025-05"},
    {"csv": "output/amex_2025-05.csv", "sheet": "Amex_2025-05"},
]

UPLOAD_PLAN_JUNE = [
    {"csv": "output/scotiaBank_2025-06.csv", "sheet": "ScotiaBank_2025-06"},
    {"csv": "output/scotiaVisa_2025-06.csv", "sheet": "ScotiaVisa_2025-06"},
    {"csv": "output/tdVisa_2025-06.csv", "sheet": "TDVisa_2025-06"},
    {"csv": "output/amex_2025-06.csv", "sheet": "Amex_2025-06"},
]


UPLOAD_PLAN_JULY = [
    {"csv": "output/scotiaBank_2025-07.csv", "sheet": "ScotiaBank_2025-07"},
    {"csv": "output/scotiaVisa_2025-07.csv", "sheet": "ScotiaVisa_2025-07"},
    {"csv": "output/tdVisa_2025-07.csv", "sheet": "TDVisa_2025-07"},
    {"csv": "output/amex_2025-07.csv", "sheet": "Amex_2025-07"},
]

UPLOAD_PLAN_AUGUST = [
    {"csv": "output/scotiaBank_2025-08.csv", "sheet": "ScotiaBank_2025-08"},
    {"csv": "output/scotiaVisa_2025-08.csv", "sheet": "ScotiaVisa_2025-08"},
    {"csv": "output/tdVisa_2025-08.csv", "sheet": "TDVisa_2025-08"},
    {"csv": "output/amex_2025-08.csv", "sheet": "Amex_2025-08"},
]


def upload_many(plan, spreadsheet_id, creds_path, *, create_if_missing=True, overwrite=True):
    """Loop over a list of {'csv': path, 'sheet': name} entries and upload each CSV."""
    for item in plan:
        csv_path = item["csv"]
        sheet_name = item["sheet"]

        if not os.path.exists(csv_path):
            print(f"⚠️ Skipping: file not found -> {csv_path}")
            continue

        print(f"⬆️ Uploading {csv_path} → sheet '{sheet_name}'")
        # Push to Google Sheets (will create the tab if missing)
        write_csv_to_google_sheet(
            csv_path=csv_path,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            creds_path=creds_path,
            create_sheet_if_missing=create_if_missing,
            overwrite=overwrite,
        )

def main_upload():
    upload_many(UPLOAD_PLAN_AUGUST, SPREADSHEET_ID_AUGUST, CREDS_PATH,
                create_if_missing=True, overwrite=True)

def main_download():
    df = load_all_from_folder(FOLDER_ID_2021, CREDS_PATH)
    if df.empty:
        print("⚠️ No data loaded.")
    else:
        print(f"✅ Combined data shape: {df.shape}")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"📁 Data saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main_download()