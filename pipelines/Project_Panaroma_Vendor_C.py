import os
import time
from datetime import datetime as dt_obj
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import gspread
from dotenv import load_dotenv

from utils.sheets_exporter import export_multiple_dfs_to_google_sheet

load_dotenv()


# ==========================================
# Setup & Authentication
# ==========================================
def get_gspread_client():
    """Authenticates with Google Sheets via service account or default credentials."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        return gspread.service_account(filename=creds_path)
    return gspread.service_account()


base_dir = Path(os.getenv("VENDOR_C_BASE_DIR", "./data/Vendor_C Anchor System Data/Stock Position by Point Report"))
target_spreadsheet_id = os.getenv("TARGET_SPREADSHEET_ID")
master_spreadsheet_id = os.getenv("TARGET_SPREADSHEET_ID")


# ==========================================
# File Checking Script
# ==========================================
def _check_single_file(file_path):
    try:
        df = pd.read_excel(file_path)
        db_name = df.columns[1]
        date_val = df.iloc[0, 1]
        report_date = df.iloc[1, 1]
        return {
            'db_name': db_name,
            'file_name': Path(file_path).stem,
            'date': date_val,
            'report_date': report_date
        }
    except Exception:
        return None


def file_checking_script(path_location, gc):
    path_location = Path(path_location)
    if not path_location.exists():
        print(f"X Path does not exist: {path_location}")
        return None, None

    xlsx_files = [str(f.resolve()) for f in path_location.rglob("*.xlsx") if f.is_file()]
    print(f"Found {len(xlsx_files)} .xlsx file(s):")

    # Fetch mapping data from Google Sheets
    spreadsheet = gc.open_by_key(master_spreadsheet_id)
    worksheet = spreadsheet.worksheet('Mapping Data')
    data = worksheet.get_values('T:V')
    df_db_list = pd.DataFrame(data[1:], columns=data[0])

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(_check_single_file, xlsx_files))
    records = [r for r in results if r is not None]
    df1 = pd.DataFrame(records)

    merged_df = pd.merge(df_db_list, df1, left_on='Drishti ID & Name', right_on='db_name', how='left')

    print("\n--- Missing files ---")
    file_count = len(os.listdir(path_location))
    print(f"Total items in directory: {file_count}")

    missing_df = merged_df[merged_df['db_name'].isnull()]
    missing_records = missing_df[['DMS ID', 'DMS Name']]
    print("\nMissing Distributors:")
    print(missing_records)

    merged_df1 = pd.merge(df1, df_db_list, left_on='db_name', right_on='Drishti ID & Name', how='left')
    found_df = merged_df1[~merged_df1['db_name'].isnull()]
    found_records = found_df[['DMS ID', 'DMS Name', 'date', 'file_name', 'report_date']].sort_values(by='DMS ID')
    print("\nFound Distributors:")
    print(found_records)

    return missing_records, found_records


# ==========================================
# File Processing Script
# ==========================================
def process_single_excel(file_path):
    try:
        try:
            immediate_dir_name = file_path.parent.name.strip()
            dir_date_obj = dt_obj.strptime(immediate_dir_name, "%d-%m-%Y")
        except ValueError:
            return None, None

        df = pd.read_excel(file_path)
        db_name = df.columns[1]
        raw_date = str(df.iloc[0, 1]).strip()
        normalized_raw_date = raw_date.replace('/', '-')
        parsed_file_date = pd.NaT

        # Try MM-DD-YYYY first
        try:
            temp_date = pd.to_datetime(normalized_raw_date, format="%m-%d-%Y", errors="coerce")
            if pd.notna(temp_date) and temp_date.month == dir_date_obj.month:
                parsed_file_date = temp_date
        except Exception:
            pass

        # Try DD-MM-YYYY next
        if pd.isna(parsed_file_date):
            try:
                temp_date = pd.to_datetime(normalized_raw_date, format="%d-%m-%Y", errors="coerce")
                if pd.notna(temp_date) and temp_date.day == dir_date_obj.day:
                    parsed_file_date = temp_date
            except Exception:
                pass

        # Fallbacks
        if pd.isna(parsed_file_date):
            parsed_file_date = pd.to_datetime(normalized_raw_date, format="%m-%d-%Y", errors="coerce")
        if pd.isna(parsed_file_date):
            parsed_file_date = pd.to_datetime(normalized_raw_date, format="%d-%m-%Y", errors="coerce")
        if pd.isna(parsed_file_date):
            parsed_file_date = pd.to_datetime(normalized_raw_date, errors="coerce", dayfirst=False)

        if pd.isna(parsed_file_date):
            return None, {
                "file name": file_path.name,
                "Directory Date": dir_date_obj.strftime("%m,%d,%Y"),
                "Report Date": raw_date,
            }

        if dir_date_obj.date() != parsed_file_date.date():
            return None, {
                "file name": file_path.name,
                "Directory Date": dir_date_obj.strftime("%m,%d,%Y"),
                "Report Date": parsed_file_date.strftime("%m,%d,%Y"),
            }

        # Data Slicing and Header promotion
        df_clean = df.iloc[4:].copy()
        df_clean.columns = df_clean.iloc[0]
        df_clean = df_clean.iloc[1:].reset_index(drop=True)

        # Features
        df_clean["Brand"] = df_clean["Product"].astype(str).str.split().str[0]
        df_clean["Date"] = parsed_file_date
        df_clean["db_house"] = db_name

        return df_clean, None
    except Exception as e:
        print(f"Error processing file {file_path.name} in {file_path.parent}: {e}")
        return None, None


def file_processing_script(path_location, gc, spreadsheet_id):
    path_location = Path(path_location)
    if not path_location.exists():
        print(f"X Path does not exist: {path_location}")
        return pd.DataFrame()

    xlsx_files = [f for f in path_location.rglob("*.xlsx") if f.is_file()]
    print(f"Found {len(xlsx_files)} .xlsx file(s) across all subfolders.")

    records = []
    skipped_records = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(process_single_excel, xlsx_files))

    for valid_df, skipped_info in results:
        if valid_df is not None:
            records.append(valid_df)
        elif skipped_info is not None:
            skipped_records.append(skipped_info)

    df_skipped = pd.DataFrame(skipped_records)
    print("\n" + "=" * 50)
    print("SKIPPED FILES SUMMARY")
    print("=" * 50)
    if not df_skipped.empty:
        print(df_skipped.to_string(index=False))
    else:
        print("No files were skipped.")
    print("=" * 50 + "\n")

    if records:
        final_df = pd.concat(records, ignore_index=True)

        spreadsheet = gc.open_by_key(master_spreadsheet_id)
        worksheet = spreadsheet.worksheet("vendor_c_sku_price")
        data = worksheet.get_values("A:D")
        df_sku_rate = pd.DataFrame(data[1:], columns=data[0])
        df_sku_rate["per_piece_sale_price"] = pd.to_numeric(
            df_sku_rate["per_piece_sale_price"], errors="coerce"
        )
        df_sku_rate["per_piece_lifting_price"] = pd.to_numeric(
            df_sku_rate["per_piece_lifting_price"], errors="coerce"
        )

        merged_df = pd.merge(
            final_df,
            df_sku_rate,
            left_on="Code",
            right_on="sku_code",
            how="left"
        )

        for col in ["IMS", "ClosingStock", "ProductReceived"]:
            merged_df[col] = pd.to_numeric(merged_df[col], errors="coerce").fillna(0)

        merged_df["IMS_value"] = merged_df["IMS"] * merged_df["per_piece_sale_price"]
        merged_df["ClosingStock_value"] = merged_df["ClosingStock"] * merged_df["per_piece_lifting_price"]
        merged_df["ProductReceived_value"] = merged_df["ProductReceived"] * merged_df["per_piece_lifting_price"]

        merged_df.drop(
            columns=["sku_name", "sku_code", "per_piece_sale_price", "per_piece_lifting_price"],
            errors="ignore",
            inplace=True
        )

        final_df = merged_df[[
            "Date",
            "db_house",
            "Brand",
            "Product",
            "Code",
            "ProductReceived",
            "ProductReceived_value",
            "IMS",
            "IMS_value",
            "ClosingStock",
            "ClosingStock_value",
            "TransferOut"
        ]]

        final_df = final_df.drop_duplicates(
            subset=["Date", "db_house", "Code"], keep="last"
        )

        print("Combined processing complete!")
        my_reports = {"vendor_c_export": final_df}
        export_multiple_dfs_to_google_sheet(spreadsheet_id, my_reports, gc)
        return final_df
    else:
        print("X No valid Excel files processed.")
        return pd.DataFrame()


# ==========================================
# Execution Entry Point
# ==========================================
if __name__ == "__main__":
    gc = get_gspread_client()
    action_mode = os.getenv("ACTION_MODE", "Run")
    if action_mode.lower() == "run":
        print("Running RUN mode")
        file_processing_script(base_dir, gc, target_spreadsheet_id)
    elif action_mode.lower() == "check":
        print("Running CHECK mode")
        file_checking_script(base_dir, gc)
