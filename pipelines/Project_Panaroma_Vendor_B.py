import os
import glob
import time
from pathlib import Path
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm
import gspread
from dotenv import load_dotenv

from utils.sheets_exporter import export_multiple_dfs_to_google_sheet

load_dotenv()


def get_gspread_client():
    """Authenticates with Google Sheets via service account or default credentials."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        return gspread.service_account(filename=creds_path)
    return gspread.service_account()


def scan_files(base_dir, calculation_month, result_date):
    """Scans and maps required files, keeping the latest modified file for Customer Balances."""
    sub_folders = {
        # 'cust_bal_Vendor_B_1': 'Customer Balance (Vendor_B_1)',
        # 'cust_bal_Vendor_B_2': 'Customer Balance (Vendor_B_2)',
        # 'sales_analysis': 'Sales Analysis',
        # 'payout': 'Payout (DDT)',
        # 'inward': 'PSI Report',
        'stock': 'Details Stock Report',
        # 'dsr_dt_statement': 'DSR+DT Statement Report (Summarized View)'
    }
    direct_path_keys = {'stock', 'cust_bal_Vendor_B_1', 'cust_bal_Vendor_B_2', 'dsr_dt_statement'}
    file_lists = {}
    print("\n--- Scanning for Files ---")

    for key, folder_name in sub_folders.items():
        base = Path(base_dir) / folder_name
        if key in direct_path_keys:
            target_path = base
            all_found = (
                list(target_path.rglob("*.xlsx"))
                + list(target_path.rglob("*.xls"))
                + list(target_path.rglob("*.xlsm"))
            )
        else:
            target_path = base / calculation_month / result_date
            all_found = (
                list(target_path.glob("*.xlsx"))
                + list(target_path.glob("*.xls"))
                + list(target_path.rglob("*.xlsm"))
            )

        # Deduplicate Customer Balances (keep latest per parent folder)
        if key in ['cust_bal_Vendor_B_1', 'cust_bal_Vendor_B_2'] and all_found:
            latest_map = {}
            for file_path in all_found:
                parent_folder = file_path.parent
                if (
                    parent_folder not in latest_map
                    or file_path.stat().st_mtime > latest_map[parent_folder].stat().st_mtime
                ):
                    latest_map[parent_folder] = file_path
            files = [str(f) for f in latest_map.values()]
        else:
            files = [str(f) for f in all_found]

        file_lists[key] = files
        print(f"  Found {len(files)} files for '{key}' ({folder_name}).")
    return file_lists


def _process_single_cust_bal_file(file_path, brand):
    """Worker: Processes one Customer Balance file."""
    try:
        temp_df = pd.DataFrame()
        effective_sheet_name = 'Circulate'
        effective_header_row = 4

        # Stage 1: Initial Read Attempt
        try:
            temp_df = pd.read_excel(file_path, sheet_name=effective_sheet_name, header=effective_header_row)
            print(f"  [!] Info: Initial read attempt for {Path(file_path).name} with sheet '{effective_sheet_name}' and header={effective_header_row}")
            temp_df.columns = temp_df.columns.astype(str).str.strip()
        except ValueError as ve:
            if "Worksheet named 'Circulate' not found" in str(ve):
                print(f"  [!] Warning: '{effective_sheet_name}' sheet not found in {Path(file_path).name}. Attempting first sheet.")
                effective_sheet_name = 0
            else:
                raise

        # Stage 2: Header Auto-detection & Re-read if needed
        town_keywords = ['town', 'twn', 'city', 'location', 'district', 'area', 'region', 'thana', 'upazila']
        town_col_found_in_initial_read = not temp_df.empty and any(
            keyword in col.lower() for col in temp_df.columns for keyword in town_keywords
        )

        if temp_df.empty or not town_col_found_in_initial_read:
            if not temp_df.empty and not town_col_found_in_initial_read:
                print(f"  [!] Info: Plausible town column not found with initial read settings for {Path(file_path).name}. Auto-detecting header.")
            elif temp_df.empty:
                print(f"  [!] Info: Initial read attempt failed or returned empty for {Path(file_path).name}. Auto-detecting header.")

            temp_df_peek = pd.read_excel(file_path, sheet_name=effective_sheet_name, header=None, nrows=20)
            detected_header_row_index = -1
            for i in range(len(temp_df_peek)):
                row_values_lower = temp_df_peek.iloc[i].astype(str).str.strip().str.lower().tolist()
                if any(keyword in val for val in row_values_lower for keyword in town_keywords):
                    detected_header_row_index = i
                    break

            if detected_header_row_index != -1:
                effective_header_row = detected_header_row_index
                print(f"  [!] Info: Auto-detected header for {Path(file_path).name} at index {effective_header_row} (row {effective_header_row + 1}).")
            else:
                effective_header_row = 0
                print(f"  [!] Warning: Could not auto-detect 'Town' column header in {Path(file_path).name}. Falling back to header=0.")

            temp_df = pd.read_excel(file_path, sheet_name=effective_sheet_name, header=effective_header_row)
            temp_df.columns = temp_df.columns.astype(str).str.strip()

        if temp_df.empty:
            return None, f"Error processing {Path(file_path).name}: DataFrame is empty after reading with detected header."

        # Stage 3: Identify and standardize 'Town' column
        town_col_expected = 'TOWN' if brand == 'Vendor_B_1' else 'Town'
        actual_town_col = None

        for col in temp_df.columns:
            if col.lower() == town_col_expected.lower():
                actual_town_col = col
                break

        if actual_town_col is None:
            for col in temp_df.columns:
                if any(keyword in col.lower() for keyword in town_keywords):
                    actual_town_col = col
                    print(f"  [!] Info: Broadly matched column '{col}' as town column in {Path(file_path).name}.")
                    break

        if actual_town_col is None:
            print(f"  [!] Debug: Columns present in {Path(file_path).name}: {list(temp_df.columns)}")
            return None, f"Error: Failed to identify any 'Town'-like column in {Path(file_path).name}. Expected: '{town_col_expected}'."
        elif actual_town_col != town_col_expected:
            print(f"  [!] Warning: Renaming '{actual_town_col}' to '{town_col_expected}' in {Path(file_path).name}")
            temp_df = temp_df.rename(columns={actual_town_col: town_col_expected})

        # Anonymized region filter
        rows_to_keep = ["Region_A", "Region_B", "Region_C"]
        if town_col_expected not in temp_df.columns:
            return None, f"Error: Renaming did not result in expected column '{town_col_expected}' in {Path(file_path).name}"

        temp_df = temp_df[temp_df[town_col_expected].astype(str).isin(rows_to_keep)].reset_index(drop=True)
        if temp_df.empty:
            return None, f"Warning: No relevant rows found in {Path(file_path).name} after filtering by region."

        target_indices = [0, 1, 2, 3, 4, 13]
        if len(temp_df.columns) <= max(target_indices):
            print(f"  [!] Debug: Columns present in {Path(file_path).name} before iloc: {list(temp_df.columns)}")
            return None, f"Error processing {Path(file_path).name}: Not enough columns for target_indices {target_indices}."

        temp_df = temp_df.iloc[:, target_indices]
        date_col_name = temp_df.columns[-1]
        try:
            date_val = pd.to_datetime(date_col_name)
        except ValueError:
            return None, f"Error processing {Path(file_path).name}: Could not parse date from column name '{date_col_name}'."

        temp_df['Date'] = date_val
        if len(temp_df.columns) < 2:
            return None, f"Error processing {Path(file_path).name}: Not enough columns to rename the balance column."

        temp_df = temp_df.rename(columns={temp_df.columns[-2]: f'Customer Balance {brand}'})
        temp_df = temp_df.rename(columns={town_col_expected: 'Join_Town'})
        return temp_df, None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {type(e).__name__}: {e}"


def process_customer_balance(file_list_b1, file_list_b2, max_workers=8):
    """Parallel processes customer balances and merges them into a total balance dataframe."""
    print("\n--- Processing Customer Balances ---")

    def run_parallel(files, brand):
        dfs, logs = [], []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_single_cust_bal_file, f, brand): f for f in files}
            for future in as_completed(futures):
                res, log = future.result()
                if log:
                    logs.append(log)
                if res is not None:
                    dfs.append(res)
        for log in logs:
            print(log)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    df_b1 = run_parallel(file_list_b1, 'Vendor_B_1')
    df_b2 = run_parallel(file_list_b2, 'Vendor_B_2')

    if df_b1.empty or df_b2.empty:
        print("  [!] Warning: One or both customer balance dataframes are empty. Cannot merge.")
        return pd.DataFrame()

    total_customer_balance = df_b1.merge(
        df_b2[['Join_Town', 'Customer Balance Vendor_B_2', 'Date']],
        left_on=['Join_Town', 'Date'],
        right_on=['Join_Town', 'Date'],
        how="inner"
    )
    total_customer_balance['Total Customer Balance'] = (
        total_customer_balance['Customer Balance Vendor_B_1'] + total_customer_balance['Customer Balance Vendor_B_2']
    )

    total_customer_balance = total_customer_balance.rename(columns={'Join_Town': 'TOWN'})
    cols_to_drop = [
        c for c in ['Customer Balance Vendor_B_1', 'Customer Balance Vendor_B_2', 'CLUSTER', 'AREA', 'Territory']
        if c in total_customer_balance.columns
    ]
    total_customer_balance = total_customer_balance.drop(columns=cols_to_drop)
    return total_customer_balance


def _process_single_sales_file(file_path):
    """Worker: Pre-aggregates sales data to save memory."""
    cols_to_keep = ['Town Name', 'Sub Element', 'Delivery Date', 'Sales Value', 'Fresh Return Value']
    try:
        temp_df = pd.read_excel(file_path, engine="calamine")
        if 'Region' in temp_df.columns:
            temp_df = temp_df[temp_df['Region'] != 'Total']

        missing_cols = [col for col in cols_to_keep if col not in temp_df.columns]
        if missing_cols:
            print(f"  [!] Error: Missing columns {missing_cols} in {Path(file_path).name}")
            return None, f"Missing columns in {Path(file_path).name}"

        temp_df = temp_df[cols_to_keep]
        temp_df['Delivery Date'] = pd.to_datetime(temp_df['Delivery Date'], errors='coerce')
        temp_df['Fresh Return Value'] = pd.to_numeric(temp_df['Fresh Return Value'], errors='coerce').fillna(0)
        temp_df['Sales Value'] = pd.to_numeric(temp_df['Sales Value'], errors='coerce').fillna(0)
        temp_df = temp_df.dropna(subset=['Delivery Date'])
        if temp_df.empty:
            return None, None

        agg_df = temp_df.groupby(['Town Name', 'Delivery Date', 'Sub Element'], as_index=False).agg({
            'Sales Value': 'sum',
            'Fresh Return Value': 'sum'
        })
        return agg_df, None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {e}"


def process_sales_analysis(file_list, max_workers=8):
    """Processes Sales Analysis and splits into Non-Claim and Daily Sales views."""
    print(f"\n--- Processing Sales Analysis ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_sales_file, f): f for f in file_list}
        for future in as_completed(futures):
            res, log = future.result()
            if log:
                logs.append(log)
            if res is not None:
                dfs.append(res)

    for log in logs:
        print(log)
    if not dfs:
        return pd.DataFrame(), pd.DataFrame()

    full_df = pd.concat(dfs, ignore_index=True)

    # 1. Non-Claim
    non_claim = full_df[full_df['Sub Element'] == 'SUB_CHANNEL_A'].groupby(
        ['Town Name', 'Delivery Date', 'Sub Element'], as_index=False
    ).agg({'Sales Value': 'sum', 'Fresh Return Value': 'sum'})
    non_claim['Final Sales'] = non_claim['Sales Value'] + non_claim['Fresh Return Value']
    non_claim['Non Claim Usage'] = non_claim['Final Sales'] * (2.87 / 100)
    non_claim = non_claim.drop(['Sub Element', 'Final Sales', 'Sales Value', 'Fresh Return Value'], axis=1)

    # 2. Daily Sales
    daily_sales = full_df.drop('Sub Element', axis=1).groupby(
        ['Town Name', 'Delivery Date'], as_index=False
    ).agg({'Sales Value': 'sum', 'Fresh Return Value': 'sum'})
    daily_sales['Final Sales'] = daily_sales['Sales Value'] + daily_sales['Fresh Return Value']

    return non_claim, daily_sales


def _process_single_inward_file(file_path):
    cols_to_keep = ['Town', 'Invoice Date', 'Value After Discount', 'Invoice Number']
    try:
        temp_df = pd.read_excel(file_path)
        missing_cols = [col for col in cols_to_keep if col not in temp_df.columns]
        if missing_cols:
            return None, f"Missing columns in {Path(file_path).name}"
        temp_df = temp_df[cols_to_keep]
        temp_df = temp_df.groupby(['Town', 'Invoice Date'], as_index=False).agg({
            'Value After Discount': 'sum',
            'Invoice Number': 'nunique'
        })
        return temp_df, None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {e}"


def process_inward(file_list, max_workers=8):
    print(f"\n--- Processing PSI/Inward ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_inward_file, f): f for f in file_list}
        for future in as_completed(futures):
            res, log = future.result()
            if log:
                logs.append(log)
            if res is not None:
                dfs.append(res)

    for log in logs:
        print(log)
    if dfs:
        return pd.concat(dfs, ignore_index=True).groupby(['Town', 'Invoice Date'], as_index=False).agg({
            'Value After Discount': 'sum',
            'Invoice Number': 'sum'
        })
    return pd.DataFrame()


def _process_single_dsr_dt_statement(file_path):
    cols_to_keep = ['Town', 'Total Discount', 'Incentive Credit Note', 'Market Return Credit Note']
    try:
        temp_df = pd.read_excel(file_path)
        missing_cols = [col for col in cols_to_keep if col not in temp_df.columns]
        if missing_cols:
            return None, f"Missing columns in {Path(file_path).name}"
        if 'Region' in temp_df.columns:
            temp_df = temp_df[temp_df['Region'] != 'Total']
        temp_df = temp_df[cols_to_keep]

        file_path_obj = Path(file_path)
        parts = file_path_obj.stem.split('_')
        if len(parts) >= 2:
            file_id = parts[0]
            date_val = pd.to_datetime(parts[1])
        else:
            file_id = "Unknown"
            date_val = "Unknown"

        temp_df['Dist Code'] = file_id
        temp_df['Date'] = date_val
        temp_df['Total Discount'] = temp_df['Total Discount'] * (-1)
        temp_df = temp_df.groupby(['Dist Code', 'Date', 'Town'], as_index=False).agg({
            'Total Discount': 'sum',
            'Incentive Credit Note': 'sum',
            'Market Return Credit Note': 'sum'
        })
        return temp_df, None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {e}"


def _process_dsr_dt_statement(file_list, max_workers=8):
    print(f"\n--- Processing DSR+DT Statement Report (Summarized View) ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_dsr_dt_statement, f): f for f in file_list}
        for future in as_completed(futures):
            res, log = future.result()
            if log:
                logs.append(log)
            if res is not None:
                dfs.append(res)

    for log in logs:
        print(log)
    if dfs:
        return pd.concat(dfs, ignore_index=True).groupby(['Dist Code', 'Date', 'Town'], as_index=False).agg({
            'Total Discount': 'sum',
            'Incentive Credit Note': 'sum',
            'Market Return Credit Note': 'sum'
        })
    return pd.DataFrame()


def _process_single_payout_file(file_path):
    cols_to_keep = ['Town Name', 'Adjusted Amount', 'Invoice Delivery Date']
    try:
        temp_df = pd.read_excel(file_path)
        if 'Credit Note Type' in temp_df.columns:
            temp_df = temp_df[temp_df['Credit Note Type'] == 'Credit Note Incentive Reward']
        missing_cols = [col for col in cols_to_keep if col not in temp_df.columns]
        if missing_cols:
            return None, f"Missing columns in {Path(file_path).name}"
        temp_df = temp_df[cols_to_keep]
        temp_df = temp_df.groupby(['Town Name', 'Invoice Delivery Date'], as_index=False).agg({'Adjusted Amount': 'sum'})
        return temp_df, None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {e}"


def process_payout(file_list, max_workers=8):
    print(f"\n--- Processing Payouts ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_payout_file, f): f for f in file_list}
        for future in as_completed(futures):
            res, log = future.result()
            if log:
                logs.append(log)
            if res is not None:
                dfs.append(res)

    for log in logs:
        print(log)
    if dfs:
        return pd.concat(dfs, ignore_index=True).groupby(['Town Name', 'Invoice Delivery Date'], as_index=False).agg({'Adjusted Amount': 'sum'})
    return pd.DataFrame()


NUMERIC_COLUMNS = [
    "Closing Value ALP",
    "Total Out Value ALP",
    "Sec Intransit Value ALP",
    "PSI Intransit Value LP",
    "Closing Value ALP (Incl. Jupiter)",
    "Total Closing Stock (Incl. Intransit & Jupiter)",
]


def _process_single_stock_file(file_path):
    """Worker: Fast extracts and filters raw records from one stock file."""
    cols_to_keep = [
        "Stock Date",
        "Distributor Code",
        "Stock Type",
    ] + NUMERIC_COLUMNS

    try:
        temp_df = pd.read_excel(file_path, engine="calamine")
        if temp_df.empty:
            return None, None

        temp_df.columns = temp_df.columns.str.strip()
        existing_cols = [col for col in cols_to_keep if col in temp_df.columns]
        temp_df = temp_df[existing_cols]
        if "Stock Type" not in temp_df.columns:
            return (
                None,
                f"X Missing column 'Stock Type' in {Path(file_path).name}",
            )

        temp_df = temp_df[temp_df["Stock Type"] == "01 Sound"]
        if temp_df.empty:
            return None, None

        for num_col in NUMERIC_COLUMNS:
            if num_col in temp_df.columns:
                temp_df[num_col] = pd.to_numeric(
                    temp_df[num_col], errors="coerce"
                ).fillna(0)

        return temp_df.drop(columns=["Stock Type"]), None
    except Exception as e:
        return None, f"Error processing {Path(file_path).name}: {e}"


def process_stock_report(file_list, max_workers=8):
    """Parallel processes stock reports and runs a single unified global aggregation."""
    print(f"\n--- Processing Stock Reports ({len(file_list)} files) ---")
    dfs, logs = [], []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_single_stock_file, f): f
            for f in file_list
        }

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Processing Stock Reports",
        ):
            res, log = future.result()
            if log:
                logs.append(log)
            if res is not None and not res.empty:
                dfs.append(res)

    for log in logs:
        print(log)
    print(
        f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}"
    )
    if not dfs:
        return pd.DataFrame()

    final_df = pd.concat(dfs, ignore_index=True)
    final_df["Stock Date"] = pd.to_datetime(
        final_df["Stock Date"], errors="coerce"
    )
    final_df = final_df.dropna(subset=["Stock Date"])

    agg_dict = {
        col: "sum" for col in NUMERIC_COLUMNS if col in final_df.columns
    }
    if not agg_dict:
        print("Warning: No matching numeric columns found for aggregation.")
        return pd.DataFrame()

    final_df = final_df.groupby(
        ["Stock Date", "Distributor Code"], as_index=False
    ).agg(agg_dict)
    return final_df


if __name__ == "__main__":
    start_time = time.time()
    gc = get_gspread_client()

    base_dir = os.getenv("VENDOR_B_BASE_DIR", "./data/Vendor_B QPI Files (Field) (DDT)")
    target_spreadsheet_id = os.getenv("TARGET_SPREADSHEET_ID")

    while True:
        raw_date = input("Enter result date (DD-MM-YYYY): ").strip()
        try:
            parsed = pd.to_datetime(raw_date, format='%d-%m-%Y')
            result_date = parsed.strftime('%d-%m-%Y')
            calculation_month = parsed.strftime('%B %Y')
            print(f"  Result Date       : {result_date}")
            print(f"  Calculation Month : {calculation_month}")
            break
        except ValueError:
            print("  [!] Invalid format. Please use DD-MM-YYYY (e.g. 22-04-2026)")

    # 1. Map Files
    file_lists = scan_files(base_dir, calculation_month, result_date)

    # 2. Process Files in Parallel
    df_stock = process_stock_report(file_lists.get('stock', []))

    # 3. Consolidate Results
    my_reports = {
        'DDT_Stock_Summary_Vendor_B': df_stock,
    }

    # 4. Export
    export_multiple_dfs_to_google_sheet(target_spreadsheet_id, my_reports, gc)
    print(f"\n[Total execution time: {time.time() - start_time:.1f}s]")
