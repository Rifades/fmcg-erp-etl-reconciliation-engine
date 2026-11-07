import os
import re
import glob
import time
from pathlib import Path
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
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


def read_and_clean_excel(file_path, required_cols=None, sheet_name='Sheet1'):
    """
    Reads an Excel file, strips column-name whitespace,
    validates required columns, and returns a clean DataFrame.
    Returns None on any error.
    """
    try:
        df = pd.read_excel(
            file_path,
            skiprows=[0, 1],
            header=0,
            engine='calamine',
            sheet_name=sheet_name
        )
        df.columns = df.columns.str.strip()

        # Ensure column names are unique after stripping whitespace
        cols = pd.Series(df.columns)
        for idx, item in enumerate(cols):
            count = (cols.iloc[:idx] == item).sum()
            if count > 0:
                cols.iloc[idx] = f"{item}_{count}"
        df.columns = cols

        if required_cols:
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise KeyError(f"Missing expected columns: {missing_cols}")
            df = df[required_cols]

        return df
    except Exception as e:
        print(f"  [X] Error reading '{Path(file_path).name}': {e}")
        return None


def _process_single_stock_file(file_path):
    """Worker: processes one stock report file. Returns (summary_df, log_msg)."""
    source_cols = ['PrdDcode', 'PrdName', 'MRP', 'Saleable', 'DisplaySalRate', 'DisplayRate']
    path = Path(file_path)
    temp_df = read_and_clean_excel(path, required_cols=source_cols)
    if temp_df is None:
        return None, None

    # Clean rows
    temp_df = temp_df[temp_df['PrdDcode'].astype(str).str.upper() != 'TOTAL'].copy()
    temp_df = temp_df.dropna(subset=['PrdDcode'])

    # Compute (rate x qty, not a raw rate sum)
    temp_df['DisplaySalRate'] = pd.to_numeric(temp_df['DisplaySalRate'], errors='coerce').fillna(0)
    temp_df['Saleable'] = pd.to_numeric(temp_df['Saleable'], errors='coerce').fillna(0)
    temp_df['DisplaySalRate'] = temp_df['Saleable'] * temp_df['DisplaySalRate']

    # Extract Dist Code & Date from filename
    parts = path.stem.split('_')
    if len(parts) == 2:
        file_id, dist_name = parts[0], parts[1]
        temp_df['Dist Code'] = str(file_id).strip()
        temp_df['Date'] = date.today().strftime('%m-%d-%Y')
    elif len(parts) == 3:
        file_id = parts[0]
        parsed_date = pd.to_datetime(parts[1], errors='coerce')
        if pd.isna(parsed_date):
            return None, f"  [!] Skipping (unparseable date): {path.name}"
        temp_df['Dist Code'] = str(parts[0]).strip()
        temp_df['Date'] = parsed_date
        temp_df['Date'] = pd.to_datetime(temp_df['Date']).dt.strftime('%m-%d-%Y')
    else:
        return None, f"  [!] Skipping (unexpected filename format): {path.name}"

    summary = temp_df.groupby(['Date', 'Dist Code'], as_index=False).agg({'DisplaySalRate': 'sum'})
    return summary, None


def process_stock_report(file_list, max_workers=8):
    """
    Reads stock report files in parallel, computes
    (Saleable x DisplaySalRate), and returns a summary by Date & Dist Code.
    Output columns: Date | Dist Code | DisplaySalRate
    """
    print(f"\n--- Processing Stock Reports ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_stock_file, f): f for f in file_list}
        for future in as_completed(futures):
            result, log = future.result()
            if log:
                logs.append(log)
            if result is not None:
                dfs.append(result)

    for log in logs:
        print(log)
    print(f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}")

    if dfs:
        summary_df = pd.concat(dfs, ignore_index=True)
        dupes = summary_df.duplicated(subset=['Date', 'Dist Code'])
        if dupes.any():
            print(f"  [!] {dupes.sum()} duplicate Date+DistCode rows dropped (kept last)")
        return summary_df.drop_duplicates(
            subset=['Date', 'Dist Code'], keep='last'
        ).reset_index(drop=True)
    return pd.DataFrame()


def _process_single_billwise_itemwise_file(file_path):
    """Worker: processes one billwise itemwise file. Returns (aggregated_df, log_msg)."""
    source_cols = [
        'Dist Code', 'Bill Number', 'Retailer Code', 'Retailer Name', 'Status', 'ChannelName',
        'Gross Amt', 'NetAmount', 'Delivery Date', 'Product Name', 'Batch Code',
        'SecondaryScheme Amt', 'Sales Qty', 'Offer Qty', 'Total Qty', 'Bill Date'
    ]
    try:
        temp_df = pd.read_excel(
            file_path,
            skiprows=[0, 1],
            header=0,
            sheet_name='Sheet1'
        )
        missing_cols = [col for col in source_cols if col not in temp_df.columns]
        if missing_cols:
            return None, f"Missing columns {missing_cols} in '{file_path}'"

        temp_df = temp_df[source_cols]
        if temp_df.empty:
            return None, None

        if 'Dist Code' in temp_df.columns:
            temp_df['Dist Code'] = temp_df['Dist Code'].astype(str)

        temp_df['Delivery Date'] = pd.to_datetime(
            temp_df['Delivery Date'], dayfirst=True, errors='coerce'
        ).dt.date
        temp_df['Bill Date'] = pd.to_datetime(
            temp_df['Bill Date'], errors='coerce'
        ).dt.date

        temp_df = temp_df.dropna(subset=['Delivery Date', 'Dist Code'])
        if temp_df.empty:
            return None, None

        temp_df = temp_df.groupby(
            ['Dist Code', 'Bill Number', 'Delivery Date', 'Retailer Code', 'Retailer Name', 'ChannelName', 'Status', 'Product Name'],
            as_index=False, dropna=False
        ).agg({
            'Gross Amt': 'sum',
            'NetAmount': 'sum',
            'Sales Qty': 'sum',
            'Offer Qty': 'sum',
            'Total Qty': 'sum',
            'SecondaryScheme Amt': 'sum'
        })
        return temp_df, None
    except Exception as e:
        return None, f"Error processing file '{file_path}': {e}. Skipping this file."


def process_billwise_itemwise(file_list, max_workers=8):
    """
    Reads billwise itemwise files in parallel, filters delivered orders,
    and returns four DataFrames: sales_df, nonclaim_df, claim_df, helper_df_claim.
    """
    print(f"\n--- Processing Billwise Items ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_billwise_itemwise_file, f): f for f in file_list}
        for future in as_completed(futures):
            result, log = future.result()
            if log:
                logs.append(log)
            if result is not None:
                dfs.append(result)

    for log in logs:
        print(log)
    print(f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}")

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)

        # 1. Sales Dataframe
        sales_df = full_df[full_df['Status'] == 'Delivered'].groupby(
            ['Dist Code', 'Delivery Date'], as_index=False
        ).agg({'Gross Amt': 'sum'})

        # 2. Nonclaim Dataframe
        nonclaim_df = full_df[
            (full_df['ChannelName'] == 'Wholesale') & (full_df['Status'] == 'Delivered')
        ].groupby(
            ['Dist Code', 'Delivery Date', 'Retailer Code'], as_index=False
        ).agg({'NetAmount': 'sum'})

        # 3. Claim DataFrame
        claim_df = full_df[full_df['Status'] == 'Delivered'].groupby(
            ['Dist Code', 'Delivery Date'], as_index=False
        ).agg({'SecondaryScheme Amt': 'sum'})

        # 4. Helper for Claim Calculation
        helper_df_claim = full_df[full_df['Status'] == 'Delivered'].groupby(
            ['Dist Code', 'Delivery Date', 'Bill Number'], as_index=False
        ).agg({'Gross Amt': 'sum'})
        helper_df_claim = helper_df_claim.drop(columns=['Gross Amt'])

        return sales_df, nonclaim_df, claim_df, helper_df_claim
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def _process_single_damage(file_path):
    """Worker: processes one billwise itemwise file for returns/damage. Returns (aggregated_df, log_msg)."""
    source_cols = ["Status", "NetAmount", "Delivery Date", "Bill Date", "Dsr Name"]
    path = Path(file_path)
    try:
        parts = path.stem.split("_")
        if len(parts) != 3:
            return (
                None,
                f"  [!] Skipping (unexpected filename format): {path.name}",
            )
        parsed_date = pd.to_datetime(parts[1], errors="coerce")
        if pd.isna(parsed_date):
            return None, f"  [!] Skipping (unparseable date): {path.name}"
        dist_code = str(parts[0]).strip()

        temp_df = pd.read_excel(
            file_path,
            skiprows=[0, 1],
            header=0,
            sheet_name="Sheet1",
            engine="calamine",
        )

        missing_cols = [col for col in source_cols if col not in temp_df.columns]
        if missing_cols:
            return None, f"Missing columns {missing_cols} in '{file_path}'"

        keep_cols = source_cols + ["Dist Code", "File Date"]
        temp_df["Dist Code"] = dist_code
        temp_df["File Date"] = parsed_date
        temp_df = temp_df[keep_cols]
        if temp_df.empty:
            return None, None

        temp_df["Delivery Date"] = pd.to_datetime(
            temp_df["Delivery Date"], dayfirst=True, errors="coerce"
        ).dt.date
        temp_df["Bill Date"] = pd.to_datetime(
            temp_df["Bill Date"], errors="coerce"
        ).dt.date

        date_lookup = temp_df.loc[
            temp_df["Status"] == "Delivered",
            ["Dsr Name", "Bill Date", "Delivery Date"],
        ].drop_duplicates()

        df_final = temp_df.loc[
            temp_df["Status"] == "Return",
            ["Dsr Name", "Bill Date", "NetAmount", "Dist Code"],
        ].copy()
        df_final["NetAmount"] *= -1

        df_final = (
            df_final.merge(
                date_lookup, on=["Dsr Name", "Bill Date"], how="left"
            )
            .groupby(["Dist Code", "Dsr Name", "Bill Date"])
            .agg({"NetAmount": "sum"})
            .reset_index()
        )
        return df_final, None
    except Exception as e:
        return (
            None,
            f"Error processing file '{file_path}': {e}. Skipping this file.",
        )


def process_damage(file_list, max_workers=8):
    print(f"\n--- Processing Billwise Items ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_single_damage, f): f for f in file_list
        }
        for future in as_completed(futures):
            result, log = future.result()
            if log:
                logs.append(log)
            if result is not None and not result.empty:
                dfs.append(result)

    for log in logs:
        print(log)
    print(
        f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}"
    )

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        full_df = (
            full_df.groupby(["Dist Code", "Dsr Name", "Bill Date"])
            .agg({"NetAmount": "sum"})
            .reset_index()
        )
        return full_df
    return pd.DataFrame()


def _process_single_grn_file(file_path):
    """Worker: processes one GRN listing file. Returns (df, log_msg)."""
    source_cols = ['GRN Date', 'Net Amt.', 'AIT']
    temp_df = read_and_clean_excel(file_path, required_cols=source_cols)
    if temp_df is None:
        return None, None

    temp_df['GRN Date'] = pd.to_datetime(
        temp_df['GRN Date'],
        format='%b %d %Y %I:%M%p',
        errors='coerce'
    ).dt.date

    null_count = temp_df['GRN Date'].isna().sum()
    log = None
    if null_count > 0:
        log = f"  [!] {null_count} rows dropped (unparseable GRN Date): {Path(file_path).name}"
    temp_df = temp_df.dropna(subset=['GRN Date'])
    if temp_df.empty:
        return None, log

    parts = Path(file_path).stem.split('_')
    if len(parts) > 1:
        temp_df['Dist Code'] = str(parts[0]).strip()
        return temp_df, log
    return None, f"  [!] Skipping (no Dist Code in filename): {Path(file_path).name}"


def process_grn_listing(file_list, max_workers=8):
    """
    Reads GRN listing files in parallel and returns aggregated inward data.
    Output columns: Dist Code | GRN Date | Net Amt. | AIT | Lifting Value
    """
    print(f"\n--- Processing GRN Listings ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_grn_file, f): f for f in file_list}
        for future in as_completed(futures):
            result, log = future.result()
            if log:
                logs.append(log)
            if result is not None:
                dfs.append(result)

    for log in logs:
        print(log)
    print(f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}")

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        agg_df = final_df.groupby(
            ['Dist Code', 'GRN Date'], as_index=False
        ).agg({'Net Amt.': 'sum', 'AIT': 'sum'})
        agg_df['Lifting Value'] = agg_df['Net Amt.'] + agg_df['AIT']
        return agg_df
    return pd.DataFrame()


def _process_single_claims_file(file_path):
    """Worker: processes one claims file. Returns (df, log_msg)."""
    source_cols = ['Bill No', 'Amt Adj So For']
    temp_df = read_and_clean_excel(file_path, required_cols=source_cols)
    if temp_df is None:
        return None, None
    if temp_df.empty:
        return None, f"  [!] Skipping (no matching Bill No rows): {Path(file_path).name}"

    parts = Path(file_path).stem.split('_')
    if len(parts) < 2:
        return None, f"  [!] Skipping (no Dist Code in filename): {Path(file_path).name}"

    temp_df['Dist Code'] = str(parts[0]).strip()
    if 'Bill No' in temp_df.columns:
        temp_df = temp_df.rename(columns={'Bill No': 'Bill Number'})
    temp_df = temp_df[temp_df['Bill Number'].notna()]
    return temp_df, None


def process_claims(file_list, df_helper_claim, max_workers=8):
    """
    Reads claims files in parallel and combines them.
    Output columns: Bill Number | Amt Adj So For | Dist Code | Delivery Date
    """
    print(f"\n--- Processing Claims ({len(file_list)} files) ---")
    dfs, logs = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_claims_file, f): f for f in file_list}
        for future in as_completed(futures):
            result, log = future.result()
            if log:
                logs.append(log)
            if result is not None:
                dfs.append(result)

    for log in logs:
        print(log)
    print(f"Finished. Processed: {len(dfs)}, Skipped: {len(file_list) - len(dfs)}")

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        final_df = pd.merge(final_df, df_helper_claim, on=['Dist Code', 'Bill Number'], how='left')
        return final_df
    return pd.DataFrame()


if __name__ == "__main__":
    start_time = time.time()
    gc = get_gspread_client()

    # A. Setup Variables
    base_dir = os.getenv("VENDOR_A_BASE_DIR", "./data/Vendor_A JBP Files")
    target_spreadsheet_id = os.getenv("TARGET_SPREADSHEET_ID")

    # User Input: Date
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

    sub_folders = {
        'billwise': 'Bill Wise Item Wise Sales Report',
        'stock': 'Stock Report',
        'grn': 'GRN Listing',
        'crnotrep': 'Bill Wise Retailer Credit Note Adjustment',
        'damage': 'Bill Wise Item Wise Sales Report'
    }

    # B. File Discovery
    file_lists = {}
    print("\n--- Searching for Files ---")
    for key, folder_name in sub_folders.items():
        if key in ['stock', 'delconstat', 'on_vehicle']:
            full_path = os.path.join(base_dir, folder_name)
        else:
            full_path = os.path.join(base_dir, folder_name, calculation_month, result_date)

        files = []
        if os.path.exists(full_path):
            for root, _, filenames in os.walk(full_path):
                for filename in filenames:
                    if filename.lower().endswith(('.xlsx', '.xls')):
                        files.append(os.path.join(root, filename))

        file_lists[key] = files
        status = f"{len(files)} files" if files else "0 files - check path!"
        print(f"  {folder_name}: {status}")

    # C. Data Processing (Parallel)
    df_stock = process_stock_report(file_lists['stock'])
    df_sales, df_non_claim, claim_df, df_helper_claim = process_billwise_itemwise(file_lists['billwise'])
    df_grn = process_grn_listing(file_lists['grn'])
    df_claims = process_claims(file_lists['crnotrep'], df_helper_claim)
    damage_df = process_damage(file_lists['damage'])

    # Validation summary
    print("\n--- Data Summary ---")
    for name, df in [
        ('Stock Summary', df_stock),
        ('Sales', df_sales),
        ('Non-Claim', df_non_claim),
        ('GRN/Inward', df_grn),
        ('Claims', df_claims),
        ('Damage', damage_df)
    ]:
        status = f"{len(df)} rows" if not df.empty else "EMPTY"
        print(f"  {name:<18}: {status}")

    # D. Consolidation & Export
    my_reports = {
        'Vendor_A_Stock_Summary': df_stock,
        'Vendor_A_Non_Claim': df_non_claim,
        'Vendor_A_Inward': df_grn,
        'Vendor_A_Sales': df_sales,
        'Vendor_A_Secondary_Claim': claim_df,
        'Vendor_A_Claim': df_claims,
        'Vendor_A_Damage': damage_df
    }

    export_multiple_dfs_to_google_sheet(target_spreadsheet_id, my_reports, gc)
    print(f"\nTotal execution time: {time.time() - start_time:.1f}s")
