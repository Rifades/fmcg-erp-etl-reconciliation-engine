import os
from datetime import datetime
from pathlib import Path
import zoneinfo

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

load_dotenv()

# 1. Authentication & Base Directory
creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if creds_path and os.path.exists(creds_path):
    gc = gspread.service_account(filename=creds_path)
else:
    gc = gspread.service_account()

base_dir = os.getenv("BASE_DATA_DIR", "./data")
master_sheet_id = os.getenv("TARGET_SPREADSHEET_ID")
audit_output_sheet_id = os.getenv("TARGET_SPREADSHEET_ID")

# 2. Folder Configurations & Master Mappings
config = {
    "vendor_a": {
        "parent": "Vendor_A JBP Files",
        "subs": [
            "Stock Report",
            "Bill Wise Item Wise Sales Report",
            "GRN Listing",
            "Bill Wise Retailer Credit Note Adjustment",
        ],
    },
    "vendor_b": {
        "parent": "Vendor_B QPI Files (Field) (DDT)",
        "subs": [
            "Details Stock Report",
            "DSR+DT Statement Report (Summarized View)",
            "PSI Report",
            "Sales Analysis",
        ],
    },
    "vendor_c": {
        "parent": "Vendor_C Anchor System Data",
        "subs": ["Stock Position by Point Report"],
    },
}

company_sources = {
    "vendor_a": {
        "sheet_id": master_sheet_id,
        "worksheet": "Mapping Data",
        "range": "A1:C12",
        "ref_key": "dist_code",
    },
    "vendor_b": {
        "sheet_id": master_sheet_id,
        "worksheet": "Mapping Data",
        "range": "F1:J7",
        "ref_key": "DB ID",
    },
    "vendor_c": {
        "sheet_id": master_sheet_id,
        "worksheet": "Mapping Data",
        "range": "T1:V25",
        "ref_key": "Drishti ID & Name",
    },
}

# 3. Pre-load Master Data from Google Sheets
ref_dfs = {}
for company, src in company_sources.items():
    try:
        sh_ref = gc.open_by_key(src["sheet_id"])
        ws_ref = (
            sh_ref.worksheet(src["worksheet"])
            if isinstance(src["worksheet"], str)
            else sh_ref.get_worksheet(src["worksheet"])
        )
        cell_range = src.get("range")
        data = ws_ref.get(cell_range) if cell_range else ws_ref.get_all_values()
        if data and len(data) > 1:
            header = [str(col).strip() for col in data[0]]
            rows = data[1:]
            ref_df = pd.DataFrame(rows, columns=header)
            ref_df = ref_df.dropna(how="all")

            ref_key = src["ref_key"].strip()
            ref_df[ref_key] = ref_df[ref_key].astype(str).str.strip()
            ref_dfs[company] = ref_df
            print(
                f"Loaded {company} master ({cell_range}): {len(ref_df)} entities."
            )
        else:
            print(f"Warning: No valid data found in range {cell_range} for {company}")
            ref_dfs[company] = None
    except Exception as e:
        print(f"Error loading master sheet for {company}: {e}")
        ref_dfs[company] = None

# 4. Prompt Date & Scan Folders
date_input = input("Please Enter Date (Format DD-MM-YYYY): ").strip()
parsed_date = datetime.strptime(date_input, "%d-%m-%Y")
target_month = parsed_date.strftime("%B %Y")
target_date = parsed_date.strftime("%d-%m-%Y")

company_audit_tables = {}
for company, info in config.items():
    src = company_sources[company]
    ref_df = ref_dfs.get(company)
    if ref_df is None or ref_df.empty:
        print(f"Skipping audit for {company}: Reference master data is unavailable.")
        continue

    # Step A: Collect submitted files across all sub-folders.
    records = []
    for sub in info["subs"]:
        folder = os.path.join(
            base_dir, info["parent"], sub, target_month, target_date
        )
        files = (
            [
                f
                for f in os.listdir(folder)
                if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
            ]
            if os.path.exists(folder)
            else []
        )

        if company in ["vendor_a", "vendor_b"]:
            for f in files:
                parts = Path(f).stem.split("_")
                if len(parts) > 3:
                    continue
                records.append(
                    {
                        "join_id": parts[0].strip(),
                        "Report Name": sub,
                        "file_name": f,
                    }
                )
        elif company == "vendor_c":
            for f in files:
                try:
                    file_path = os.path.join(folder, f)
                    raw_df = pd.read_excel(file_path)
                    records.append(
                        {
                            "join_id": str(raw_df.columns[1]).strip(),
                            "Report Name": sub,
                            "file_name": Path(file_path).stem,
                            "date": raw_df.iloc[0, 1],
                            "report_date": raw_df.iloc[1, 1],
                        }
                    )
                except Exception as e:
                    print(f"Error processing {f}: {e}")

    df_files = pd.DataFrame(records)
    if not df_files.empty:
        df_files = df_files.drop_duplicates(
            subset=["join_id", "Report Name"], keep="first"
        )
    else:
        df_files = pd.DataFrame(columns=["join_id", "Report Name", "file_name"])

    # Step B: Build Full Expected Matrix (All Master DB IDs x All Sub-Reports)
    master_grid = ref_df.copy()
    master_grid["join_id"] = master_grid[src["ref_key"]].astype(str).str.strip()
    subs_df = pd.DataFrame({"Report Name": info["subs"]})
    expected_matrix = master_grid.merge(subs_df, how="cross")

    # Step C: Left-join uploaded files against expected matrix
    audit_df = pd.merge(
        expected_matrix,
        df_files,
        on=["join_id", "Report Name"],
        how="left",
    )

    # Step D: Flag Status and keep ONLY 'Missing' records
    audit_df["Status"] = audit_df["file_name"].apply(
        lambda x: "Received" if pd.notna(x) and str(x).strip() != "" else "Missing"
    )
    audit_df = audit_df[audit_df["Status"] == "Missing"].copy()

    # Drop temporary & file-specific metadata columns
    cols_to_drop = [
        c
        for c in ["join_id", "file_name", "date", "report_date"]
        if c in audit_df.columns
    ]
    audit_df = audit_df.drop(columns=cols_to_drop)

    # Organize columns and sort by Report Name and reference ID
    lead_cols = [src["ref_key"], "Report Name", "Status"]
    other_cols = [c for c in audit_df.columns if c not in lead_cols]
    audit_df = audit_df[lead_cols + other_cols]
    audit_df = audit_df.sort_values(
        by=["Report Name", src["ref_key"]], ascending=[True, True]
    )
    company_audit_tables[company] = audit_df

# 5. Export Audit Matrices to Google Sheets
sh = gc.open_by_key(audit_output_sheet_id)

for company, audit_table in company_audit_tables.items():
    tab_title = f"{company.upper()}_Missing"[:31]
    try:
        ws = sh.worksheet(tab_title)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=tab_title,
            rows=max(len(audit_table) + 15, 20),
            cols=max(len(audit_table.columns) + 2, 5),
        )

    # 1. Dedicate cell A1 to target date
    tz = zoneinfo.ZoneInfo("UTC")
    local_now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    ws.update(values=[[target_date]], range_name="A1")
    ws.update(values=[[local_now]], range_name="B1")

    # 2. Export the rest of the data starting from row 2 (A2)
    if not audit_table.empty:
        set_with_dataframe(ws, audit_table, row=2, col=1, include_index=False)
    else:
        ws.update(
            values=[["All files received! No missing reports."]],
            range_name="A2",
        )

print(f"\nMissing files audit complete: {sh.url}")
