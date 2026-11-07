import calendar
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import gspread
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# CONSTANTS & CONFIGURATION
# ==========================================
BI_SERVER_URL = os.getenv("BI_SERVER_URL")
SPREADSHEET_ID = os.getenv("TARGET_SPREADSHEET_ID")
SUB_ANCHOR_TYPE = "Telco"
TARGET_DB_IDS = [1001, 1002, 1003, 1004]
BL_DB_IDS = [1001, 1002]
USERNAME = os.getenv("BI_USERNAME")
PASSWORD = os.getenv("BI_PASSWORD")


# ==========================================
# AUTHENTICATION & HELPERS
# ==========================================
def get_metabase_session():
    """Authenticates with BI Server using environment variables."""
    if not USERNAME or not PASSWORD:
        raise ValueError("BI_USERNAME and BI_PASSWORD must be set in environment variables.")

    print("Authenticating with BI Server...")
    auth_url = f"{BI_SERVER_URL}/api/session"
    auth_payload = {"username": USERNAME, "password": PASSWORD}
    try:
        response = requests.post(auth_url, json=auth_payload)
        response.raise_for_status()
        return response.json()["id"]
    except requests.exceptions.HTTPError as e:
        if e.response is not None:
            print(f"\n[DEBUG] Status Code: {e.response.status_code}")
            print(f"[DEBUG] Error Response: {e.response.text}\n")
        raise e


def get_sheets_client():
    """Authenticates with Google Sheets API and opens the target workbook."""
    print("Authenticating with Google Sheets...")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        gc = gspread.service_account(filename=creds_path)
    else:
        gc = gspread.service_account()
    return gc.open_by_key(SPREADSHEET_ID)


def fetch_csv_df(session_token, question_id, parameters):
    """Executes BI Server CSV export request for a given question and parameters."""
    export_url = f"{BI_SERVER_URL}/api/card/{question_id}/query/csv"
    headers = {
        "X-Metabase-Session": session_token,
        "Content-Type": "application/json"
    }
    payload = {"parameters": parameters}
    csv_response = requests.post(export_url, headers=headers, json=payload)
    if not csv_response.ok:
        print(f"X BI Server Error Status Code: {csv_response.status_code}")
        print("X BI Server Error Details:")
        print(csv_response.text)
        csv_response.raise_for_status()
    return pd.read_csv(io.StringIO(csv_response.text))


def push_df_to_sheet(sh, worksheet_name, df):
    """Clears and writes the DataFrame to Google Sheets."""
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="26")

    worksheet.clear()
    formatted_df = df.copy()
    for col in formatted_df.columns:
        if formatted_df[col].apply(lambda x: isinstance(x, date)).any():
            formatted_df[col] = formatted_df[col].astype(str)

    data_to_write = [formatted_df.columns.values.tolist()] + formatted_df.values.tolist()
    worksheet.update(range_name="A1", values=data_to_write)
    print(f"✓ '{worksheet_name}' data successfully synced to Google Sheets!")


def get_dynamic_date_ranges():
    """Generates 3 date ranges for the current month (1st-10th, 11th-20th, 21st-End)."""
    today = date.today()
    year = today.year
    month = today.month
    last_day = calendar.monthrange(year, month)[1]
    return [
        (f"{year}-{month:02d}-01", f"{year}-{month:02d}-10"),
        (f"{year}-{month:02d}-11", f"{year}-{month:02d}-20"),
        (f"{year}-{month:02d}-21", f"{year}-{month:02d}-{last_day:02d}"),
    ]


# ==========================================
# PIPELINE 1: DMS DATA (QUESTION #9548)
# ==========================================
def transform_dms_data(df):
    """Transforms raw Question #9548 data."""
    temp_df = df.assign(
        delivered_date=pd.to_datetime(
            df["delivered_date"], errors="coerce"
        ).dt.strftime("%m/%d/%Y"),
        brand_name=np.select(
            [
                df["DB_id"].isin(BL_DB_IDS),
                df["product_name"].astype(str).str.lower().str.startswith("brand_b")
            ],
            ["Brand_A", "Brand_B"],
            default="Brand_C"
        )
    )
    temp_df = temp_df.query("DB_id in @TARGET_DB_IDS")
    grouped_df = temp_df.groupby(
        ["delivered_date", "DB_id", "product_name", "brand_name", "category"],
        as_index=False
    ).agg({
        "delivered_qty": "sum",
        "delivered_value": "sum",
        "nmv": "sum"
    })
    target_cols = [
        "delivered_date",
        "DB_id",
        "product_name",
        "brand_name",
        "category",
        "delivered_qty",
        "delivered_value",
        "nmv"
    ]
    keep_cols = [col for col in target_cols if col in grouped_df.columns]
    return grouped_df[keep_cols]


def run_pipeline():
    """Pipeline runner for Question #9548 -> 'dms_data'."""
    session_token = get_metabase_session()
    sh = get_sheets_client()
    date_ranges = get_dynamic_date_ranges()
    category_names = ["Scratch Cards", "Sim Cards"]
    tasks = []
    for category_name in category_names:
        for start_date, end_date in date_ranges:
            chunk_params = [
                {"type": "date/single", "target": ["variable", ["template-tag", "from"]], "value": start_date},
                {"type": "date/single", "target": ["variable", ["template-tag", "to"]], "value": end_date},
                {"type": "text", "target": ["variable", ["template-tag", "sub_anchor_type"]], "value": SUB_ANCHOR_TYPE},
                {"type": "text", "target": ["variable", ["template-tag", "category_name"]], "value": category_name}
            ]
            tasks.append((category_name, start_date, end_date, chunk_params))

    print("Downloading and combining data for Question #9548...")
    dfs = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_task = {
            executor.submit(fetch_csv_df, session_token, 9548, params): (cat, s_date, e_date)
            for cat, s_date, e_date, params in tasks
        }
        for future in as_completed(future_to_task):
            cat, s_date, e_date = future_to_task[future]
            print(f"Downloaded chunk: {cat} | {s_date} to {e_date}...")
            dfs.append(future.result())

    combined_raw_df = pd.concat(dfs, ignore_index=True).fillna("")
    transformed_df = transform_dms_data(combined_raw_df)
    push_df_to_sheet(sh, "dms_data", transformed_df)
    print("\nAll BI datasets successfully merged and synced to Google Sheets!")


# ==========================================
# PIPELINE 2: STOCK DATA (QUESTION #2999)
# ==========================================
def transform_stock_data(df):
    """Transforms raw Question #2999 data."""
    temp_df = df.assign(
        Date=pd.to_datetime(
            df["Date"], errors="coerce"
        ).dt.strftime("%m/%d/%Y"),
        brand_name=np.select(
            [
                df["DB_ID"].isin(BL_DB_IDS),
                df["product_name"].astype(str).str.lower().str.startswith("brand_b")
            ],
            ["Brand_A", "Brand_B"],
            default="Brand_C"
        )
    )
    temp_df = temp_df.query("DB_ID in @TARGET_DB_IDS")
    grouped_df = temp_df.groupby(
        ["Date", "DB_ID", "product_name", "brand_name", "category_name"],
        as_index=False
    ).agg({
        "fresh_inventory": "sum",
        "fresh_value": "sum"
    })
    target_cols = [
        "Date",
        "DB_ID",
        "product_name",
        "brand_name",
        "category_name",
        "fresh_inventory",
        "fresh_value"
    ]
    keep_cols = [col for col in target_cols if col in grouped_df.columns]
    return grouped_df[keep_cols]


def run_pipeline_1():
    """Pipeline runner for Question #2999 -> 'stock_data'."""
    session_token = get_metabase_session()
    sh = get_sheets_client()
    today = date.today()

    target_dates = list(dict.fromkeys([
        today - timedelta(days=1),
        date(today.year, today.month, 1) - timedelta(days=1)
    ]))

    category_names = ["Scratch Cards", "Sim Cards"]
    tasks = []
    for category_name in category_names:
        for target_date in target_dates:
            chunk_params = [
                {"type": "date/single", "target": ["variable", ["template-tag", "order_date"]], "value": str(target_date)},
                {"type": "text", "target": ["variable", ["template-tag", "category_name"]], "value": category_name}
            ]
            tasks.append((category_name, target_date, chunk_params))

    print("Downloading and combining data for Question #2999...")
    dfs = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_task = {
            executor.submit(fetch_csv_df, session_token, 2999, params): (cat, t_date)
            for cat, t_date, params in tasks
        }
        for future in as_completed(future_to_task):
            cat, t_date = future_to_task[future]
            print(f"Downloaded chunk: {cat} | {t_date}")
            dfs.append(future.result())

    combined_raw_df = pd.concat(dfs, ignore_index=True).fillna("")
    transformed_df = transform_stock_data(combined_raw_df)
    push_df_to_sheet(sh, "stock_data", transformed_df)
    print("\nAll BI datasets successfully merged and synced to Google Sheets!")


if __name__ == "__main__":
    run_pipeline()
    run_pipeline_1()
