import io
import os
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import gspread
from dotenv import load_dotenv

load_dotenv()

BI_SERVER_URL = os.getenv("BI_SERVER_URL")
USERNAME = os.getenv("BI_USERNAME")
PASSWORD = os.getenv("BI_PASSWORD")

SPREADSHEET_ID = os.getenv("TARGET_SPREADSHEET_ID")
TARGET_DB_IDS = [1001, 1002, 1003, 1004]
ANCHOR_TYPE = "FMCG"


def get_metabase_session():
    print("Authenticating with BI Server...")
    auth_url = f"{BI_SERVER_URL}/api/session"
    auth_payload = {"username": USERNAME, "password": PASSWORD}
    response = requests.post(auth_url, json=auth_payload)
    response.raise_for_status()
    return response.json()["id"]


def get_sheets_client():
    print("Authenticating with Google Sheets...")
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        gc = gspread.service_account(filename=creds_path)
    else:
        gc = gspread.service_account()
    return gc.open_by_key(SPREADSHEET_ID)


def push_df_to_sheet(sh, worksheet_name, df):
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="26")

    worksheet.clear()

    # Preserve original date-to-string conversion logic
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, date)).any():
            df[col] = df[col].astype(str)

    data_to_write = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.update(range_name="A1", values=data_to_write)
    print(f"✓ '{worksheet_name}' data successfully synced to Google Sheets!")


def fetch_csv_df(session_token, question_id, parameters):
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


def process_task(task_config, session_token, sh):
    name = task_config["name"]
    qid = task_config["qid"]
    params = task_config["params"]
    transform_fn = task_config["transform"]

    print(f"\nDownloading data for Question #{qid}...")
    df = fetch_csv_df(session_token, qid, params)
    df = df.fillna("")

    # Execute transformations
    df = transform_fn(df)

    # Push to Google Sheets
    push_df_to_sheet(sh, name, df)


def run_pipeline():
    session_token = get_metabase_session()
    sh = get_sheets_client()
    today = date.today()

    tasks = [
        # 1. Daily Sales (#237074)
        {
            "name": "daily_sales",
            "qid": 237074,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "from"]], "value": today.replace(day=1).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "to"]], "value": today.strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df[df['DB_ID'].isin(TARGET_DB_IDS)][
                    [col for col in [
                        "Date", "DB_ID", "DB", "Sales", "Market_Damage", "NMV",
                        "Claim", "Non_Claim",
                        "Market_Short_Given", "Market_Short_Collection", "Credit_Given",
                        "Credit_Collection", "Cash_Collection", "Deposit_Approved",
                        "Deposit_Not_Submitted", "Deposit_Pending", "Deposit_Rejected", "Deposit_Discrepancy"
                    ] if col in df.columns]
                ]
            )
        },
        # 2. Payment (#2838)
        {
            "name": "payment",
            "qid": 2838,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "START"]], "value": today.replace(day=1).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "END"]], "value": today.strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df[df['UDH_DB_ID'].isin(TARGET_DB_IDS)][
                    [col for col in [
                        "REQUISITION_CREATION_DATE", "REQUISITION_ID", "AMOUNT", "CURRENT_STATUS", "UDH_DB_ID", "UDH_DB_NAME"
                    ] if col in df.columns]
                ]
            )
        },
        # 3. Attendance (#236386)
        {
            "name": "attendance",
            "qid": 236386,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "start"]], "value": today.replace(day=1).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "end"]], "value": today.strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df[df['db_id'].isin(TARGET_DB_IDS)][
                    [col for col in [
                        "db_id", "db_name", "team", "employee_id", "emplyee_name", "employee_salary", "date", "attendance_type"
                    ] if col in df.columns]
                ]
            )
        },
        # 4. Expense (#8889)
        {
            "name": "expense",
            "qid": 8889,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "incur_start"]], "value": today.replace(day=1).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "incur_end"]], "value": today.strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df.assign(
                    db_id=pd.to_numeric(df['hub_id'].astype(str).str[-4:], errors='coerce')
                ).pipe(
                    lambda d: d[d['db_id'].isin(TARGET_DB_IDS)][
                        [col for col in [
                            "expense_incur_date", "expense_creation_date", "entity_type", "expense_status", "expense_amount", "hub_id", "db_id"
                        ] if col in d.columns]
                    ].drop(columns=['hub_id'], errors='ignore')
                )
            )
        },
        # 5. Daily Recon (#232076)
        {
            "name": "daily_recon",
            "qid": 232076,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "START"]], "value": today.replace(day=1).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "END"]], "value": today.strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df[df['warehouse_id'].isin(TARGET_DB_IDS)].assign(
                    order_date=lambda d: pd.to_datetime(d["order_date"]).dt.date.astype(str)
                )
            )
        },
        # 6. Outward (#849)
        {
            "name": "outward",
            "qid": 849,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "from"]], "value": (today - timedelta(days=4)).strftime("%Y-%m-%d")},
                {"type": "date/single", "target": ["variable", ["template-tag", "to"]], "value": (today - timedelta(days=1)).strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df.assign(
                    Pick_list_Type=~df['Pick_list_Type'].isin(['TRANSFER_ORDER', 'RETURN_TO_SUPPLIER'])
                ).pipe(
                    lambda d: d[d['DB_ID'].isin(TARGET_DB_IDS)][
                        [col for col in [
                            "Warehouse", "DB_ID", "DB", "Picklist_Created_Time", "Picklist_Updated_Time", "Picklist ID", "Outward",
                            "Picked_Quantity", "Delivered_Quantity", "Returned_Quantity",
                            "Remaining_Qty", "Lifting_Price", "Picked_Value",
                            "Delivered_Quantity_Value", "Return_Quantity_Value", "Remaining_Quantity_Value"
                        ] if col in d.columns]
                    ]
                ).assign(
                    Picklist_Updated_Time=lambda d: pd.to_datetime(d['Picklist_Updated_Time'], format='mixed').dt.date.astype(str)
                )
            )
        },
        # 7. Closing Balance (#2999)
        {
            "name": "closing_balance",
            "qid": 2999,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "order_date"]], "value": (today - timedelta(days=1)).strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: df[df['DB_ID'].isin(TARGET_DB_IDS)]
        },
        # 8. Pending PO (#15852)
        {
            "name": "pending_po",
            "qid": 15852,
            "params": [
                {"type": "date/single", "target": ["variable", ["template-tag", "start"]], "value": "2026-07-01"},
                {"type": "date/single", "target": ["variable", ["template-tag", "end"]], "value": (today + timedelta(days=1)).strftime("%Y-%m-%d")}
            ],
            "transform": lambda df: (
                df.assign(GRN_Time=pd.to_datetime(df['GRN_Time']).dt.date)[df['warehouse_id'].isin(TARGET_DB_IDS)]
            )
        },
        # 9. Payment VS GRN (#232911)
        {
            "name": "payment_vs_grn",
            "qid": 232911,
            "params": [
                {"type": "text", "target": ["variable", ["template-tag", "business_unit"]], "value": ANCHOR_TYPE},
                {"type": "text", "target": ["variable", ["template-tag", "DB_House_Status"]], "value": "Active"}
            ],
            "transform": lambda df: (
                df[
                    (df['DB_ID'].isin(TARGET_DB_IDS)) &
                    (df['Indent_Status'].isin(["Paid", "Processing"])) &
                    (df['Requisition_Status'].isin(["PROCESSOR_APPROVED", "PROCESSING"])) &
                    (df['GRN_Status'] != "Received")
                ][
                    [col for col in [
                        "DB_ID", "DB_Name", "Requisition_ID", "Anchor_Name", "Indent_Status",
                        "Requisition_Status", "Payment_Date", "GRN_Status", "Remaining_Amount"
                    ] if col in df.columns]
                ]
            )
        }
    ]

    with ThreadPoolExecutor(max_workers=9) as executor:
        futures = [executor.submit(process_task, task, session_token, sh) for task in tasks]
        for future in futures:
            future.result()

    print("\nAll 9 BI datasets successfully synced to Google Sheets!")


if __name__ == "__main__":
    run_pipeline()
