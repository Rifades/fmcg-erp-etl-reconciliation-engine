import time
import gspread
from gspread_dataframe import set_with_dataframe


def export_multiple_dfs_to_google_sheet(
    spreadsheet_id_or_url, data_dict, gc_client, delay_between_sheets=2
):
    """
    Exports DataFrames to named worksheets in a Google Sheet.
    Resizes each sheet to exact dimensions to stay under the 10M cell limit.
    Includes exponential retry logic for 429 rate limits and API rate-limit buffer.
    """
    print("\n--- Exporting to Google Sheets ---")
    try:
        if str(spreadsheet_id_or_url).startswith("http"):
            sh = gc_client.open_by_url(spreadsheet_id_or_url)
        else:
            sh = gc_client.open_by_key(spreadsheet_id_or_url)
    except Exception as e:
        print(f"  [X] Could not open spreadsheet: {e}")
        return

    for sheet_name, df in data_dict.items():
        if df.empty:
            print(f"  [-] Skipping '{sheet_name}': DataFrame is empty.")
            continue

        try:
            # 1. Get or create worksheet
            try:
                worksheet = sh.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=sheet_name, rows=1, cols=1)

            # 2. Resize to exact dimensions
            rows_needed = len(df) + 1
            cols_needed = len(df.columns)
            print(f"  > Resizing '{sheet_name}' to {rows_needed}x{cols_needed}...")
            worksheet.resize(rows=rows_needed, cols=cols_needed)

            # 3. Convert dates to strings for Sheets compatibility
            df_export = df.copy()
            for col in df_export.columns:
                if df_export[col].dtype == 'object' or str(
                    df_export[col].dtype
                ).startswith('date'):
                    df_export[col] = df_export[col].astype(str)

            # 4. Clear & write with retry
            worksheet.clear()
            for attempt in range(5):
                try:
                    set_with_dataframe(worksheet, df_export, include_index=False)
                    break
                except gspread.exceptions.APIError as err:
                    status_code = getattr(getattr(err, "response", None), "status_code", None)
                    if status_code == 429 or attempt < 4:
                        wait = 10 * (2 ** attempt)
                        print(
                            f"  [!] API/Rate limit hit on '{sheet_name}'. Retrying in {wait}s..."
                        )
                        time.sleep(wait)
                    else:
                        raise
            print(f"  [+] Successfully updated: {sheet_name}")

            # Pace requests to prevent hitting 60 writes/min limit
            time.sleep(delay_between_sheets)
        except Exception as e:
            print(f"  [X] Failed to update '{sheet_name}': {e}")

    print("\nAll exports completed!")
