import requests
import urllib3
import pandas as pd
import os
import smartsheet

urllib3.disable_warnings()

def push_assets_to_smartsheet():
    # -----------------------------
    # 1. Fetch assets data
    # -----------------------------
    BASE_URL = "https://admin.smartsheet.com/api/acs/v1/planinsights/assets"

    HEADERS = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Cookie": os.getenv("SMARTSHEET_COOKIE"),
        "User-Agent": "Mozilla/5.0",
        "x-smar-xsrf-token": os.getenv("SMARTSHEET_XSRF_TOKEN")
    }

    resp = requests.get(BASE_URL, headers=HEADERS, verify=False)
    resp.raise_for_status()
    result = resp.json()

    assets = result.get("assets", [])
    df = pd.json_normalize(assets, sep="_")

    # -----------------------------
    # 2. Keep only required columns
    # -----------------------------
    required_columns = [
        "assetType",
        "assetCount",
        "pctOfTotal",
        "lastUpdated",
        "created_currentValue",
        "viewed_currentValue",
        "shared_currentValue",
        "edits_currentValue"
    ]

    df = df[[c for c in required_columns if c in df.columns]]

    # -----------------------------
    # 3. Smartsheet setup
    # -----------------------------
    ss = smartsheet.Smartsheet(os.getenv("SM_TOKEN"))
    ss.errors_as_exceptions(True)

    SHEET_ID = int(os.getenv("SM_SHEET_ID"))

    sheet = ss.Sheets.get_sheet(SHEET_ID)

    # Column title → ID map
    col_map = {col.title: col.id for col in sheet.columns}

    # -----------------------------
    # 4. WIPE existing rows
    # -----------------------------
    existing_row_ids = [row.id for row in sheet.rows]

    if existing_row_ids:
        ss.Sheets.delete_rows(
            SHEET_ID,
            existing_row_ids,
            ignore_rows_not_found=True
        )
        print(f"Deleted {len(existing_row_ids)} existing rows")

    # -----------------------------
    # 5. Build new rows
    # -----------------------------
    new_rows = []

    for _, record in df.iterrows():
        cells = []

        for col_name in df.columns:
            col_id = col_map.get(col_name)
            if not col_id:
                continue

            val = record[col_name]
            if pd.isna(val):
                val = ""

            cells.append(
                smartsheet.models.Cell({
                    "column_id": col_id,
                    "value": val
                })
            )

        row = smartsheet.models.Row()
        row.to_top = True
        row.cells = cells
        new_rows.append(row)

    # -----------------------------
    # 6. Push rows in batches
    # -----------------------------
    BATCH_SIZE = 200
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i:i + BATCH_SIZE]
        ss.Sheets.add_rows(SHEET_ID, batch)
        print(f"Inserted rows {i+1} → {i+len(batch)}")

    print("✅ Assets data successfully refreshed in Smartsheet")

if __name__ == "__main__":
    push_assets_to_smartsheet()
