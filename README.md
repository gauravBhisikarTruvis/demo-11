# excel_to_metadata_json_simple.py
# --------------------------------
# Requirements:
#   pip install pandas openpyxl

from pathlib import Path
import json
import re
import pandas as pd

# --------- EDIT THESE -------------
INPUT_XLSX   = r"DataDictionary_UK.xlsx"  # path to your Excel
SHEET        = 0                          # sheet index or name (e.g., "Sheet1")
OUTPUT_JSON  = r"metadata.json"           # output JSON file
# -----------------------------------

FIXED_DATA_SOURCE_ID = "hsbc"  # always "hsbc" as requested

EXPECTED_COLS = ["Tags", "Dataset Name", "Table Name", "Sample Usecase"]


def to_list(value):
    """Convert comma-separated cell to list; empty -> []"""
    if pd.isna(value):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def title_from_table(table_name: str) -> str:
    """Convert snake_case table name to Title Case"""
    if not table_name:
        return ""
    return re.sub(r"[_\s]+", " ", str(table_name)).strip().title()


def row_to_item(row: pd.Series) -> dict:
    tags_list     = to_list(row.get("Tags"))
    dataset       = "" if pd.isna(row.get("Dataset Name")) else str(row.get("Dataset Name")).strip()
    table_name    = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
    usecase_text  = "" if pd.isna(row.get("Sample Usecase")) else str(row.get("Sample Usecase")).strip()

    return {
        "data_source_id": FIXED_DATA_SOURCE_ID,
        "data_namespace": dataset,
        "table_name_details": table_name,
        "display_name": title_from_table(table_name),
        "description": usecase_text or "",

        "columns_metadata": [],
        "filter_columns": [],
        "aggregate_columns": [],
        "sort_columns": [],
        "key_columns": [],
        "related_business_terms": [],
        "tags": tags_list,
        "sample_usage": (
            [{"description": usecase_text, "sql": ""}] if usecase_text else []
        ),
        "join_tables": []
    }


def main():
    xlsx_path = Path(INPUT_XLSX)
    if not xlsx_path.exists():
        raise SystemExit(f"Input file not found: {xlsx_path}")

    df = pd.read_excel(xlsx_path, sheet_name=SHEET)
    df.columns = [str(c).strip() for c in df.columns]

    # Ensure expected columns exist
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = ""

    result = []
    for _, row in df.iterrows():
        table_name = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
        if not table_name:
            continue  # skip blank table rows

        result.append(row_to_item(row))

    out_path = Path(OUTPUT_JSON)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"✅ Generated {len(result)} records → {out_path.resolve()}")


if __name__ == "__main__":
    main()
