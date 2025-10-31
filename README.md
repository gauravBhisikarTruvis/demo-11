# excel_to_metadata_json_simple.py
# --------------------------------
# Requirements:
#   pip install pandas openpyxl

from pathlib import Path
import json
import re
import pandas as pd

# --------- EDIT THESE -------------
INPUT_XLSX   = r"DataDictionary_UK.xlsx"  
SHEET        = 0                        
OUTPUT_JSON  = r"metadata.json"          
# -----------------------------------

FIXED_DATA_SOURCE_ID = "hsbc"  # always "hsbc"

EXPECTED_COLS = ["Tags", "Dataset Name", "Table Name", "Sample Usecase"]


def to_list(value):
    if pd.isna(value):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def title_from_table(table_name: str) -> str:
    if not table_name:
        return ""
    return re.sub(r"[_\s]+", " ", str(table_name)).strip().title()


def row_to_item(row: pd.Series) -> dict:
    tags_list     = to_list(row.get("Tags"))
    dataset       = "" if pd.isna(row.get("Dataset Name")) else str(row.get("Dataset Name")).strip()
    table_name    = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
    sample_sql    = "" if pd.isna(row.get("Sample Usecase")) else str(row.get("Sample Usecase")).strip()

    return {
        "data_source_id": FIXED_DATA_SOURCE_ID,
        "data_namespace": dataset,
        "table_name_details": table_name,
        "display_name": title_from_table(table_name),
        
        # ✅ Always empty description
        "description": "",

        "columns_metadata": [],
        "filter_columns": [],
        "aggregate_columns": [],
        "sort_columns": [],
        "key_columns": [],
        "related_business_terms": [],
        "tags": tags_list,

        # ✅ Put sample usecase as a sample query, not description
        "sample_usage": (
            [{"description": "", "sql": sample_sql}] if sample_sql else []
        ),

        "join_tables": []
    }


def main():
    if not Path(INPUT_XLSX).exists():
        raise SystemExit(f"❌ Input Excel not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, sheet_name=SHEET)
    df.columns = [str(c).strip() for c in df.columns]

    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = ""

    result = []
    for _, row in df.iterrows():
        table_name = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
        if not table_name:
            continue

        result.append(row_to_item(row))

    Path(OUTPUT_JSON).write_text(
        json.dumps(result, indent=2, ensure_ascii=False)
    )

    print(f"✅ JSON generated with {len(result)} tables → {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
