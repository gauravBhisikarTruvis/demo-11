# excel_to_json_sample_queries.py
# pip install pandas openpyxl

from pathlib import Path
import json
import re
import pandas as pd

# ---- EDIT THESE PATHS ----
INPUT_XLSX  = r"DataDictionary_UK.xlsx"   # your Excel file
SHEET       = 0                           # sheet index or name
OUTPUT_JSON = r"metadata.json"            # output JSON file
# --------------------------

FIXED_DATA_SOURCE_ID = "hsbc"  # always hsbc

# expected column headers in Excel (case-insensitive match after strip)
REQUIRED_COLS = ["Tags", "Dataset Name", "Table Name", "Sample Usecase"]

def as_list_from_commas(value: str):
    if pd.isna(value):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def title_from_table(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"[_\s]+", " ", str(name)).strip().title()

def split_sample_usecase_to_queries(text: str):
    """
    Convert the 'Sample Usecase' cell into a list of SQL strings.

    Supports formats like:
      "Query 1", "Query 2", "Query 3"
    OR  Query 1; Query 2; Query 3
    OR  a single query string
    """
    if pd.isna(text):
        return []

    s = str(text).strip()
    if not s:
        return []

    # Case 1: looks like a quoted, comma-separated list -> split on " , "
    if re.search(r'^"\s*.+\s*"', s) and '",' in s:
        # remove leading/trailing quotes if the whole cell is quoted
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]
        parts = re.split(r'"\s*,\s*"', s)
    else:
        # Case 2: split by semicolons if present, else treat as single
        if ";" in s:
            parts = [p.strip() for p in s.split(";")]
        else:
            parts = [s]

    # Clean each part: strip any stray surrounding quotes
    cleaned = []
    for p in parts:
        q = p.strip()
        if q.startswith('"') and q.endswith('"') and len(q) >= 2:
            q = q[1:-1].strip()
        if q:
            cleaned.append(q)

    return cleaned

def row_to_item(row: pd.Series) -> dict:
    tags           = as_list_from_commas(row.get("Tags"))
    data_namespace = "" if pd.isna(row.get("Dataset Name")) else str(row.get("Dataset Name")).strip()
    table_name     = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
    sample_text    = "" if pd.isna(row.get("Sample Usecase")) else str(row.get("Sample Usecase")).strip()

    # Build sample_usage from sample queries; description must be empty string
    queries = split_sample_usecase_to_queries(sample_text)
    sample_usage = [{"description": "", "sql": q} for q in queries]

    return {
        "data_source_id": FIXED_DATA_SOURCE_ID,
        "data_namespace": data_namespace or "",
        "table_name_details": table_name or "",
        "display_name": title_from_table(table_name),
        "description": "",                         # <-- always empty
        "columns_metadata": [],
        "filter_columns": [],
        "aggregate_columns": [],
        "sort_columns": [],
        "key_columns": [],
        "related_business_terms": [],
        "tags": tags,
        "sample_usage": sample_usage,              # <-- list of {description:"", sql:"..."}
        "join_tables": []
    }

def main():
    xlsx_path = Path(INPUT_XLSX)
    if not xlsx_path.exists():
        raise SystemExit(f"Input Excel not found: {xlsx_path}")

    df = pd.read_excel(xlsx_path, sheet_name=SHEET)
    # normalize headers (strip + case-insensitive matching via dict)
    df.columns = [str(c).strip() for c in df.columns]

    # Add missing required columns as empty
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = ""

    items = []
    for _, row in df.iterrows():
        table_name = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
        if not table_name:
            continue  # skip empty rows
        items.append(row_to_item(row))

    Path(OUTPUT_JSON).write_text(json.dumps(items, indent=2, ensure_ascii=False))
    print(f"✅ Wrote {len(items)} records → {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
