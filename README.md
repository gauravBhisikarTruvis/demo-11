#!/usr/bin/env python3
"""
excel_to_metadata_json.py

Reads an Excel sheet with columns:
- Tags
- Type of DB
- Dataset Name
- Table Name
- Sample Usecase

Produces a JSON list where each row becomes one object with the required schema.
Fields not present in the sheet are left empty.
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd


def to_list(value):
    """Split comma-separated string into a trimmed list. Empty -> []"""
    if pd.isna(value) or str(value).strip() == "":
        return []
    return [x.strip() for x in str(value).split(",") if x.strip()]


def nice_name_from_table(table_name: str) -> str:
    """Turn snake_case into Title Case for display_name."""
    if not table_name:
        return ""
    name = re.sub(r"[_\s]+", " ", str(table_name).strip())
    return name.title()


def row_to_schema(row: pd.Series) -> dict:
    # Pull what we can from the sheet
    tags_list = to_list(row.get("Tags"))
    dataset_name = "" if pd.isna(row.get("Dataset Name")) else str(row.get("Dataset Name")).strip()
    table_name = "" if pd.isna(row.get("Table Name")) else str(row.get("Table Name")).strip()
    sample_usecase = "" if pd.isna(row.get("Sample Usecase")) else str(row.get("Sample Usecase")).strip()

    # Map to your schema; leave unspecified fields empty
    item = {
        "data_source_id": "",                       # not in sheet -> keep empty
        "data_namespace": dataset_name,             # mapped from Dataset Name (or "")
        "table_name_details": table_name,           # mapped from Table Name
        "display_name": nice_name_from_table(table_name),
        "description": sample_usecase,              # put sample usecase text here; change if you prefer
        "columns_metadata": [],                     # no columns in sheet -> empty
        "filter_columns": [],
        "aggregate_columns": [],
        "sort_columns": [],
        "key_columns": [],
        "related_business_terms": [],
        "tags": tags_list,                          # split by commas from Tags
        "sample_usage": (
            [{"description": sample_usecase, "sql": ""}]
            if sample_usecase else []
        ),
        "join_tables": []
    }
    return item


def main():
    ap = argparse.ArgumentParser(description="Convert Excel metadata to JSON schema list.")
    ap.add_argument("excel_path", help="Path to the input .xlsx file")
    ap.add_argument("-s", "--sheet", default=0, help="Sheet name or index (default: first sheet)")
    ap.add_argument("-o", "--out", default="metadata.json", help="Output JSON path (default: metadata.json)")
    args = ap.parse_args()

    # Load Excel
    df = pd.read_excel(args.excel_path, sheet_name=args.sheet)

    # Normalize expected column names (strip surrounding spaces)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"Tags", "Type of DB", "Dataset Name", "Table Name", "Sample Usecase"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required column(s) in Excel: {missing}")

    # Drop completely empty rows (no table name and no tags and no usecase)
    df = df.dropna(how="all")

    result = []
    for _, row in df.iterrows():
        # Skip if even Table Name is empty
        if pd.isna(row.get("Table Name")) or str(row.get("Table Name")).strip() == "":
            continue
        result.append(row_to_schema(row))

    # Write JSON (pretty)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"Wrote {len(result)} item(s) to {out_path.resolve()}")


if __name__ == "__main__":
    main()
