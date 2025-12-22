def parse_structured_context(self, raw_text: dict) -> dict:
    """
    Normalizes table/column metadata from vector store context chunks.
    Accepts context: raw dict created during vector indexing.

    Expected raw format example:
    {
        "table_name": "...",
        "column_name": "...",
        "description": "...",
        "data_namespace": "...",
        "data_type": "...",
        "is_filterable": true,
        ...
    }

    We standardize keys, return safe defaults when missing.
    """
    
    if not raw_text:
        return {}

    return {
        "table_name": raw_text.get("table_name"),
        "column_name": raw_text.get("column_name"),
        "description": raw_text.get("description"),
        "data_namespace": raw_text.get("data_namespace"),
        "data_type": raw_text.get("data_type"),
        "is_filterable": raw_text.get("is_filterable"),
        "is_aggregatable": raw_text.get("is_aggregatable"),
        "sample_usage": raw_text.get("sample_usage", []),
        "tags": raw_text.get("tags", []),
        "related_business_terms": raw_text.get("related_business_terms", []),
        "join_tables": raw_text.get("join_tables", []),
        "key_columns": raw_text.get("key_columns", []),
        "sort_columns": raw_text.get("sort_columns", []),
        "aggregate_columns": raw_text.get("aggregate_columns", []),
        "display_name": raw_text.get("display_name"),
        "category": raw_text.get("category"),
        "columns": raw_text.get("columns", []),
        "filter_columns": raw_text.get("filter_columns", [])
    }
