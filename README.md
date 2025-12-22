def parse_structured_context(self, raw_text):
    """
    Accepts raw context which may be:
    - dict metadata object
    - plain string
    - list of dicts
    - None
    
    Always returns standardized structure.
    """

    # CASE 1: missing / None
    if raw_text is None:
        return {}

    # CASE 2: this is already a list (vector store sometimes returns list of dicts)
    if isinstance(raw_text, list):
        results = []
        for item in raw_text:
            results.append(self.parse_structured_context(item))
        return results

    # CASE 3: string context → return safe wrapper
    if isinstance(raw_text, str):
        return {
            "table_name": None,
            "column_name": None,
            "description": raw_text,
            "data_namespace": None,
            "data_type": None,
            "structured": False
        }

    # CASE 4: dict context
    if isinstance(raw_text, dict):
        return {
            "table_name": raw_text.get("table_name"),
            "column_name": raw_text.get("column_name"),
            "description": raw_text.get("description"),
            "data_namespace": raw_text.get("data_namespace"),
            "data_type": raw_text.get("data_type"),
            "is_filterable": raw_text.get("is_filterable"),
            "is_aggregatable": raw_text.get("is_aggregatable"),
            "tags": raw_text.get("tags"),
            "structured": True
        }

    # CASE 5: unknown object → convert to string
    return {
        "table_name": None,
        "column_name": None,
        "description": str(raw_text),
        "data_namespace": None,
        "data_type": None,
        "structured": False
    }
