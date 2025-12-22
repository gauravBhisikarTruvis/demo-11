def extract_tables_from_rag(self, context_data):
    # .get() defaults to an empty list if 'structured_context' is missing
    structured_list = context_data.get('structured_context', []) or []
    
    tables = set()
    for item in structured_list:
        # Check if it's a table type
        if item.get('context_type') == 'table':
            kv = item.get('key_value_context', {})
            # Extract the table name; if not found, it stays None
            name = kv.get('table')
            if name:
                tables.add(name)
    
    # If set is empty, join returns an empty string ""
    return ", ".join(sorted(tables))

def extract_columns_from_rag(self, context_data):
    structured_list = context_data.get('structured_context', []) or []
    
    columns = []
    for item in structured_list:
        # Check if it's a column type
        if item.get('context_type') == 'column':
            kv = item.get('key_value_context', {})
            # In your logs, columns sometimes appear in 'column' or 'context'
            name = kv.get('column') or item.get('context')
            if name and name.strip():
                columns.append(name.strip())
    
    # Deduplicate while keeping order
    unique_cols = list(dict.fromkeys(columns))
    
    # Returns empty string if list is empty
    return ", ".join(unique_cols)
