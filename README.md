import json

# ... (previous logic to get tables_column_mapping) ...

# 1. SETUP: Define Project Prefix & Construct Names
PROJECT_PREFIX = "hsbc-12010598-fdrasp-dev" 
qualified_tables = []

for table_key in tables_column_mapping.keys():
    # Defensive check: Only add prefix if it's missing
    if not table_key.startswith(PROJECT_PREFIX):
        full_name = f"{PROJECT_PREFIX}.{table_key}"
    else:
        full_name = table_key
    qualified_tables.append(full_name)

# 2. BUILD QUERY
formatted_table_list = ", ".join([f"'{t}'" for t in qualified_tables])

meta_query = f"""
    SELECT table_name, metadata, row_count, size_bytes
    FROM `{metadata_dataset}.{metadata_table_name}`
    WHERE table_name IN ({formatted_table_list})
"""

logger.info(f"Executing Meta Query: {meta_query}")

# 3. EXECUTE QUERY & LOOP THROUGH RESULTS
try:
    # Use the client stored in your state
    query_job = state['bq_client'].query(meta_query)
    results = query_job.result()
    
    llm_context_list = []

    for row in results:
        # Parse the 'metadata' string column into a real Python dictionary
        # This prevents "double-string-encoding" which confuses LLMs
        try:
            # The DB column 'metadata' is a string like '{"schema": ...}'
            schema_info = json.loads(row.metadata)
        except (json.JSONDecodeError, TypeError):
            # Fallback: keep as string if parsing fails
            schema_info = row.metadata

        # Build the clean dictionary for this table
        table_context = {
            "table_name": row.table_name,
            "row_count": row.row_count,
            "size_bytes": row.size_bytes,
            "schema_info": schema_info 
        }
        llm_context_list.append(table_context)

    # 4. FINAL JSON OUTPUT
    # This string is what you pass to the LLM
    final_json_context = json.dumps(llm_context_list, indent=2)
    
    # Store in state for the next node
    state['table_metadata_context'] = final_json_context
    print("Final JSON Context:\n", final_json_context)

except Exception as e:
    logger.error(f"Failed to query bq_metadata: {str(e)}")
    # Handle error or set empty context
    state['table_metadata_context'] = "[]"
