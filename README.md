def insert_table_metadata(record: dict):
    import json
    from database.db_connector import GoogleCloudSqlUtility

    # 1) derive keys/values
    table_name_val = record.get("table_name") or record.get("table_name_details")
    if not table_name_val:
        raise ValueError("table_name/table_name_details is required")

    id_key = f'{record["data_source_id"]}~{record.get("data_namespace","")}~{table_name_val}'

    # arrays (TEXT[])
    filter_columns         = record.get("filter_columns", [])
    aggregate_columns      = record.get("aggregate_columns", [])
    sort_columns           = record.get("sort_columns", [])
    key_columns            = record.get("key_columns", [])
    related_business_terms = record.get("related_business_terms", [])
    tags                   = record.get("tags", [])

    # JSONB
    sample_usage = json.dumps(record.get("sample_usage", []))
    join_tables  = json.dumps(record.get("join_tables", []))

    # 2) insert / upsert
    sql_insert = """
        INSERT INTO public.table_config (
            id_key, data_source_id, table_name, display_name, data_namespace, description,
            filter_columns, aggregate_columns, sort_columns, key_columns,
            related_business_terms, sample_usage, tags, join_tables
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s::jsonb, %s, %s::jsonb
        )
        ON CONFLICT (id_key) DO UPDATE SET
            data_source_id = EXCLUDED.data_source_id,
            table_name     = EXCLUDED.table_name,
            display_name   = EXCLUDED.display_name,
            data_namespace = EXCLUDED.data_namespace,
            description    = EXCLUDED.description,
            filter_columns = EXCLUDED.filter_columns,
            aggregate_columns = EXCLUDED.aggregate_columns,
            sort_columns   = EXCLUDED.sort_columns,
            key_columns    = EXCLUDED.key_columns,
            related_business_terms = EXCLUDED.related_business_terms,
            sample_usage   = EXCLUDED.sample_usage,
            tags           = EXCLUDED.tags,
            join_tables    = EXCLUDED.join_tables;
    """

    conn = None
    try:
        db_util = GoogleCloudSqlUtility(<your_market_or_env_here>)  # same as elsewhere
        conn = db_util.get_db_connection()
        cur = conn.cursor()
        cur.execute(
            sql_insert,
            (
                id_key, record["data_source_id"], table_name_val, record.get("display_name",""),
                record.get("data_namespace",""), record.get("description",""),
                filter_columns, aggregate_columns, sort_columns, key_columns,
                related_business_terms, sample_usage, tags, join_tables
            )
        )
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()
