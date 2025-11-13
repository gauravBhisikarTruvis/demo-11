def insert_column_context(record: dict, table: str = "column_context") -> None:
    # Same id_key as above so upsert lines up
    table_name  = record.get("table_name") or record.get("table_name_details")
    column_name = record.get("column_name") or record.get("column_name_details")
    data_source = record.get("data_source_id")
    data_ns     = record.get("data_namespace", "")
    id_key      = f"{data_source}~{data_ns}~{table_name}~{column_name}"

    # raw text to embed – build something readable from the record
    raw_text = (
        f"table={table_name}, column={column_name}, type={record.get('data_type','')}, "
        f"desc={record.get('description','')}, filterable={record.get('is_filterable', False)}, "
        f"aggregatable={record.get('is_aggregatable', False)}"
    )

    embedding = get_openai_embedding(raw_text)  # your existing function

    upsert_sql = f"""
        INSERT INTO public.{table} (id_key, embedding, raw_text)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_key) DO UPDATE SET
            embedding = EXCLUDED.embedding,
            raw_text  = EXCLUDED.raw_text;
    """

    conn = None
    try:
        db_util = GoogleCloudSqlUtility(MARKET)
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None (check market/credentials).")
        cur = conn.cursor()
        cur.execute(upsert_sql, (id_key, embedding, raw_text))
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"[insert_column_context] failed: {e}")
    finally:
        if conn:
            conn.close()
