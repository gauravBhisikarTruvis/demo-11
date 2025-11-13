def insert_table_context(self, record: dict, table: str = "table_context") -> None:
    """
    Insert / upsert a row into table_context with an embedding and raw_text.
    record: single table record (dict) coming from your JSON.
    """
    # --- build id and fields ---
    table_name_val = record.get("table_name") or record.get("table_name_details") or ""
    data_source = record.get("data_source_id", "")
    data_ns = record.get("data_namespace", "")

    # id_key should be unique per table row (same pattern you used for table_config)
    id_key = f"{data_source}~{data_ns}~{table_name_val}"

    # Build a readable raw_text for embedding (combine useful fields)
    raw_parts = [
        f"table: {table_name_val}",
        f"display_name: {record.get('display_name','')}",
        f"description: {record.get('description','')}",
        f"tags: {', '.join(record.get('tags',[]) if isinstance(record.get('tags',[]), list) else [])}",
        f"filter_columns: {', '.join(record.get('filter_columns',[]) if isinstance(record.get('filter_columns',[]), list) else [])}",
        f"aggregate_columns: {', '.join(record.get('aggregate_columns',[]) if isinstance(record.get('aggregate_columns',[]), list) else [])}",
    ]
    # Add sample_usage as JSON strings if present
    try:
        sample_usage_str = json.dumps(record.get("sample_usage", []), ensure_ascii=False)
    except Exception:
        sample_usage_str = str(record.get("sample_usage", []))
    raw_parts.append(f"sample_usage: {sample_usage_str}")

    raw_text = "\n".join([p for p in raw_parts if p])

    # --- get embedding (using your existing PostgresVectorLoader wrapper) ---
    gemini_em = PostgresVectorLoader()                # match your existing class name
    embedding = gemini_em.get_gemini_embedding(raw_text)

    # --- upsert SQL (id_key, embedding, raw_text) ---
    upsert_sql = f"""
    INSERT INTO public.{table} (id_key, embedding, raw_text)
    VALUES (%s, %s, %s)
    ON CONFLICT (id_key) DO UPDATE
      SET embedding = EXCLUDED.embedding,
          raw_text  = EXCLUDED.raw_text;
    """

    # --- DB work (same pattern as your other helpers) ---
    conn = None
    try:
        db_util = GoogleCloudSqlUtility(MARKET)   # use MARKET or the same market string you use elsewhere
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None (check credentials/market).")

        cur = conn.cursor()
        cur.execute(upsert_sql, (id_key, embedding, raw_text))
        conn.commit()
        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"[insert_table_context] failed: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
