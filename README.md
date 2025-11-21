def insert_table_context(record: Dict[str, Any]) -> None:
    """
    Build a readable raw_text for the table record, compute embedding,
    and upsert into table_context (id_key, embedding, raw_text).

    Raises RuntimeError on DB connection problems; raises Exception on SQL/embedding failures.
    """
    # required fields / safe lookups
    table_name_val = record.get("table_name") or record.get("table_name_details") or ""
    data_source = record.get("data_source_id", "")
    data_ns = record.get("data_namespace", "")

    id_key = f"{data_source}~{data_ns}~{table_name_val}"

    # Build readable raw_text — ensure all parts are strings before join
    parts = []
    parts.append(f"table: {table_name_val}")
    parts.append(f"display_name: {record.get('display_name','')}")
    parts.append(f"description: {record.get('description','')}")
    parts.append(f"tags: { _safe_join_list_field(record.get('tags', [])) }")
    parts.append(f"filter_columns: { _safe_join_list_field(record.get('filter_columns', [])) }")
    parts.append(f"aggregate_columns: { _safe_join_list_field(record.get('aggregate_columns', [])) }")
    parts.append(f"sort_columns: { _safe_join_list_field(record.get('sort_columns', [])) }")
    parts.append(f"key_columns: { _safe_join_list_field(record.get('key_columns', [])) }")
    parts.append(f"related_business_terms: { _safe_join_list_field(record.get('related_business_terms', [])) }")

    # sample usage might be a list of objects -> json-dump it
    try:
        sample_usage_str = json.dumps(record.get("sample_usage", []), ensure_ascii=False)
    except Exception:
        sample_usage_str = str(record.get("sample_usage", ""))

    parts.append(f"sample_usage: {sample_usage_str}")

    raw_text = "\n".join(part for part in parts if part is not None)

    # --- embedding ---
    # Replace VertexAIEmbedding with your actual embedding wrapper
    gemini_em = VertexAIEmbedding(MARKET)   # adjust constructor as you have it
    embedding_raw = gemini_em.create_gemini_embeddings(raw_text)

    # Ensure embedding is an iterable of numbers and convert to floats
    try:
        embedding = [float(x) for x in embedding_raw]
    except Exception as e:
        raise Exception(f"Embedding returned non-numeric values: {e}")

    # Upsert into DB
    db_util = GoogleCloudSqlUtility(MARKET)   # your DB util
    conn = db_util.get_db_connection()
    if conn is None:
        raise RuntimeError("DB connection is None (check credentials/market).")

    cur = None
    try:
        cur = conn.cursor()
        upsert_sql = """
        INSERT INTO public.table_context (id_key, embedding, raw_text)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_key) DO UPDATE
          SET embedding = EXCLUDED.embedding,
              raw_text  = EXCLUDED.raw_text;
        """
        cur.execute(upsert_sql, (id_key, embedding, raw_text))
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"[insert_table_context] failed: {e}")
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
