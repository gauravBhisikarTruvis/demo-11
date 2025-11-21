def insert_table_context(record: dict, cur) -> None:
    """
    Upsert a record into table_context (id_key, embedding, raw_text).
    Expects a psycopg2 cursor `cur` that is already connected.
    Caller is responsible for commit/rollback and closing connection.
    """
    if cur is None:
        raise RuntimeError("Cursor (cur) is required and must be a live DB cursor.")

    # basic fields
    table_name_val = record.get("table_name") or record.get("table_name_details") or ""
    data_source = record.get("data_source_id") or ""
    data_ns = record.get("data_namespace", "")

    # id key (same pattern as table/column id keys you use elsewhere)
    id_key = f"{data_source}~{data_ns}~{table_name_val}"

    # Helper to safely join lists (returns string)
    def join_list(maybe_list):
        if maybe_list is None:
            return ""
        if isinstance(maybe_list, (list, tuple)):
            return ", ".join(str(x) for x in maybe_list)
        # fallback if it's a single string or other type
        return str(maybe_list)

    # Build raw_text as plain strings joined by newline
    raw_parts = [
        f"table: {table_name_val}",
        f"display_name: {record.get('display_name','')}",
        f"description: {record.get('description','')}",
        f"tags: {join_list(record.get('tags', []))}",
        f"filter_columns: {join_list(record.get('filter_columns', []))}",
        f"aggregate_columns: {join_list(record.get('aggregate_columns', []))}",
        f"sort_columns: {join_list(record.get('sort_columns', []))}",
        f"key_columns: {join_list(record.get('key_columns', []))}",
        f"join_tables: {join_list(record.get('join_tables', []))}",
        f"related_business_terms: {join_list(record.get('related_business_terms', []))}"
    ]
    # ensure everything is string and no nested lists remain
    raw_parts = [str(p) for p in raw_parts]
    raw_text = "\n".join(raw_parts)

    # include sample_usage JSON string (if present) appended so embedding has examples too
    try:
        sample_usage_str = json.dumps(record.get("sample_usage", []), ensure_ascii=False)
    except Exception:
        sample_usage_str = str(record.get("sample_usage", ""))
    if sample_usage_str:
        raw_text = raw_text + "\n" + f"sample_usage: {sample_usage_str}"

    # generate embedding (use your VertexAIEmbedding wrapper)
    try:
        gemini_em = VertexAIEmbedding("hsbc-12432649-c48nlpuk-dev")   # use market key you use elsewhere
        embedding_raw = gemini_em.create_gemini_embeddings(raw_text)
        # normalize to plain Python floats list
        embedding = [float(x) for x in embedding_raw]
    except Exception as e:
        raise Exception(f"Failed to generate embedding: {e}")

    # Upsert into table_context (parameterized)
    upsert_sql = f"""
    INSERT INTO public.table_context (id_key, embedding, raw_text)
    VALUES (%s, %s, %s)
    ON CONFLICT (id_key) DO UPDATE
      SET embedding = EXCLUDED.embedding,
          raw_text = EXCLUDED.raw_text;
    """

    try:
        cur.execute(upsert_sql, (id_key, embedding, raw_text))
        # do NOT commit here; caller should commit/rollback so batch work can be atomic if required
    except Exception as e:
        # raise with a helpful message for debugging
        raise Exception(f"[insert_table_context] failed to execute upsert for id_key={id_key}: {e}")
