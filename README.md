def update_column_metadata(payload: ColumnUpdateRequest):
    data_source = payload.market
    id_key = payload.id_key
    column_name = payload.column
    if not payload.obj:
        raise HTTPException(status_code=400, detail="'obj' must contain at least one field")
    db_util = GoogleCloudSqlUtility(data_source)
    conn = None
    try:
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'column_config';
            """
        )
        col_info = {row[0]: {"data_type": row[1], "udt_name": row[2]} for row in cur.fetchall()}
        set_parts = []
        params = []
        for col, val in payload.obj.items():
            col_quoted = f'"{col}"'
            info = col_info.get(col, {})
            udt = info.get("udt_name", "").lower()
            dtype = info.get("data_type", "").lower()
            if udt in ("jsonb", "json") or dtype in ("json", "jsonb"):
                set_parts.append(f"{col_quoted} = %s::jsonb")
                params.append(json.dumps(val, ensure_ascii=False))
            elif udt.startswith("_"):
                elem_type = udt[1:]
                set_parts.append(f"{col_quoted} = %s::{elem_type}[]")
                params.append(val)
            else:
                set_parts.append(f"{col_quoted} = %s")
                params.append(val)
        set_parts.append("updated_at = NOW()")
        sql_query = f"""
UPDATE public.column_config
SET {', '.join(set_parts)}
WHERE id_key = %s AND column_name = %s
RETURNING *;
"""
        params.append(id_key)
        params.append(column_name)
        cur.execute(sql_query, params)
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No matching row to update")
        cols = [d[0] for d in cur.description]
        updated_rows = [dict(zip(cols, r)) for r in rows]
        conn.commit()
        raw_parts = []
        for k, v in payload.obj.items():
            if isinstance(v, (list, dict)):
                raw_parts.append(f"{k} = '{json.dumps(v, ensure_ascii=False)}'")
            else:
                raw_parts.append(f"{k} = {v!r}")
        raw_text = "\n".join(raw_parts)
        gemini_em = VertexAIEmbedding()
        embedding = gemini_em.create_gemini_embeddings(raw_text)
        embedding = [float(x) for x in embedding]
        emb_literal = "[" + ",".join(map(str, embedding)) + "]"
        vector_sql = f"""
INSERT INTO public.column_context (id_key, column_name, embedding, raw_text)
VALUES (%s, %s, %s::vector, %s)
ON CONFLICT (id_key, column_name) DO UPDATE
SET embedding = EXCLUDED.embedding,
    raw_text = EXCLUDED.raw_text;
"""
        cur.execute(vector_sql, (id_key, column_name, emb_literal, raw_text))
        conn.commit()
        return {"message": "column update complete", "updated_rows": updated_rows}
    except HTTPException:
        raise
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
