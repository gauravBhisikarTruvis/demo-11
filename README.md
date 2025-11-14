# Column Config (main table)
id_key_cfg = f"{data_source}~{data_namespace}~{table}~{column}"

# Column Context (embedding table)
id_key_ctx = f"{data_source}.{data_namespace}.{table}.{column}"



@pg_router.post("/update-column-metadata")
def update_column_metadata(payload: ColumnMetadataUpdateRequest):
    data_source   = payload.market
    data_namespace = payload.obj.get("data_namespace", "")
    table         = payload.table
    column        = payload.column

    # -------------------------------
    # Build BOTH id_keys (important!)
    # -------------------------------
    # column_config → uses '~'
    id_key_cfg = f"{data_source}~{data_namespace}~{table}~{column}"

    # column_context → uses '.'
    id_key_ctx = f"{data_source}.{data_namespace}.{table}.{column}"

    db_util = GoogleCloudSqlUtility(data_source)
    conn = None

    try:
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection failed")

        # -------------------------------------------------------------
        # ----------- BUILD UPDATE QUERY FOR column_config ------------
        # -------------------------------------------------------------
        set_parts = []
        params = []

        for col, val in payload.obj.items():
            col_quoted = f"\"{col}\""

            if isinstance(val, list):
                if len(val) == 0:
                    set_parts.append(f"{col_quoted} = %s")
                    params.append([])
                else:
                    placeholders = ", ".join(["%s"] * len(val))
                    set_parts.append(f"{col_quoted} = ARRAY[{placeholders}]")
                    params.extend(val)

            elif isinstance(val, dict):
                set_parts.append(f"{col_quoted} = %s::jsonb")
                params.append(json.dumps(val, ensure_ascii=False))

            else:
                set_parts.append(f"{col_quoted} = %s")
                params.append(val)

        set_parts.append("updated_at = NOW()")
        params.append(id_key_cfg)

        sql_cfg = f"""
            UPDATE public.column_config
            SET {", ".join(set_parts)}
            WHERE id_key = %s
            RETURNING *;
        """

        print("\n--- COLUMN CONFIG UPDATE SQL ---")
        print(sql_cfg)
        print("PARAMS:", params)

        cur = conn.cursor()
        cur.execute(sql_cfg, params)
        rows = cur.fetchall()

        if not rows:
            conn.rollback()
            raise HTTPException(status_code=404, detail="No matching column_config row")

        cols = [d[0] for d in cur.description]
        updated_rows = [dict(zip(cols, r)) for r in rows]

        # -------------------------------------------------------------
        # ----------- BUILD raw_text AND EMBEDDINGS -------------------
        # -------------------------------------------------------------
        raw_parts = []
        for k, v in payload.obj.items():
            if isinstance(v, (list, dict)):
                raw_parts.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")
            else:
                raw_parts.append(f"{k} = {v}")

        raw_text = "\n".join(raw_parts)

        gemini_em = PostgresVectorLoader()
        embedding = gemini_em.get_gemini_embedding(raw_text)
        embedding = [float(x) for x in embedding]
        emb_literal = "[" + ",".join(map(str, embedding)) + "]"

        # -------------------------------------------------------------
        # ----------- UPSERT INTO column_context -----------------------
        # -------------------------------------------------------------
        vector_sql = """
            INSERT INTO public.column_context (id_key, embedding, raw_text)
            VALUES (%s, %s::vector, %s)
            ON CONFLICT (id_key) DO UPDATE
            SET embedding = EXCLUDED.embedding,
                raw_text = EXCLUDED.raw_text;
        """

        print("\n--- COLUMN CONTEXT UPSERT ---")
        print(vector_sql)
        print("CTX-ID-KEY:", id_key_ctx)

        cur2 = conn.cursor()
        cur2.execute(vector_sql, (id_key_ctx, emb_literal, raw_text))

        conn.commit()

        return {
            "message": "Column metadata + context updated",
            "id_key_config": id_key_cfg,
            "id_key_context": id_key_ctx,
            "updated_rows": updated_rows
        }

    except HTTPException:
        raise

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
