
@pg_router.post("/update-column-metadata")
def update_column(payload: ColumnUpdateRequest):
    table_name_value = payload.table
    column_name_value = payload.column
    update_obj = payload.obj or {}

    if not update_obj:
        raise HTTPException(400, "'obj' must have at least one field")

    set_parts = []
    params: list[Any] = []

    # Build SET column logic
    for col, val in update_obj.items():
        ident = sql.Identifier(col)

        if isinstance(val, list):
            # list → ARRAY[]
            placeholders = sql.SQL(", ").join(sql.SQL("%s") for _ in val)
            set_parts.append(sql.SQL("{} = ARRAY[").format(ident) + placeholders + sql.SQL("]"))
            params.extend(val)
        else:
            set_parts.append(sql.SQL("{} = %s").format(ident))
            params.append(val)

    # Always update timestamp
    set_parts.append(sql.SQL("updated_at = NOW()"))

    # WHERE table_name + column_name
    where_clause = sql.SQL("table_name = %s AND column_name = %s")
    params.extend([table_name_value, column_name_value])

    query = sql.SQL("""
        UPDATE column_config
        SET {set_clause}
        WHERE {where_clause}
        RETURNING *;
    """).format(
        set_clause=sql.SQL(", ").join(set_parts),
        where_clause=where_clause
    )

    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()

        with conn, conn.cursor() as cur:
            # Debug: print real SQL executed
            try:
                rendered = cur.mogrify(query.as_string(conn), params).decode()
                logger.info("Rendered SQL:\n%s", rendered)
            except Exception:
                pass

            cur.execute(query, params)
            rows = cur.fetchall()

            if not rows:
                raise HTTPException(404, "No row found for given table & column")

            colnames = [d[0] for d in cur.description]
            updated = [dict(zip(colnames, r)) for r in rows]

        return {"updated": updated}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Update failed: %s", e)
        raise HTTPException(500, "Update failed")
