@update_router.post("/update-column")
def update_column(payload: ColumnConfigUpdateRequest):
    table_name_value = payload.table       # value to match in column_config.table_name
    column_name_value = payload.column     # value to match in column_config.column_name
    update_obj = payload.obj

    if not update_obj:
        raise HTTPException(400, "obj must have at least one field")

    # Build SET clause & params dynamically
    set_parts = []
    params = []

    for col, val in update_obj.items():
        set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
        params.append(val)

    # auto update timestamp
    set_parts.append(sql.SQL("updated_at = NOW()"))

    # WHERE table_name & column_name
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
            cur.execute(query, params)
            rows = cur.fetchall()

            if not rows:
                raise HTTPException(404, "No row found for given table & column")

            colnames = [desc[0] for desc in cur.description]
            updated = [dict(zip(colnames, row)) for row in rows]

        return {"updated": updated}

    except Exception as e:
        logger.exception(f"Update failed: {e}")
        raise HTTPException(500, "Update failed")
