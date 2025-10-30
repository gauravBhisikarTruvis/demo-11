
@pg_router.post("/update-table-metadata")
def update_table(payload: TableUpdateRequest):
    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()

        if not payload.obj:
            raise HTTPException(status_code=400, detail="'obj' must contain at least one field")

        set_parts = []
        params = []

        for col, val in payload.obj.items():
            ident = sql.Identifier(col)

            if isinstance(val, list):
                # treat Python lists as text[] → ARRAY[%s, %s, ...]
                ph = sql.SQL(", ").join(sql.SQL("%s") for _ in val)
                set_parts.append(sql.SQL("{} = ARRAY[").format(ident) + ph + sql.SQL("]"))
                params.extend(val)
            else:
                # scalars (text/boolean/number)
                set_parts.append(sql.SQL("{} = %s").format(ident))
                params.append(val)

        # bump timestamp
        set_parts.append(sql.SQL("updated_at = NOW()"))

        query = sql.SQL("""
            UPDATE table_config
            SET {set_clause}
            WHERE table_name = %s
              AND data_source_id = %s
            RETURNING *;
        """).format(
            set_clause=sql.SQL(", ").join(set_parts)
        )

        params.extend([payload.table, FIXED_DS_ID])

        with conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="No matching row to update")
            cols = [d[0] for d in cur.description]
            result = [dict(zip(cols, r)) for r in rows]

        return {"updated_rows": result}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Update failed")
        raise HTTPException(status_code=500, detail="Update failed")
