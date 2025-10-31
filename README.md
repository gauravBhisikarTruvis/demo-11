
@pg_router.post("/update-table-metadata")
def update_table(payload: TableUpdateRequest):
    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()

        if not payload.obj:
            raise HTTPException(status_code=400, detail="'obj' must contain at least one field")

        set_parts = []
        params: list[Any] = []

        for col, val in payload.obj.items():
            col_quoted = f'"{col}"'  # simple identifier quoting
            if isinstance(val, list):
                placeholders = ", ".join(["%s"] * len(val))
                set_parts.append(f"{col_quoted} = ARRAY[{placeholders}]")
                params.extend(val)
            else:
                set_parts.append(f"{col_quoted} = %s")
                params.append(val)

        # bump timestamp
        set_parts.append("updated_at = NOW()")

        sql_query = f"""
            UPDATE table_config
            SET {", ".join(set_parts)}
            WHERE table_name = %s
              AND data_source_id = %s
            RETURNING *;
        """

        params.extend([payload.table, FIXED_DS_ID])

        with conn, conn.cursor() as cur:
            cur.execute(sql_query, params)
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="No matching row to update")
            cols = [d[0] for d in cur.description]
            return {"updated_rows": [dict(zip(cols, r)) for r in rows]}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Update failed")
        raise HTTPException(status_code=500, detail="Update failed")
