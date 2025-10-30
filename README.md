@update_router.post("/update-table")
def update_table(payload: TableUpdateRequest):
    logger.info("Updating table %s for market %s", payload.table, payload.market)

    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()

        obj = payload.obj

        set_clauses = []
        values = []

        for col, val in obj.items():
            set_clauses.append(f"{col} = %s")
            values.append(val)

        # always update timestamp
        set_clauses.append("updated_at = NOW()")

        set_query = ", ".join(set_clauses)

        sql_query = f"""
            UPDATE table_config
            SET {set_query}
            WHERE table_name = %s 
            AND data_source_id = 'hsbc-12010598-fdrasp-dev'
            RETURNING *;
        """

        values.append(payload.table)  # final param for WHERE

        with conn.cursor() as cur:
            cur.execute(sql_query, values)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        data = [dict(zip(colnames, row)) for row in rows]
        return {"updated_rows": data}

    except Exception as e:
        logger.exception(e)
        raise HTTPException(500, "Update failed")
