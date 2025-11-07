@pg_router.post("/bulk-upload")
async def bulk_upload(payload: Metadata):
    conn = cur = None
    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn, cur = db_util.get_db_connection()

        # 0) Log where we are
        cur.execute("SELECT current_database(), current_user, current_schema, current_setting('search_path');")
        db, usr, schema, sp = cur.fetchone()
        logger.info("DB=%s USER=%s SCHEMA=%s SEARCH_PATH=%s", db, usr, schema, sp)

        # 1) Ensure pgvector exists (autocommit, outside txn)
        conn.autocommit = True
        cur.execute("SELECT name FROM pg_available_extensions WHERE name='vector';")
        if cur.fetchone() is None:
            raise RuntimeError("pgvector not available on this instance")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        logger.info("pgvector installed/ensured")
        conn.autocommit = False  # back to transactional

        # 2) Create schema explicitly (optional but good)
        exec_ddl = lambda sql: (cur.execute(sql), logger.info(cur.statusmessage))

        exec_ddl("CREATE SCHEMA IF NOT EXISTS public;")

        # 3) Create the 4 tables with schema-qualified names
        exec_ddl("""
        CREATE TABLE IF NOT EXISTS public.table_config (
            id_key VARCHAR(200) PRIMARY KEY,
            data_source_id VARCHAR(100) NOT NULL,
            table_name VARCHAR(100) NOT NULL
        );""")

        exec_ddl("""
        CREATE TABLE IF NOT EXISTS public.column_config (
            id_key VARCHAR(200) PRIMARY KEY,
            data_source_id VARCHAR(100) NOT NULL
        );""")

        exec_ddl("""
        CREATE TABLE IF NOT EXISTS public.table_context (
            id_key VARCHAR(255) PRIMARY KEY,
            embedding vector(1536),
            raw_text TEXT
        );""")

        exec_ddl("""
        CREATE TABLE IF NOT EXISTS public.column_context (
            id_key VARCHAR(255) PRIMARY KEY,
            embedding vector(1536),
            raw_text TEXT
        );""")

        conn.commit()

        # 4) Verify existence immediately (from same connection)
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('table_config','column_config','table_context','column_context')
            ORDER BY table_name;
        """)
        found = cur.fetchall()
        logger.info("Found tables: %s", found)

        return {"status": "ok", "found": found}

    except Exception as e:
        logger.exception("Bulk upload failed: %s", e)
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Bulk upload failed: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
