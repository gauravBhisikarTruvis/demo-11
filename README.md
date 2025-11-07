@pg_router.post("/bulk-upload")
async def bulk_upload(payload: Metadata):
    conn = cur = None
    try:
        logger.info("DB connection started")
        db_util = GoogleCloudSqlUtility(payload.market)

        # same pattern as your working API
        conn = db_util.get_db_connection()
        cur = conn.cursor()

        # log where we actually are (helps catch “wrong DB/schema/replica”)
        cur.execute("SELECT current_database(), current_user, current_schema, current_setting('search_path');")
        db, usr, schema, sp = cur.fetchone()
        logger.info("DB=%s | USER=%s | SCHEMA=%s | SEARCH_PATH=%s", db, usr, schema, sp)

        # --- Ensure pgvector installed once (autocommit) ---
        conn.autocommit = True
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        logger.info("pgvector ensured")
        conn.autocommit = False  # back to transactional mode

        # --- Create tables (schema-qualified) ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.table_config (
                id_key VARCHAR(200) PRIMARY KEY,
                data_source_id VARCHAR(100) NOT NULL,
                table_name VARCHAR(100) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.column_config (
                id_key VARCHAR(200) PRIMARY KEY,
                data_source_id VARCHAR(100) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.table_context (
                id_key VARCHAR(255) PRIMARY KEY,
                embedding vector(1536),
                raw_text TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.column_context (
                id_key VARCHAR(255) PRIMARY KEY,
                embedding vector(1536),
                raw_text TEXT
            );
        """)

        conn.commit()

        # verify immediately from the same connection
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('table_config','column_config','table_context','column_context')
            ORDER BY table_name;
        """)
        found = cur.fetchall()
        logger.info("Found tables: %s", found)

        return {"status": "ok", "tables": found}

    except Exception as e:
        logger.exception("Bulk upload failed: %s", e)
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Bulk upload failed: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()




        SELECT current_user, session_user, current_database();

SELECT schemaname, tablename, tableowner
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('table_config','column_config','table_context','column_context');




GRANT USAGE ON SCHEMA public TO postgres;

GRANT SELECT ON TABLE public.table_context TO postgres;
GRANT SELECT ON TABLE public.column_context TO postgres;






cur.execute("GRANT USAGE ON SCHEMA public TO postgres;")
cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.table_context  TO postgres;")
cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.column_context TO postgres;")

# optional: let postgres see all current & future tables created by this owner
cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;")
cur.execute("""
ALTER DEFAULT PRIVILEGES FOR USER "query-genie@hsbc-12432649-c48nlpuk-dev.iam"
IN SCHEMA public
GRANT SELECT ON TABLES TO postgres;
""")
conn.commit()

