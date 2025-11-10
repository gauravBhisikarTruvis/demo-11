def insert_table_metadata(self, record: dict, table_name: str = "table_config"):
    table_name_val = record.get("table_name") or record.get("table_name_details")
    id_key = generate_id_key(
        record.get("data_source_id"),
        record.get("data_namespace", ""),
        table_name_val
    )

    conn = None
    try:
        conn = psycopg2.connect(
            host=self.host, port=self.port,
            dbname=self.database, user=self.user, password=self.password
        )
        cur = conn.cursor()

        sql_insert = sql.SQL("""
            INSERT INTO {table} (id_key, data_source_id, table_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_key) DO UPDATE SET
                data_source_id = EXCLUDED.data_source_id,
                table_name     = EXCLUDED.table_name;
        """).format(table=sql.Identifier(table_name))

        cur.execute(sql_insert, (id_key, record["data_source_id"], table_name_val))
        conn.commit()
    except Exception as e:
        raise Exception(f"[insert_table_metadata] failed: {e}")
    finally:
        if conn:
            conn.close()







def insert_table_context(self, record: dict, table_name: str = "table_context"):
    table_name_val = record.get("table_name") or record.get("table_name_details")
    id_key = generate_id_key(
        record.get("data_source_id"),
        record.get("data_namespace", ""),
        table_name_val
    )

    raw_text = self._to_readable_text(record)
    embedding = self.get_openai_embedding(raw_text)

    conn = None
    try:
        conn = psycopg2.connect(
            host=self.host, port=self.port,
            dbname=self.database, user=self.user, password=self.password
        )
        cur = conn.cursor()

        sql_insert = sql.SQL("""
            INSERT INTO {table} (id_key, embedding, raw_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_key) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                raw_text  = EXCLUDED.raw_text;
        """).format(table=sql.Identifier(table_name))

        cur.execute(sql_insert, (id_key, embedding, raw_text))
        conn.commit()
    except Exception as e:
        raise Exception(f"[insert_table_context] failed: {e}")
    finally:
        if conn:
            conn.close()






def insert_column_metadata(self, record: dict, table_name: str = "column_config"):
    column_name = record.get("column_name") or record.get("column_name_details")

    id_key = generate_id_key(
        record.get("data_source_id"),
        record.get("data_namespace", ""),
        column_name
    )

    conn = None
    try:
        conn = psycopg2.connect(
            host=self.host, port=self.port,
            dbname=self.database, user=self.user, password=self.password
        )
        cur = conn.cursor()

        sql_insert = sql.SQL("""
            INSERT INTO {table} (id_key, data_source_id)
            VALUES (%s, %s)
            ON CONFLICT (id_key) DO UPDATE SET
                data_source_id = EXCLUDED.data_source_id;
        """).format(table=sql.Identifier(table_name))

        cur.execute(sql_insert, (id_key, record["data_source_id"]))
        conn.commit()
    except Exception as e:
        raise Exception(f"[insert_column_metadata] failed: {e}")
    finally:
        if conn:
            conn.close()




def insert_column_context(self, record: dict, table_name: str = "column_context"):
    column_name = record.get("column_name") or record.get("column_name_details")

    id_key = generate_id_key(
        record.get("data_source_id"),
        record.get("data_namespace", ""),
        column_name
    )

    raw_text = self._to_readable_text(record)
    embedding = self.get_openai_embedding(raw_text)

    conn = None
    try:
        conn = psycopg2.connect(
            host=self.host, port=self.port,
            dbname=self.database, user=self.user, password=self.password
        )
        cur = conn.cursor()

        sql_insert = sql.SQL("""
            INSERT INTO {table} (id_key, embedding, raw_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (id_key) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                raw_text  = EXCLUDED.raw_text;
        """).format(table=sql.Identifier(table_name))

        cur.execute(sql_insert, (id_key, embedding, raw_text))
        conn.commit()
    except Exception as e:
        raise Exception(f"[insert_column_context] failed: {e}")
    finally:
        if conn:
            conn.close()





@pg_router.post("/bulk-upload")
async def bulk_upload(payload: Metadata):
    """
    Process order:
      1) Every file in config/context/table/*.json
         - insert into table_config (text)
         - insert into table_context (embedding)
      2) Every file in config/context/column/*.json
         - insert into column_config (text)
         - insert into column_context (embedding)
    """
    import os, json, psycopg2
    from fastapi import HTTPException
    from langchain_community.document_loaders import JSONLoader

    # ---- config/paths ----
    try:
        cfg = load_config(payload.market)
        project_id = cfg.get("Database", "bigquery_project")
        dataset_id = cfg.get("Database", "bigquery_dataset")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Config load failed: {e}")

    CONTEXT_BASE = os.path.join("config", "context")
    TABLE_DIR    = os.path.join(CONTEXT_BASE, "table")
    COLUMN_DIR   = os.path.join(CONTEXT_BASE, "column")

    # ---- ensure DB objects exist ----
    conn = cur = None
    try:
        db_util = GoogleCloudSqlUtility(payload.market)
        conn = db_util.get_db_connection()
        cur  = conn.cursor()

        conn.autocommit = True
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        conn.autocommit = False

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
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"DDL failed: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

    # ---- use existing loader for inserts ----
    loader = PostgresLoader(payload.market)

    # idempotent create via loader if needed
    try:
        loader.create_table_config()
        loader.create_column_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Loader create_* failed: {e}")

    # Counters
    table_cfg_ct = table_ctx_ct = 0
    col_cfg_ct   = col_ctx_ct   = 0

    # ---- 1) TABLE files: text -> embedding (per doc) ----
    try:
        if os.path.isdir(TABLE_DIR):
            for fn in sorted(os.listdir(TABLE_DIR)):
                if not fn.endswith(".json"):
                    continue
                fp = os.path.join(TABLE_DIR, fn)
                docs = JSONLoader(
                    file_path=fp,
                    jq_schema=".",
                    text_content=False,
                    json_lines=False
                ).load()

                for doc in docs:
                    record = json.loads(doc.page_content)
                    record["data_source_id"] = project_id
                    record["data_namespace"] = dataset_id

                    # text first
                    loader.insert_table_metadata(record, "table_config")
                    table_cfg_ct += 1
                    # then embedding
                    loader.insert_table_context(record, "table_context")
                    table_ctx_ct += 1
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table phase failed: {e}")

    # ---- 2) COLUMN files: text -> embedding (per doc) ----
    try:
        if os.path.isdir(COLUMN_DIR):
            for fn in sorted(os.listdir(COLUMN_DIR)):
                if not fn.endswith(".json"):
                    continue
                fp = os.path.join(COLUMN_DIR, fn)
                docs = JSONLoader(
                    file_path=fp,
                    jq_schema=".",
                    text_content=False,
                    json_lines=False
                ).load()

                for doc in docs:
                    record = json.loads(doc.page_content)
                    record["data_source_id"] = project_id
                    record["data_namespace"] = dataset_id

                    # text first
                    loader.insert_column_metadata(record, "column_config")
                    col_cfg_ct += 1
                    # then embedding
                    loader.insert_column_context(record, "column_context")
                    col_ctx_ct += 1
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Column phase failed: {e}")

    return {
        "status": "success",
        "message": "Bulk upload complete (tables first, then columns; text then embedding per doc).",
        "counts": {
            "table_config": table_cfg_ct,
            "table_context": table_ctx_ct,
            "column_config": col_cfg_ct,
            "column_context": col_ctx_ct
        }
    }
