from psycopg2.extras import Json

def insert_table_metadata(self, record: dict):

    table_name_val = record.get("table_name") or record.get("table_name_details") or ""
    id_key = (
        record.get("data_source_id", "") + "~" +
        record.get("data_namespace", "") + "~" +
        table_name_val
    )

    # map basic fields
    data_source_id   = record.get("data_source_id", "")
    data_namespace   = record.get("data_namespace", "")
    display_name     = record.get("display_name", "")
    description      = record.get("description", "")

    # TEXT[] fields must be Python lists
    filter_columns        = record.get("filter_columns", [])
    aggregate_columns     = record.get("aggregate_columns", [])
    sort_columns          = record.get("sort_columns", [])
    key_columns           = record.get("key_columns", [])
    related_business      = record.get("related_business_terms", [])
    tags                  = record.get("tags", [])

    # JSONB fields must use Json(...)
    join_tables           = Json(record.get("join_tables", []))
    sample_usage          = Json(record.get("sample_usage", []))

    sql_insert = """
        INSERT INTO table_config (
            id_key, data_source_id, table_name, display_name, data_namespace, description,
            filter_columns, aggregate_columns, sort_columns, key_columns,
            join_tables, related_business_terms, sample_usage, tags
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (id_key) DO UPDATE SET
            data_source_id = EXCLUDED.data_source_id,
            table_name     = EXCLUDED.table_name,
            display_name   = EXCLUDED.display_name,
            data_namespace = EXCLUDED.data_namespace,
            description    = EXCLUDED.description,
            filter_columns = EXCLUDED.filter_columns,
            aggregate_columns = EXCLUDED.aggregate_columns,
            sort_columns   = EXCLUDED.sort_columns,
            key_columns    = EXCLUDED.key_columns,
            join_tables    = EXCLUDED.join_tables,
            related_business_terms = EXCLUDED.related_business_terms,
            sample_usage   = EXCLUDED.sample_usage,
            tags           = EXCLUDED.tags;
    """

    conn = None
    try:
        db = GoogleCloudSqlUtility(self.market)
        conn = db.get_db_connection()
        cur = conn.cursor()

        cur.execute(sql_insert, (
            id_key, data_source_id, table_name_val, display_name, data_namespace, description,
            filter_columns, aggregate_columns, sort_columns, key_columns,
            join_tables, related_business, sample_usage, tags
        ))

        conn.commit()
    except Exception as e:
        raise Exception(f"[insert_table_metadata] failed: {e}")
    finally:
        if conn:
            conn.close()
