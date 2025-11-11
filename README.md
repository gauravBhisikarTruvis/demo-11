# TABLE files
for fn in sorted(os.listdir(TABLE_DIR)):
    if not fn.endswith(".json"):
        continue
    fp = os.path.join(TABLE_DIR, fn)
    docs = JSONLoader(file_path=fp, jq_schema=".", text_content=False, json_lines=False).load()

    # docs[i].page_content is a JSON string.
    for doc in docs:
        parsed = json.loads(doc.page_content)  # typically a LIST of dicts
        if isinstance(parsed, dict):
            parsed = [parsed]

        for rec in parsed:                    # <-- iterate list of dicts
            rec["data_source_id"] = project_id
            rec["data_namespace"] = dataset_id

            insert_table_metadata(rec)        # text first
            insert_table_context(rec)         # then embedding





import psycopg2
from psycopg2.extras import Json
from datetime import datetime

def _safe_list(v):
    return v if isinstance(v, list) else []

def _get_table_name(rec):
    # your JSON uses "table_name_details"; fall back to "table_name"
    return rec.get("table_name") or rec.get("table_name_details") or ""

def _gen_id_key(rec):
    # stable id_key: data_source_id~data_namespace^table_name
    return f"{rec.get('data_source_id','')}~{rec.get('data_namespace','')}^{_get_table_name(rec)}"

def insert_table_metadata(record: dict):
    table_name_val = _get_table_name(record)
    id_key = _gen_id_key(record)

    payload = {
        "id_key": id_key,
        "data_source_id": record.get("data_source_id", ""),
        "table_name": table_name_val,
        "display_name": record.get("display_name", ""),
        "data_namespace": record.get("data_namespace", ""),
        "description": record.get("description", ""),

        # arrays (TEXT[])
        "filter_columns": _safe_list(record.get("filter_columns", [])),
        "aggregate_columns": _safe_list(record.get("aggregate_columns", [])),
        "sort_columns": _safe_list(record.get("sort_columns", [])),
        "key_columns": _safe_list(record.get("key_columns", [])),
        "related_business_terms": _safe_list(record.get("related_business_terms", [])),
        "tags": _safe_list(record.get("tags", [])),

        # jsonb
        "join_tables": record.get("join_tables", []),     # list/dict -> JSONB
        "sample_usage": record.get("sample_usage", []),   # list of {description, sql}

        # audit
        "created_by": record.get("created_by", "bulk-upload"),
        "updated_by": record.get("updated_by", "bulk-upload"),
    }

    sql_insert = """
    INSERT INTO public.table_config
    ( id_key, data_source_id, table_name, display_name, data_namespace, description,
      filter_columns, aggregate_columns, sort_columns, key_columns, join_tables,
      related_business_terms, sample_usage, tags,
      created_at, updated_at, created_by, updated_by )
    VALUES
    ( %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s::jsonb,
      %s, %s::jsonb, %s,
      NOW(), NOW(), %s, %s )
    ON CONFLICT (id_key) DO UPDATE SET
      data_source_id = EXCLUDED.data_source_id,
      table_name = EXCLUDED.table_name,
      display_name = EXCLUDED.display_name,
      data_namespace = EXCLUDED.data_namespace,
      description = EXCLUDED.description,
      filter_columns = EXCLUDED.filter_columns,
      aggregate_columns = EXCLUDED.aggregate_columns,
      sort_columns = EXCLUDED.sort_columns,
      key_columns = EXCLUDED.key_columns,
      join_tables = EXCLUDED.join_tables,
      related_business_terms = EXCLUDED.related_business_terms,
      sample_usage = EXCLUDED.sample_usage,
      tags = EXCLUDED.tags,
      updated_at = NOW(),
      updated_by = EXCLUDED.updated_by;
    """

    conn = cur = None
    try:
        db_util = GoogleCloudSqlUtility("<your-market-here>")  # or pass in
        conn = db_util.get_db_connection()
        cur = conn.cursor()
        cur.execute(
            sql_insert,
            (
                payload["id_key"],
                payload["data_source_id"],
                payload["table_name"],
                payload["display_name"],
                payload["data_namespace"],
                payload["description"],
                payload["filter_columns"],
                payload["aggregate_columns"],
                payload["sort_columns"],
                payload["key_columns"],
                Json(payload["join_tables"]),
                payload["related_business_terms"],
                Json(payload["sample_usage"]),
                payload["tags"],
                payload["created_by"],
                payload["updated_by"],
            ),
        )
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        raise Exception(f"[insert_table_metadata] failed: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()
