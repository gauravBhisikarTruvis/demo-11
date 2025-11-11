# db_helper.py
import json
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

MARKET = "hsbc-12432649-c48nlpuk-dev"  # keep the market fixed as you requested

class DbHelper:
    def __init__(self):
        self.user = None  # fill if you track created_by/updated_by

    # ----- util -----
    def _conn(self, market: str = MARKET):
        from utils.google_cloud_sql import GoogleCloudSqlUtility  # your existing helper
        db_util = GoogleCloudSqlUtility(market)
        return db_util.get_db_connection()

    def _id_key_for_column(self, record: dict) -> str:
        table_name  = record.get("table_name") or record.get("table_name_details") or ""
        column_name = record.get("column_name") or record.get("column_name_details") or ""
        return f"{record.get('data_source_id','')}~{record.get('data_namespace','')}~{table_name}~{column_name}"

    # ============= COLUMN CONFIG (text) =================
    def insert_column_metadata(self, record: dict, table: str = "column_config") -> None:
        """
        Upsert one column record into public.column_config.
        Expects keys like: table_name[_details], column_name[_details], description, data_type,
        is_filterable, is_aggregatable, sample_values(list), related_business_terms(list), sample_usage(list/dict)
        """
        # Coalesce fields from your JSON
        table_name   = record.get("table_name") or record.get("table_name_details") or ""
        column_name  = record.get("column_name") or record.get("column_name_details") or ""
        data_source  = record.get("data_source_id", "")
        data_ns      = record.get("data_namespace", "")
        description  = record.get("description", "")
        data_type    = record.get("data_type", "")
        is_filter    = bool(record.get("is_filterable", False))
        is_aggr      = bool(record.get("is_aggregatable", False))

        sample_values = record.get("sample_values") or []
        if isinstance(sample_values, str):
            sample_values = [sample_values]

        related_terms = record.get("related_business_terms") or []
        if isinstance(related_terms, str):
            related_terms = [related_terms]

        # JSONB field – dump to JSON string and cast to ::jsonb in SQL
        sample_usage_json = json.dumps(record.get("sample_usage") or [], ensure_ascii=False)

        id_key = self._id_key_for_column({
            "table_name": table_name, "column_name": column_name,
            "data_source_id": data_source, "data_namespace": data_ns
        })

        sql = f"""
        INSERT INTO public.{table} (
            id_key, data_source_id, table_name, column_name, data_namespace,
            description, data_type, is_filterable, is_aggregatable,
            sample_values, related_business_terms, sample_usage
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s::jsonb
        )
        ON CONFLICT (id_key) DO UPDATE SET
            data_source_id = EXCLUDED.data_source_id,
            table_name = EXCLUDED.table_name,
            column_name = EXCLUDED.column_name,
            data_namespace = EXCLUDED.data_namespace,
            description = EXCLUDED.description,
            data_type = EXCLUDED.data_type,
            is_filterable = EXCLUDED.is_filterable,
            is_aggregatable = EXCLUDED.is_aggregatable,
            sample_values = EXCLUDED.sample_values,
            related_business_terms = EXCLUDED.related_business_terms,
            sample_usage = EXCLUDED.sample_usage;
        """

        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                sql,
                (
                    id_key, data_source, table_name, column_name, data_ns,
                    description, data_type, is_filter, is_aggr,
                    sample_values, related_terms, sample_usage_json
                ),
            )
            conn.commit()
        except Exception as e:
            if conn: conn.rollback()
            raise Exception(f"[insert_column_metadata] failed: {e}")
        finally:
            if conn: conn.close()

    # ============= COLUMN CONTEXT (embedding) ===========
    def insert_column_context(self, record: dict, table: str = "column_context") -> None:
        """
        Vectorizes a single column record and upserts into public.column_context.
        Relies on your existing self.get_openai_embedding(text) method.
        """
        table_name   = record.get("table_name") or record.get("table_name_details") or ""
        column_name  = record.get("column_name") or record.get("column_name_details") or ""
        data_source  = record.get("data_source_id", "")
        data_ns      = record.get("data_namespace", "")

        id_key = self._id_key_for_column({
            "table_name": table_name, "column_name": column_name,
            "data_source_id": data_source, "data_namespace": data_ns
        })

        # Build a readable string for embedding
        sample_values = record.get("sample_values") or []
        related_terms = record.get("related_business_terms") or []
        raw_text = "\n".join([
            f"table={table_name}",
            f"column={column_name}",
            f"description={record.get('description','')}",
            f"data_type={record.get('data_type','')}",
            f"is_filterable={bool(record.get('is_filterable', False))}",
            f"is_aggregatable={bool(record.get('is_aggregatable', False))}",
            f"sample_values={', '.join([str(v) for v in sample_values])}",
            f"related_business_terms={', '.join([str(v) for v in related_terms])}",
        ])

        # Your existing embedding function should return a list[float] (size 1536)
        embedding = self.get_openai_embedding(raw_text)

        sql = f"""
        INSERT INTO public.{table} (id_key, embedding, raw_text)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_key) DO UPDATE SET
            embedding = EXCLUDED.embedding,
            raw_text  = EXCLUDED.raw_text;
        """

        conn = None
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(sql, (id_key, embedding, raw_text))
            conn.commit()
        except Exception as e:
            if conn: conn.rollback()
            raise Exception(f"[insert_column_context] failed: {e}")
        finally:
            if conn: conn.close()
