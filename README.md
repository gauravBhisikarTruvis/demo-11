from typing import Dict, Any, List
from pydantic import BaseModel
from fastapi import HTTPException
import json
import traceback

# Request model for column update
class ColumnUpdateRequest(BaseModel):
    market: str         # data_source / market (same as update_table_metadata)
    table: str
    column: str
    obj: Dict[str, Any]

@pg_router.post("/update-column-metadata")
def update_column_metadata(payload: ColumnUpdateRequest):
    """
    Update column metadata (public.column_config) and upsert embedding into public.column_context.
    """
    # Basic validation
    if not payload.obj:
        raise HTTPException(status_code=400, detail='"obj" must contain at least one field')

    db_util = GoogleCloudSqlUtility(payload.market)
    conn = None
    cur = None

    try:
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None (check credentials/market)")

        # Build SET parts and params
        set_parts: List[str] = []
        params: List[Any] = []

        for col, val in payload.obj.items():
            # Simple quoting for identifier (assumes safe column names)
            col_quoted = f'"{col}"'

            # Lists -> ARRAY[...]
            if isinstance(val, list):
                if len(val) == 0:
                    set_parts.append(f"{col_quoted} = %s")
                    params.append([])  # driver-specific handling; pg8000 will accept python list for array in many cases
                else:
                    placeholders = ", ".join(["%s"] * len(val))
                    set_parts.append(f"{col_quoted} = ARRAY[{placeholders}]")
                    params.extend(val)
            # dict -> jsonb
            elif isinstance(val, dict):
                set_parts.append(f"{col_quoted} = %s::jsonb")
                params.append(json.dumps(val, ensure_ascii=False))
            # scalar
            else:
                set_parts.append(f"{col_quoted} = %s")
                params.append(val)

        # Always update timestamp
        set_parts.append("updated_at = NOW()")

        # Build id_key for column_context and for WHERE clause (match your scheme)
        # Use same id_key style as your column_context insert: data_source.data_namespace.table.column
        data_source = payload.market
        data_namespace = payload.obj.get("data_namespace", "")
        id_key = f"{data_source}.{data_namespace}.{payload.table}.{payload.column}"

        # Final UPDATE query (parameterized)
        sql_query = """
UPDATE public.column_config
SET {set_clause}
WHERE id_key = %s
RETURNING *;
""".format(set_clause=", ".join(set_parts))

        # Append id_key to params (last placeholder)
        params.append(id_key)

        # Execute update
        cur = conn.cursor()
        cur.execute(sql_query, params)
        rows = cur.fetchall()

        if not rows:
            conn.rollback()
            raise HTTPException(status_code=404, detail="No matching row to update")

        cols = [d[0] for d in cur.description]
        updated_rows = [dict(zip(cols, r)) for r in rows]

        # Commit update
        try:
            conn.commit()
        except Exception:
            traceback.print_exc()
            raise

        # --- Build raw_text summary from payload.obj (human readable) ---
        raw_parts = []
        for k, v in payload.obj.items():
            if isinstance(v, (list, dict)):
                raw_parts.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")
            else:
                raw_parts.append(f"{k} = {v}")
        raw_text = "\n".join(raw_parts)

        # --- Build embedding and upsert into column_context ---
        gemini_em = PostgresVectorLoader()
        embedding = gemini_em.get_gemini_embedding(raw_text)
        embedding = [float(x) for x in embedding]  # ensure float

        # If you don't register pgvector adapter, pass vector as literal and cast to ::vector
        emb_literal = "[" + ",".join(map(str, embedding)) + "]"

        vector_sql = """
INSERT INTO public.column_context (id_key, embedding, raw_text)
VALUES (%s, %s::vector, %s)
ON CONFLICT (id_key) DO UPDATE
  SET embedding = EXCLUDED.embedding,
      raw_text  = EXCLUDED.raw_text;
"""

        # Upsert embedding
        # Use new cursor for this block; ensure safe close
        try:
            cur2 = conn.cursor()
            cur2.execute(vector_sql, (id_key, emb_literal, raw_text))
            conn.commit()
            try:
                cur2.close()
            except Exception:
                pass
        except Exception:
            # rollback and bubble up
            try:
                conn.rollback()
            except Exception:
                pass
            traceback.print_exc()
            raise

        # return success info
        return {"message": "column update complete", "updated_rows": updated_rows}

    except HTTPException:
        # re-raise known http exceptions
        raise
    except Exception as e:
        # rollback if needed and wrap into HTTPException
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")
    finally:
        # close cursor/connection
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass
