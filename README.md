# Add / paste into your module (pg_read.py)
import json
import traceback
from typing import Any, Dict, List

from pydantic import BaseModel
from fastapi import HTTPException

# Import your utilities
# from src.utils.db_util import GoogleCloudSqlUtility
# from src.utils.vector_loader import PostgresVectorLoader
# (use the imports that match your code base)

# -------------------------
# Pydantic model (use your existing one if already declared)
class ColumnUpdateRequest(BaseModel):
    market: str
    data_namespace: str = ""
    table: str
    column: str
    obj: Dict[str, Any]

# -------------------------
# small helpers to create SQL literals (NOT a secure sanitizer, but makes rendered SQL safer-format)
def _sql_quote(value: Any) -> str:
    """Return simple SQL literal for most scalar types (single-quote and escape)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    # escape single quotes
    return "'" + s.replace("'", "''") + "'"

def _sql_jsonb(v: Any) -> str:
    return _sql_quote(json.dumps(v, ensure_ascii=False))  # wrap JSON string as SQL literal; cast later if needed

def _sql_array_text(lst: List[Any]) -> str:
    """Render list to an SQL ARRAY literal of text values."""
    if not lst:
        return "ARRAY[]::text[]"
    parts = []
    for x in lst:
        if isinstance(x, (dict, list)):
            parts.append(_sql_quote(json.dumps(x, ensure_ascii=False)))
        else:
            parts.append(_sql_quote(x))
    return "ARRAY[" + ",".join(parts) + "]"

# -------------------------
# The route (synchronous)
# Decorator name and router object must match yours; replace `pg_router` with your router
# Example: if you use `pg_router = APIRouter()` earlier, use that. Otherwise use `app.post(...)`.
@pg_router.post("/update-column-metadata")
def update_column_metadata(payload: ColumnUpdateRequest):
    """
    Update column metadata (column_config) and upsert column_context (embedding).
    Uses f-string rendered SQL and prints the SQL executed.
    """
    conn = None
    cur = None
    try:
        # validations
        if not payload.obj or not isinstance(payload.obj, dict):
            raise HTTPException(status_code=400, detail="'obj' must contain at least one field")

        # Build id_key for column_config (you requested "~" style)
        data_source = payload.market
        data_namespace = payload.data_namespace or ""
        table = payload.table or ""
        column = payload.column or ""

        id_key_cfg = f"{data_source}~{data_namespace}~{table}~{column}"
        # id_key for column_context uses '.' (as you requested)
        id_key_ctx = f"{data_source}.{data_namespace}.{table}.{column}"

        # Get DB connection (adapt to your project util)
        db_util = GoogleCloudSqlUtility(data_source)
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None (check credentials/market)")

        # Build SET clauses (rendered)
        set_parts: List[str] = []
        for k, v in payload.obj.items():
            # identifier quoting (simple)
            col_ident = f'"{k}"'   # assumes column names do not contain double-quotes
            if isinstance(v, list):
                # lists -> SQL ARRAY text representation
                set_parts.append(f"{col_ident} = {_sql_array_text(v)}")
            elif isinstance(v, dict):
                # dictionaries -> jsonb
                set_parts.append(f"{col_ident} = {_sql_jsonb(v)}::jsonb")
            elif isinstance(v, bool):
                set_parts.append(f"{col_ident} = {'TRUE' if v else 'FALSE'}")
            elif v is None:
                set_parts.append(f"{col_ident} = NULL")
            else:
                # scalar -> quoted literal
                set_parts.append(f"{col_ident} = {_sql_quote(v)}")

        # always update timestamp
        set_parts.append("updated_at = NOW()")

        set_clause = ", ".join(set_parts)

        # Render the final UPDATE statement completely (f-string style)
        sql_cfg = f"""
UPDATE public.column_config
SET {set_clause}
WHERE id_key = {_sql_quote(id_key_cfg)}
RETURNING *;
""".strip()

        # Print the SQL to logs (exact SQL)
        print("\n--- COLUMN CONFIG UPDATE SQL ---")
        print(sql_cfg)

        # Execute update
        cur = conn.cursor()
        cur.execute(sql_cfg)
        rows = cur.fetchall()
        if not rows:
            conn.rollback()
            raise HTTPException(status_code=404, detail="No matching column_config row to update")

        cols = [d[0] for d in cur.description]
        updated_rows = [dict(zip(cols, r)) for r in rows]

        # commit config update
        conn.commit()
        print("column_config update committed, rows:", len(updated_rows))

        # Build raw_text from payload.obj for embedding
        raw_parts = []
        for k, v in payload.obj.items():
            if isinstance(v, (dict, list)):
                raw_parts.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")
            else:
                raw_parts.append(f"{k} = {v}")
        raw_text = "\n".join(raw_parts)

        # Compute embedding (call your loader)
        gemini_em = PostgresVectorLoader()
        embedding = gemini_em.get_gemini_embedding(raw_text)   # expected list of numbers
        embedding = [float(x) for x in embedding]

        # embed literal and vector upsert SQL (rendered)
        emb_literal = "[" + ",".join(map(str, embedding)) + "]"
        vector_sql = f"""
INSERT INTO public.column_context (id_key, embedding, raw_text)
VALUES ({_sql_quote(id_key_ctx)}, {emb_literal}::vector, {_sql_quote(raw_text)})
ON CONFLICT (id_key) DO UPDATE
  SET embedding = EXCLUDED.embedding,
      raw_text = EXCLUDED.raw_text;
""".strip()

        # Print vector SQL (can be long; print prefix)
        print("\n--- COLUMN CONTEXT UPSERT SQL (head) ---")
        print(vector_sql[:1000] + ("\n... (truncated)" if len(vector_sql) > 1000 else ""))

        # Execute vector upsert
        cur2 = conn.cursor()
        cur2.execute(vector_sql)
        conn.commit()
        try:
            cur2.close()
        except Exception:
            pass

        return {"message": "update complete", "updated_rows": updated_rows}

    except HTTPException:
        raise
    except Exception as e:
        # rollback and rethrow as 500
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")
    finally:
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
