from typing import Dict, Any, List
from pydantic import BaseModel
from fastapi import HTTPException
import json
import traceback

# Models
class TableUpdateRequest(BaseModel):
    market: str
    table: str
    obj: Dict[str, Any]

# route
@pg_router.post("/update-table-metadata")
def update_table_metadata(payload: TableUpdateRequest):
    # basic validation
    if not payload.obj:
        raise HTTPException(status_code=400, detail='"obj" must contain at least one field')

    db_util = GoogleCloudSqlUtility(payload.market)
    conn = None
    try:
        conn = db_util.get_db_connection()
        if conn is None:
            raise RuntimeError("DB connection is None (check credentials/market)")

        # Build SET parts and params safely
        set_parts: List[str] = []
        params: List[Any] = []

        for col, val in payload.obj.items():
            # quote identifier (simple quoting)
            col_quoted = f'"{col}"'

            # Lists -> SQL ARRAY literal with parameter placeholders
            if isinstance(val, list):
                if len(val) == 0:
                    # Set to empty array (text[]). If you want a specific type change accordingly
                    set_parts.append(f"{col_quoted} = %s")
                    params.append([])  # pass empty list
                else:
                    placeholders = ", ".join(["%s"] * len(val))
                    # create ARRAY[...] expression; driver will substitute each %s
                    set_parts.append(f"{col_quoted} = ARRAY[{placeholders}]")
                    params.extend(val)
            # dicts -> JSONB
            elif isinstance(val, dict):
                set_parts.append(f"{col_quoted} = %s::jsonb")
                params.append(json.dumps(val, ensure_ascii=False))
            # anything else -> scalar
            else:
                set_parts.append(f"{col_quoted} = %s")
                params.append(val)

        # always update timestamp
        set_parts.append("updated_at = NOW()")

        sql_query = f"""
        UPDATE public.table_config
        SET {', '.join(set_parts)}
        WHERE table_name = %s
          AND data_source_id = %s
        RETURNING *;
        """

        # table_name then data_source_id (market)
        params.extend([payload.table, payload.market])

        # Execute in a with-block to ensure cursor closed
        with conn, conn.cursor() as cur:
            cur.execute(sql_query, params)
            rows = cur.fetchall()
            if not rows:
                # nothing updated
                raise HTTPException(status_code=404, detail="No matching row to update")

            cols = [d[0] for d in cur.description]
            updated_rows = [dict(zip(cols, r)) for r in rows]

        # commit happens via context-manager above; but ensure commit for safety
        try:
            conn.commit()
        except Exception:
            # if commit fails, surface it
            traceback.print_exc()
            raise

        # --- Now create embedding & upsert into table_context ---
        # Build raw_text (human readable summary) from payload.obj (same style as your insert_table_context)
        raw_parts = []
        for k, v in payload.obj.items():
            if isinstance(v, (list, dict)):
                raw_parts.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")
            else:
                raw_parts.append(f"{k} = {v}")
        raw_text = "\n".join(raw_parts)

        # create id_key consistent with your scheme
        data_namespace = payload.obj.get("data_namespace", "")
        id_key = f"{payload.market}.{data_namespace}.{payload.table}"

        # get embedding (PostgresVectorLoader should return iterable of floats)
        gemini_em = PostgresVectorLoader()
        embedding = gemini_em.get_gemini_embedding(raw_text)
        embedding = [float(x) for x in embedding]  # ensure floats

        # convert embedding to vector literal if adapter is not registered
        emb_literal = "[" + ",".join(map(str, embedding)) + "]"

        upsert_sql = """
        INSERT INTO public.table_context (id_key, embedding, raw_text)
        VALUES (%s, %s::vector, %s)
        ON CONFLICT (id_key) DO UPDATE
           SET embedding = EXCLUDED.embedding,
               raw_text  = EXCLUDED.raw_text;
        """

        with conn, conn.cursor() as cur:
            cur.execute(upsert_sql, (id_key, emb_literal, raw_text))
        try:
            conn.commit()
        except Exception:
            traceback.print_exc()
            raise

        return {"message": "update complete", "updated_rows": updated_rows}

    except HTTPException:
        # re-raise HTTP errors
        raise
    except Exception as e:
        # rollback if open
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        # log full traceback to server logs, return 500 to client
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


                ----------------
# after building set_parts and params (list)
set_parts.append("updated_at = NOW()")

# build sql_query with placeholders for id_key (do NOT interpolate id_key directly)
sql_query = """
UPDATE public.table_config
SET {set_clause}
WHERE id_key = %s
RETURNING *;
""".format(set_clause=", ".join(set_parts))

# append id_key to params (id_key variable you computed earlier)
params.append(id_key)

print("Executing table_config update:", sql_query)
print("params length:", len(params))

with conn, conn.cursor() as cur:
    cur.execute(sql_query, params)    # <-- pass params here
    rows = cur.fetchall()

if not rows:
    raise HTTPException(status_code=404, detail="No matching row to update")

cols = [d[0] for d in cur.description]
updated_rows = [dict(zip(cols, r)) for r in rows]

# commit once
try:
    conn.commit()
except Exception:
    traceback.print_exc()
    raise






# build raw_text (you already do that)
raw_parts = []
for k, v in payload.obj.items():
    if isinstance(v, (list, dict)):
        raw_parts.append(f"{k} = {json.dumps(v, ensure_ascii=False)}")
    else:
        raw_parts.append(f"{k} = {v}")
raw_text = "\n".join(raw_parts)

# compute id_key consistent with your scheme
data_namespace = payload.obj.get("data_namespace", "")
id_key = f"{payload.market}.{data_namespace}.{payload.table}"

# get embedding (list of floats)
gemini_em = PostgresVectorLoader()
embedding = gemini_em.get_gemini_embedding(raw_text)
embedding = [float(x) for x in embedding]

# if you don't have pgvector adapter, make a vector literal string
emb_literal = "[" + ",".join(map(str, embedding)) + "]"

# Parameterized upsert (do NOT use f-string to interpolate values)
vector_sql = """
INSERT INTO public.table_context (id_key, embedding, raw_text)
VALUES (%s, %s::vector, %s)
ON CONFLICT (id_key) DO UPDATE
  SET embedding = EXCLUDED.embedding,
      raw_text  = EXCLUDED.raw_text;
"""

print("Table context update SQL:", vector_sql)

with conn, conn.cursor() as cur:
    cur.execute(vector_sql, (id_key, emb_literal, raw_text))

try:
    conn.commit()
except Exception:
    traceback.print_exc()
    raise

