# 1) Get embedding (list-like)
embedding = gemini_em.get_gemini_embedding(raw_text)

# 2) Normalize to native Python floats
embedding = [float(x) for x in embedding]

# 3) Build bracketed string LITERALLY like "[0.1,-0.2,...]"
embedding_str = "[" + ",".join(map(str, embedding)) + "]"

# 4) Parameterized SQL: pass embedding_str as a parameter and cast to vector
upsert_sql = f"""
INSERT INTO public.{table} (id_key, embedding, raw_text)
VALUES (%s, %s::vector, %s)
ON CONFLICT (id_key) DO UPDATE
  SET embedding = EXCLUDED.embedding,
      raw_text  = EXCLUDED.raw_text;
"""

conn = db_util.get_db_connection()   # returns pg8000 connection
cur = conn.cursor()                  # pg8000: no cursor_factory
cur.execute(upsert_sql, (id_key, embedding_str, raw_text))
conn.commit()
cur.close()
