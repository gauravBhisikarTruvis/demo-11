# Create the raw query string with %s placeholders
sql_query = """
SELECT raw_text,
    ((2 - (embedding <-> %s::vector)) / 2) AS similarity_score
FROM column_context
ORDER BY embedding <-> %s::vector
LIMIT %s
"""

# Use mogrify to see the query with actual values
debug_query = self.cur.mogrify(sql_query, (query_embedding, query_embedding, self.TOP_K_DB_FETCH))
print(debug_query.decode('utf-8')) # This will print the full query with values
