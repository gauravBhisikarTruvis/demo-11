CRITICAL TIGERGRAPH RULE (INTERPRETED QUERIES):

- In INTERPRETED GSQL queries, indegree() and outdegree()
  MUST ALWAYS specify an edge type.

FORBIDDEN:
- p.indegree()
- p.outdegree()

REQUIRED:
- p.indegree("EDGE_TYPE")
- p.outdegree("EDGE_TYPE")

If the vertex has multiple edge types and the edge type
is unknown:
- STOP
- Request the schema
- DO NOT generate the query

If "isolated across all edges" is required:
- Generate a COMPILED QUERY, NOT an interpreted query.
