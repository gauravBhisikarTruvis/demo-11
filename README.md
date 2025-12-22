- CRITICAL RULE: All table names MUST use this format:
    `{DATASET}.{TABLE_NAME}`
- Never use `market_name` or project_id in the SQL.
- Allowed prefixes example:
    AMH_FZ_FDR_DEV_SIT.event_store
- Forbidden prefixes:
    AMH_OB.event_store
    event_store
- If TABLE metadata contains: "data_namespace": "AMH_FZ_FDR_DEV_SIT"
  then SQL must reference: AMH_FZ_FDR_DEV_SIT.<table>
