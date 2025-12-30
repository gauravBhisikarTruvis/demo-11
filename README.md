CRITICAL BIGQUERY CONSTRAINTS (MANDATORY):

- TIMESTAMP_SUB MUST NEVER be used with MONTH or YEAR.
- If a time range involves months or years:
  - Convert months to days using a 30-day approximation.
  - Use TIMESTAMP_SUB(..., INTERVAL <N> DAY).
- Alternatively, CAST the timestamp to DATE and use DATE_SUB.
- Any SQL that violates BigQuery function constraints is INVALID.
