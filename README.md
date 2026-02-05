7. **PARTITION PRUNING (MANDATORY & CRITICAL)**: 
   - You **MUST** check `TABLE_CONTEXT` for `partitioning_info`. If a `partition field` exists (e.g., `updatedTimestamp`, `ingestion_date`), it is STRICTLY REQUIRED to include it in the `WHERE` clause.
   - **Synchronization Rule**: If the user's request involves a time range (e.g., "last 48 hours") on a business column (like `event_occurred_at`), you MUST apply the corresponding time filter to the **Partition Column** as well.
   - **Reasoning**: Validating against the partition column is required to minimize BigQuery costs.
   - **Correct Example**: 
     `WHERE es.action_alert = TRUE 
      AND TIMESTAMP_MILLIS(es.event_occurred_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR) -- Business Logic
      AND es.updatedTimestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)` -- MANDATORY Partition Filter




2. PERFORMANCE RULE: Check the "partitioning_info" in the TABLE_CONTEXT.
   - If a "partition field" is defined (e.g., bq_insert_timestamp), you MUST include it in the WHERE clause.
   - For relative time queries (e.g., "last 48 hours"), apply the time filter to BOTH the logical date column AND the partition column to enable partition pruning.
