UPDATE public.table_config
SET
    description = 'Test metadata for Event Store',
    
    filter_columns = '["customer_id", "transaction_type"]'::jsonb,
    aggregate_columns = '["total_amount", "count"]'::jsonb,
    sort_columns = '["transaction_date"]'::jsonb,
    key_columns = '["event_id"]'::jsonb,
    join_tables = '["customer_master"]'::jsonb,
    related_business_terms = '["Fraud Detection", "Payment Journey"]'::jsonb,
    sample_usage = '[
        {"sql": "SELECT * FROM event_store LIMIT 10", "description": "Basic sample query"}
    ]'::jsonb,
    tags = '["UK Outbound", "Payment", "TestTag"]'::jsonb
WHERE table_name = 'event_store';
