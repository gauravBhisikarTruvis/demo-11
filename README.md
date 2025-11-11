UPDATE public.table_config
SET
  description         = 'Test metadata for Event Store',

  -- TEXT[] columns
  filter_columns      = ARRAY['customer_id', 'transaction_type']::text[],
  aggregate_columns   = ARRAY['total_amount', 'count']::text[],
  sort_columns        = ARRAY['transaction_date']::text[],
  key_columns         = ARRAY['event_id']::text[],
  tags                = ARRAY['UK Outbound', 'Payment']::text[],
  related_business_terms = ARRAY['Payments', 'Events']::text[],

  -- JSONB columns
  join_tables         = '["customer_master"]'::jsonb,
  sample_usage        = jsonb_build_array(
                          jsonb_build_object(
                            'description','Recent 10 events',
                            'sql','SELECT * FROM event_store ORDER BY transaction_date DESC LIMIT 10'
                          ),
                          jsonb_build_object(
                            'description','Top customers by count',
                            'sql','SELECT customer_id, COUNT(*) AS cnt FROM event_store GROUP BY customer_id ORDER BY cnt DESC LIMIT 5'
                          )
                        )
WHERE table_name = 'event_store';
