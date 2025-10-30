UPDATE table_config
SET 
  display_name = 'Customer Events Table',
  data_namespace = 'customer.analytics',
  description = 'Contains all digital customer event logs',
  tags = ARRAY['customer', 'events', 'tracking'],
  filter_columns = ARRAY['customer_id', 'event_type'],
  aggregate_columns = ARRAY['counter', 'max'],
  sort_columns = ARRAY['event_time desc'],
  updated_by = 'gaurav',
  updated_at = NOW()
WHERE table_name = 'event_store'
  AND data_source_id = 'hsbc-12010598-fdrasp-dev'
RETURNING *;
