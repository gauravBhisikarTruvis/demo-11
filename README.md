curl -X POST "http://10.99.25.52:9000/update-table-metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "market": "hsbc-12708196-qryrecoeng-dev",
    "table": "event_store",
    "obj": {
      "display_name": "Customer Events Table",
      "data_namespace": "customer.analytics",
      "description": "Contains all digital customer event logs",
      "tags": ["customer", "events", "tracking"],
      "filter_columns": ["customer_id", "event_type"],
      "aggregate_columns": ["counter", "max"],
      "sort_columns": ["event_time desc"],
      "updated_by": "gaurav"
    }
  }'
