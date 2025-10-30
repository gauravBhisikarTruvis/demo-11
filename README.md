curl -X POST "http://localhost:8000/update-table" \
-H "Content-Type: application/json" \
-d '{
  "market": "hsbc-12708196-qryrecoeng-dev",
  "table": "cm_events",
  "obj": {
    "display_name": "Customer Events Table",
    "data_namespace": "customer.analytics",
    "description": "Contains all digital customer event logs",
    "tags": ["customer", "events", "tracking"],
    "filter_columns": ["customer_id", "event_type"],
    "aggregate_columns": ["count", "max"],
    "sort_columns": ["event_time desc"],
    "updated_by": "gaurav"
  }
}'


curl -X POST "http://localhost:8000/update-column" \
-H "Content-Type: application/json" \
-d '{
  "market": "hsbc-12708196-qryrecoeng-dev",
  "table": "cm_events",
  "column": "event_time",
  "obj": {
    "description": "Event timestamp in UTC timezone",
    "data_type": "TIMESTAMP",
    "is_filterable": true,
    "is_aggregatable": false,
    "sample_values": ["2025-01-01T12:00:00Z", "2025-01-02T08:30:00Z"],
    "related_business_terms": ["time", "event", "customer"],
    "sample_usage": [{"sql": "SELECT event_time FROM cm_events LIMIT 10"}],
    "updated_by": "gaurav"
  }
}'
