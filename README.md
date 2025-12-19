{
  "project": "hsbc-12010598-fdrasp-dev",
  "market_name": "AMH_OB",
  "sql_query": "SELECT bioc,tmx,session_id,session_id_at_local,customer_id_global,customer_id_number,payment_message_source,event_code,customer_change_data_new,customer_change_data_old,scameter_warning,sender_transaction_amount_dbl,lifecycle_id,bulk_id,payment_type,payment_sub_type,product_code,payment_status,customer_id_global,* FROM `hsbc-12010598-fdrasp-dev.HASE_FZ_FDR_DEV_SIT.event_store` WHERE customer_id_number IN ('ID772575','ID772906') AND event_code IN ('TMS') AND DATE(TIMESTAMP_MILLIS(event_received_at)) BETWEEN '2025-09-01' AND '2025-09-15'"
}
