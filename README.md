resource "local_file" "server_ca_pem" {
  # Go up one level (..) to save it in the 'main' folder
  filename = "${path.module}/../server-ca.pem"
  
  content  = google_sql_database_instance.main_db.server_ca_cert[0].cert
}
