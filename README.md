resource "google_sql_ssl_cert" "client_cert" {
  common_name = "my-app-client"   # The name for this certificate
  instance    = google_sql_database_instance.main.name
}


# 1. Save the Server CA (public)
resource "local_file" "server_ca" {
  filename = "${path.module}/server-ca.pem"
  # The server CA comes from the database instance resource, not the client cert resource
  content  = google_sql_database_instance.main.server_ca_cert[0].cert
}

# 2. Save the Client Certificate (public)
resource "local_file" "client_cert" {
  filename = "${path.module}/client-cert.pem"
  content  = google_sql_ssl_cert.client_cert.cert
}

# 3. Save the Client Private Key (private/sensitive)
resource "local_file" "client_key" {
  filename = "${path.module}/client-key.pem"
  content  = google_sql_ssl_cert.client_cert.private_key
  
  # Important: Restrict permissions so only your user can read the key
  file_permission = "0600" 
}
