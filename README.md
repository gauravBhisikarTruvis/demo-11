resource "google_sql_database_instance" "main_db" {
  name             = local.sql_instance_name
  project          = var.my_project
  database_version = "POSTGRES_15"
  region           = var.region
  deletion_protection = true

  settings {
    tier               = "db-custom-1-3840"
    availability_type  = "ZONAL"
    disk_type          = "PD_SSD"
    disk_size          = 15
    disk_autoresize    = true

    backup_configuration {
      enabled = true
      start_time = "20:00"
      backup_retention_settings {
        retained_backups = 7
      }
    }

    point_in_time_recovery_enabled = true

    ip_configuration {
      ipv4_enabled    = false
      private_network = "projects/${var.my_project}/global/networks/${var.default_network}"
      # ssl_mode may be provider dependent — keep if supported for your provider version
      ssl_mode = "TRUSTED_CLIENT_CERTIFICATE_REQUIRED"
    }

    # REQUIRED BY ORG POLICY: make sure this exact block is present inside settings
    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"         # must be the string "on"
    }

    # Add other validated flags (verify support per Postgres 15)
    database_flags {
      name  = "cloudsql.enable_pgaudit"
      value = "on"         # prefer "on" over boolean true when flag expects strings
    }

    database_flags {
      name  = "pgaudit.log"
      value = "all"
    }

    insights_config {
      query_insights_enabled = true
      record_application_tags = true
      record_client_address   = true
      query_string_length     = 1024
      query_plans_per_minute  = 10
    }

    maintenance_window {
      day = 7
      hour = 20
      update_track = "canary"
    }
  } # end settings

  encryption_key_name = local.kms_sql_key

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [settings]
  }
}
