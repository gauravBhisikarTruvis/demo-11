# file: bq_impersonate_example.py
from google.auth import default
from google.auth.impersonated_credentials import ImpersonatedCredentials
from google.cloud import bigquery

# get source credentials (these are the VM's SA1 credentials from metadata)
source_credentials, _ = default()

target_service_account = "sa2@project2.iam.gserviceaccount.com"

impersonated_creds = ImpersonatedCredentials(
    source_credentials=source_credentials,
    target_principal=target_service_account,
    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    lifetime=3600,
)

# create BigQuery client using impersonated credentials and targeting project2
client = bigquery.Client(project="project2", credentials=impersonated_creds)

query = "SELECT CURRENT_TIMESTAMP() as now"
for row in client.query(query).result():
    print(row)
