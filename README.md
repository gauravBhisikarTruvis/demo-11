# 1. Get current instance metadata
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
INSTANCE_NAME=$(hostname)
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $4}')

# 2. Get the current 'fingerprint' (Required by Google to prevent edit conflicts)
FINGERPRINT=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/metadata/fingerprint")

# 3. Send the API request to set runScript=false
curl -X POST "https://compute.googleapis.com/compute/v1/projects/${PROJECT_ID}/zones/${ZONE}/instances/${INSTANCE_NAME}/setMetadata" \
    -H "Authorization: Bearer $(curl -s -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token | jq -r .access_token)" \
    -H "Content-Type: application/json" \
    -d "{
      \"fingerprint\": \"${FINGERPRINT}\",
      \"items\": [
        {
          \"key\": \"runScript\",
          \"value\": \"false\"
        }
      ]
    }"
