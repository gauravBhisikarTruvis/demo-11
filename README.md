#!/bin/bash

# --- 1. SET UP LOGGING ---
LOG_FILE="/var/log/metadata_update.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "--- Starting Metadata Update: $(date) ---"

# --- 2. GATHER ENVIRONMENT DATA ---
# Using the same curl method from your startup script
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
INSTANCE_NAME=$(hostname)
ZONE_PATH=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone)
ZONE=$(echo "$ZONE_PATH" | awk -F/ '{print $4}')

echo "Project: $PROJECT_ID"
echo "Instance: $INSTANCE_NAME"
echo "Zone: $ZONE"

# --- 3. ATTEMPT UPDATE ---
echo "Attempting to set runScript=false..."

# We use gcloud because it provides the most detailed error logs
gcloud compute instances add-metadata "$INSTANCE_NAME" \
    --metadata runScript=false \
    --zone="$ZONE" \
    --project="$PROJECT_ID" \
    --quiet

# --- 4. ERROR HANDLING ---
if [ $? -eq 0 ]; then
    echo "SUCCESS: runScript updated to false."
else
    echo "ERROR: Failed to update metadata."
    echo "Checking for common permission issues..."
    
    # Test if the Service Account is valid
    gcloud auth list
    
    echo "Check the error above. If it mentions '403' or 'access', your VM needs"
    echo "the 'Compute Instance Admin' role in IAM."
fi

echo "--- Process Finished ---"
