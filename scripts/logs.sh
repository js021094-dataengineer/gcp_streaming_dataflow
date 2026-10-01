#!/usr/bin/env bash
# Recent producer logs from Cloud Logging (no SSH needed).
source "$(dirname "$0")/lib.sh"
gcloud logging read 'logName:"projects/'"${PROJECT_ID}"'/logs/python"' \
  --project "${PROJECT_ID}" --freshness=30m --limit "${1:-50}" --order=asc \
  --format='value(timestamp,severity,textPayload,jsonPayload.message)'
