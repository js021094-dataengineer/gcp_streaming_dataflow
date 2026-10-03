#!/usr/bin/env bash
# Recent producer logs from Cloud Logging (no SSH needed).
# Usage: scripts/logs.sh [N]   (last N entries, default 50, oldest first)
source "$(dirname "$0")/lib.sh"
# --limit keeps the first N entries in the requested order, so read newest-first
# (desc) to get the LATEST N and reverse them for display.
gcloud logging read 'logName:"projects/'"${PROJECT_ID}"'/logs/python"' \
  --project "${PROJECT_ID}" --freshness=30m --limit "${1:-50}" --order=desc \
  --format='value(timestamp,severity,textPayload,jsonPayload.message)' | tac
