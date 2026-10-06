#!/usr/bin/env bash
# Start a streaming session: launch the Dataflow job, then start the producer VM.
source "$(dirname "$0")/lib.sh"

BUCKET="$(tf_out bucket)"
VM="$(tf_out producer_vm)"
TEMPLATE="gs://${BUCKET}/templates/${TEMPLATE_NAME}.json"

if ! gcloud storage objects describe "${TEMPLATE}" >/dev/null 2>&1; then
  warn "No Flex Template at ${TEMPLATE}. Run: make build"
  exit 1
fi

if [[ -n "$(active_jobs)" ]]; then
  log "A ${JOB_PREFIX} Dataflow job is already running - not launching another"
else
  # Workers run the exact image the template was built with.
  IMAGE="$(gcloud storage cat "${TEMPLATE}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["image"])')"
  JOB_NAME="${JOB_PREFIX}-$(date -u +%Y%m%d-%H%M%S)"
  log "Launching Dataflow job ${JOB_NAME} (workers need ~3-5 min to come up)"
  gcloud dataflow flex-template run "${JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --template-file-gcs-location "${TEMPLATE}" \
    --service-account-email "$(tf_out dataflow_service_account)" \
    --subnetwork "$(tf_out subnetwork)" \
    --staging-location "gs://${BUCKET}/staging" \
    --temp-location "gs://${BUCKET}/temp" \
    --worker-machine-type "${DATAFLOW_MACHINE_TYPE}" \
    --num-workers 1 \
    --max-workers 1 \
    --enable-streaming-engine \
    --parameters "input_subscription=$(tf_out subscription),raw_table=$(tf_out raw_table),trades_table=$(tf_out trades_table),dead_letter_table=$(tf_out dead_letter_table),metrics_table=$(tf_out metrics_table),sdk_container_image=${IMAGE}" \
    --format "value(job.id)"
fi

log "Starting producer VM ${VM}"
gcloud compute instances start "${VM}" --project "${PROJECT_ID}" --zone "${ZONE}" --quiet

log "Session is up. Costs accrue until you run: make down"
log "Watch it: make status | make logs | https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
