#!/usr/bin/env bash
# End a streaming session: stop the producer first, then drain Dataflow so
# everything already in flight still lands in BigQuery.
source "$(dirname "$0")/lib.sh"

VM="$(tf_out producer_vm)"

log "Stopping producer VM ${VM}"
gcloud compute instances stop "${VM}" --project "${PROJECT_ID}" --zone "${ZONE}" --quiet || warn "VM stop failed (already stopped?)"

JOBS="$(active_jobs)"
if [[ -z "${JOBS}" ]]; then
  log "No active ${JOB_PREFIX} Dataflow jobs"
else
  for job in ${JOBS}; do
    log "Draining Dataflow job ${job}"
    gcloud dataflow jobs drain "${job}" --project "${PROJECT_ID}" --region "${REGION}" || warn "drain of ${job} failed (already draining?)"
  done
  log "Draining takes a few minutes; billing stops when the job shows 'Drained'. Check with: make status"
fi
log "Idle costs now: BigQuery/GCS storage + the stopped VM's 10 GB disk (cents per month)."
