#!/usr/bin/env bash
# What's running (= what's costing money) and is data arriving?
source "$(dirname "$0")/lib.sh"

VM="$(tf_out producer_vm)"
DATASET="$(tf_out dataset)"

log "Producer VM"
gcloud compute instances describe "${VM}" --project "${PROJECT_ID}" --zone "${ZONE}" --format="value(status)"

log "Dataflow jobs (last 5)"
gcloud dataflow jobs list --project "${PROJECT_ID}" --region "${REGION}" \
  --filter="name~^${JOB_PREFIX}" --limit 5 --format="table(id,name,state,creationTime)"

# The worker VM is what actually bills; a job can read 'Draining' while it is still running.
# Worker VM names start with the job name prefix (the producer VM does not).
log "Dataflow worker VMs (billing while RUNNING)"
workers="$(gcloud compute instances list --project "${PROJECT_ID}" \
  --filter="name~^${JOB_PREFIX}" --format="value(name,status)")"
if [[ -z "${workers}" ]]; then echo "none"; else echo "${workers}"; fi

log "BigQuery - last 15 minutes (trades = de-duplicated view, latency = median)"
# bq prints nothing at all for zero rows, so capture the output and say so explicitly.
# If the query itself fails, bq's error goes to stderr and set -e stops the script.
result="$(bq --project_id "${PROJECT_ID}" query --nouse_legacy_sql --format=pretty "
SELECT 'trades' AS tbl, symbol AS key, COUNT(*) AS n_rows,
       MAX(trade_time) AS latest,
       APPROX_QUANTILES(TIMESTAMP_DIFF(processing_time, trade_time, MILLISECOND), 100)[OFFSET(50)] AS p50_latency_ms
FROM \`${PROJECT_ID}.${DATASET}.trades_clean\`
WHERE trade_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY symbol
UNION ALL
SELECT 'raw_events', event_type, COUNT(*), MAX(ingest_ts), NULL
FROM \`${PROJECT_ID}.${DATASET}.raw_events\`
WHERE ingest_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY event_type
UNION ALL
SELECT 'dead_letter', error_stage, COUNT(*), MAX(processing_ts), NULL
FROM \`${PROJECT_ID}.${DATASET}.dead_letter\`
WHERE processing_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY error_stage
ORDER BY tbl, key")"
if [[ -z "${result}" ]]; then
  echo "(no rows in the last 15 minutes - the job is not writing yet, or nothing is running)"
else
  echo "${result}"
fi
