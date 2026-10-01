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

log "BigQuery - last 15 minutes"
bq --project_id "${PROJECT_ID}" query --nouse_legacy_sql --format=pretty "
SELECT 'trades' AS tbl, symbol AS key, COUNT(*) AS n_rows,
       MAX(trade_time) AS latest, ROUND(AVG(TIMESTAMP_DIFF(processing_time, trade_time, MILLISECOND))) AS avg_latency_ms
FROM \`${PROJECT_ID}.${DATASET}.trades\`
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
ORDER BY tbl, key"
