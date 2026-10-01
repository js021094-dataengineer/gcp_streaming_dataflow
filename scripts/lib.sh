#!/usr/bin/env bash
# Shared helpers for the scripts in this folder.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/terraform"

if [[ ! -f "${ROOT_DIR}/config.env" ]]; then
  echo "config.env not found. Run: cp config.env.example config.env  (then edit it)" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source "${ROOT_DIR}/config.env"
set +a

: "${PROJECT_ID:?set PROJECT_ID in config.env}"
: "${REGION:=europe-west6}"
: "${ZONE:=europe-west6-a}"
: "${DATAFLOW_MACHINE_TYPE:=n1-standard-2}"

JOB_PREFIX="crypto-trades"
TEMPLATE_NAME="crypto-pipeline"

tf_out() { terraform -chdir="${TF_DIR}" output -raw "$1"; }

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }

active_jobs() {
  gcloud dataflow jobs list --project "${PROJECT_ID}" --region "${REGION}" \
    --status=active --filter="name~^${JOB_PREFIX}" --format="value(id)"
}
