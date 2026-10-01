#!/usr/bin/env bash
# One-time setup: create the project, link billing, create the Terraform state bucket.
# Safe to re-run.
source "$(dirname "$0")/lib.sh"
: "${BILLING_ACCOUNT:?set BILLING_ACCOUNT in config.env (gcloud billing accounts list)}"

if gcloud projects describe "${PROJECT_ID}" >/dev/null 2>&1; then
  log "Project ${PROJECT_ID} already exists"
else
  log "Creating project ${PROJECT_ID}"
  gcloud projects create "${PROJECT_ID}" --name="gcp streaming dataflow"
fi

log "Linking billing account ${BILLING_ACCOUNT}"
gcloud billing projects link "${PROJECT_ID}" --billing-account="${BILLING_ACCOUNT}" >/dev/null

log "Enabling the APIs Terraform needs to bootstrap itself"
gcloud services enable --project "${PROJECT_ID}" \
  serviceusage.googleapis.com cloudresourcemanager.googleapis.com \
  storage.googleapis.com billingbudgets.googleapis.com cloudbilling.googleapis.com

STATE_BUCKET="gs://${PROJECT_ID}-tfstate"
if gcloud storage buckets describe "${STATE_BUCKET}" >/dev/null 2>&1; then
  log "State bucket ${STATE_BUCKET} already exists"
else
  log "Creating Terraform state bucket ${STATE_BUCKET}"
  gcloud storage buckets create "${STATE_BUCKET}" --project "${PROJECT_ID}" \
    --location "${REGION}" --uniform-bucket-level-access --public-access-prevention
  gcloud storage buckets update "${STATE_BUCKET}" --versioning
fi

if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
  warn "Terraform needs Application Default Credentials. Run:"
  warn "  gcloud auth application-default login"
fi
gcloud auth application-default set-quota-project "${PROJECT_ID}" >/dev/null 2>&1 || true

log "Bootstrap done. Next: make infra"
