#!/usr/bin/env bash
# Build the pipeline container with Cloud Build (no local Docker needed) and
# publish the Flex Template spec to GCS.
source "$(dirname "$0")/lib.sh"

BUCKET="$(tf_out bucket)"
IMAGE_REPO="$(tf_out image_repo)"
BUILD_SA="$(tf_out build_service_account)"
TAG="$(date -u +%Y%m%d-%H%M%S)"
IMAGE="${IMAGE_REPO}/${TEMPLATE_NAME}:${TAG}"
TEMPLATE="gs://${BUCKET}/templates/${TEMPLATE_NAME}.json"

log "Building ${IMAGE} with Cloud Build (takes ~5-8 min)"
gcloud builds submit "${ROOT_DIR}/pipeline" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --config "${ROOT_DIR}/pipeline/cloudbuild.yaml" \
  --substitutions "_IMAGE=${IMAGE}" \
  --service-account "${BUILD_SA}" \
  --gcs-source-staging-dir "gs://${BUCKET}/cloudbuild/source"

log "Writing Flex Template spec ${TEMPLATE}"
gcloud dataflow flex-template build "${TEMPLATE}" \
  --project "${PROJECT_ID}" \
  --image "${IMAGE}" \
  --sdk-language PYTHON

log "Build done. Next: make up"
