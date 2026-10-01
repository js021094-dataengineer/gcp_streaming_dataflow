# --------------------------------------------------------------------------- #
# Producer VM: publish to the topic, read its code, write logs.
# --------------------------------------------------------------------------- #
resource "google_service_account" "producer" {
  account_id   = "${var.name_prefix}-producer"
  display_name = "Binance WebSocket producer"
  depends_on   = [google_project_service.services]
}

resource "google_pubsub_topic_iam_member" "producer_publish" {
  topic  = google_pubsub_topic.raw.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.producer.email}"
}

resource "google_storage_bucket_iam_member" "producer_code_read" {
  bucket = google_storage_bucket.pipeline.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.producer.email}"
}

resource "google_project_iam_member" "producer_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.producer.email}"
}

# --------------------------------------------------------------------------- #
# Dataflow workers + Flex Template launcher.
# --------------------------------------------------------------------------- #
resource "google_service_account" "dataflow" {
  account_id   = "${var.name_prefix}-dataflow"
  display_name = "Dataflow streaming job"
  depends_on   = [google_project_service.services]
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

# Project-level because Dataflow creates a temporary *tracking subscription*
# on the topic when a timestamp attribute is used (watermark estimation).
resource "google_project_iam_member" "dataflow_pubsub" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_bigquery_dataset_iam_member" "dataflow_bq_write" {
  dataset_id = google_bigquery_dataset.crypto.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_bucket" {
  bucket = google_storage_bucket.pipeline.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_artifact_registry_repository_iam_member" "dataflow_pull" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.dataflow.email}"
}

# --------------------------------------------------------------------------- #
# Cloud Build: builds the Flex Template image.
# --------------------------------------------------------------------------- #
resource "google_service_account" "build" {
  account_id   = "${var.name_prefix}-build"
  display_name = "Cloud Build for the pipeline image"
  depends_on   = [google_project_service.services]
}

resource "google_artifact_registry_repository_iam_member" "build_push" {
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.build.email}"
}

resource "google_project_iam_member" "build_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/storage.objectViewer", # read the uploaded build source
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.build.email}"
}
