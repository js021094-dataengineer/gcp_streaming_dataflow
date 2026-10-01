output "topic" {
  value = google_pubsub_topic.raw.id
}

output "subscription" {
  value = google_pubsub_subscription.dataflow.id
}

output "dataset" {
  value = google_bigquery_dataset.crypto.dataset_id
}

output "raw_table" {
  value = "${var.project_id}:${google_bigquery_dataset.crypto.dataset_id}.raw_events"
}

output "trades_table" {
  value = "${var.project_id}:${google_bigquery_dataset.crypto.dataset_id}.trades"
}

output "dead_letter_table" {
  value = "${var.project_id}:${google_bigquery_dataset.crypto.dataset_id}.dead_letter"
}

output "bucket" {
  value = google_storage_bucket.pipeline.name
}

output "image_repo" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "dataflow_service_account" {
  value = google_service_account.dataflow.email
}

output "build_service_account" {
  value = google_service_account.build.id
}

output "subnetwork" {
  value = "regions/${var.region}/subnetworks/${google_compute_subnetwork.subnet.name}"
}

output "producer_vm" {
  value = google_compute_instance.producer.name
}

output "zone" {
  value = var.zone
}
