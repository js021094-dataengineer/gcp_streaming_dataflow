locals {
  services = [
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
  ]

  schema_dir = "${path.module}/../../pipeline/crypto_pipeline/schemas"
  labels = {
    project = "gcp-streaming-dataflow"
    managed = "terraform"
  }
}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_project_service" "services" {
  for_each           = toset(local.services)
  service            = each.value
  disable_on_destroy = false
}
