# Dataflow temp/staging, Flex Template spec and producer code.
resource "google_storage_bucket" "pipeline" {
  name                        = "${var.project_id}-pipeline"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  labels                      = local.labels
  depends_on                  = [google_project_service.services]

  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["temp/", "staging/"]
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_object" "producer_code" {
  for_each = toset(["producer.py", "requirements.txt"])

  bucket = google_storage_bucket.pipeline.name
  name   = "producer/${each.value}"
  source = "${path.module}/../../producer/${each.value}"
}

resource "google_artifact_registry_repository" "images" {
  repository_id = "dataflow"
  location      = var.region
  format        = "DOCKER"
  description   = "Flex Template / worker images."
  labels        = local.labels
  depends_on    = [google_project_service.services]

  cleanup_policies {
    id     = "keep-latest-3"
    action = "KEEP"
    most_recent_versions {
      keep_count = 3
    }
  }

  cleanup_policies {
    id     = "delete-older"
    action = "DELETE"
    condition {
      older_than = "7d" # anything not covered by keep-latest-3
    }
  }
}
