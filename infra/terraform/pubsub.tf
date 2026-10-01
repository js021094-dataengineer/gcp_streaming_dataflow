# One raw topic for all Binance streams; routing happens on attributes.
resource "google_pubsub_topic" "raw" {
  name                       = "${var.name_prefix}-raw"
  labels                     = local.labels
  message_retention_duration = "86400s" # 1 day: lets you seek/replay recent data
  depends_on                 = [google_project_service.services]
}

resource "google_pubsub_subscription" "dataflow" {
  name                       = "${var.name_prefix}-raw-dataflow"
  topic                      = google_pubsub_topic.raw.id
  labels                     = local.labels
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s" # unacked data survives a stopped pipeline for 1 day
  retain_acked_messages      = false

  # Default would delete the subscription after 31 idle days - we run in sessions.
  expiration_policy {
    ttl = ""
  }
}
