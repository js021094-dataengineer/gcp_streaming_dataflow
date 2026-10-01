resource "google_compute_instance" "producer" {
  name         = "${var.name_prefix}-producer"
  machine_type = var.producer_machine_type
  zone         = var.zone
  tags         = ["producer"]
  labels       = local.labels

  # Created stopped; `make up` / `make down` control it from then on.
  desired_status            = "TERMINATED"
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 10
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    # Ephemeral public IP for outbound WebSocket traffic. Cheaper than Cloud NAT
    # for a single VM; inbound is closed except IAP SSH.
    access_config {}
  }

  service_account {
    email  = google_service_account.producer.email
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata = {
    enable-oslogin    = "TRUE"
    startup-script    = file("${path.module}/../../producer/startup.sh")
    producer-code-uri = "gs://${google_storage_bucket.pipeline.name}/producer"
    pubsub-project    = var.project_id
    pubsub-topic      = google_pubsub_topic.raw.name
    symbols           = var.symbols
    streams           = var.streams
    # Changes whenever the code changes, so `terraform apply` shows a diff.
    producer-code-md5 = join(",", [for o in google_storage_bucket_object.producer_code : o.md5hash])
  }

  lifecycle {
    # make up/down start and stop the VM; Terraform must not fight that.
    ignore_changes = [desired_status]
  }

  depends_on = [
    google_project_iam_member.producer_logs,
    google_pubsub_topic_iam_member.producer_publish,
    google_storage_bucket_iam_member.producer_code_read,
  ]
}
