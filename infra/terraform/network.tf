# Dedicated VPC so the default network's broad firewall rules don't apply.
resource "google_compute_network" "vpc" {
  name                    = "${var.name_prefix}-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.services]
}

resource "google_compute_subnetwork" "subnet" {
  name                     = "${var.name_prefix}-subnet"
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = "10.10.0.0/24"
  private_ip_google_access = true
}

# SSH into the producer only through Identity-Aware Proxy (no open port 22).
resource "google_compute_firewall" "iap_ssh" {
  name          = "${var.name_prefix}-allow-iap-ssh"
  network       = google_compute_network.vpc.id
  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["producer"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# Dataflow workers talk to each other on 12345-12346.
resource "google_compute_firewall" "dataflow_internal" {
  name        = "${var.name_prefix}-allow-dataflow-internal"
  network     = google_compute_network.vpc.id
  direction   = "INGRESS"
  source_tags = ["dataflow"]
  target_tags = ["dataflow"]

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
}
