terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.0"
    }
  }

  # Bucket is created by scripts/bootstrap.sh and passed in by `make infra`:
  #   terraform init -backend-config="bucket=<project>-tfstate"
  backend "gcs" {
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# The Budgets API must be called with a quota project.
provider "google" {
  alias                 = "billing"
  project               = var.project_id
  user_project_override = true
  billing_project       = var.project_id
}
