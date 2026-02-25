terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "./keys/my-creds.json"
  project     = "dtc-de-course-485402"
  region      = "us-central1"
}
resource "google_storage_bucket" "auto-expire" {
  name          = "anu-de-expiring-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "anu_de_dataset"
  project    = "dtc-de-course-485402"
  location   = "US"

  }

 