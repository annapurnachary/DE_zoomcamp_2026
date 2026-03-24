terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = "./keys/my-creds.json"
  project = var.project
  region  = var.region
}

# Data Lake (GCS Bucket)
resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true # Allows 'terraform destroy' to delete bucket even if not empty

  storage_class = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
}

# Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  delete_contents_on_destroy = true # Deletes tables if you destroy the project
}
