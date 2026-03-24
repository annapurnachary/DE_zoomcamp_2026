variable "project" {
  description = "Your GCP Project ID"
  default     = "final-project-de-zoomcamp-2026" # REPLACE ME
}

variable "region" {
  description = "Region for GCP resources"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "divvy_data_lake" # REPLACE ME (Must be globally unique)
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "divvy_trips_data"
}
